import os
import sys
import asyncio
import http.server
import socketserver
import threading
import re
import logging
import html
import time
from telegram.error import RetryAfter, TimedOut, NetworkError

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.constants import ChatMemberStatus, ChatType
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ChatMemberHandler,
    ContextTypes,
    filters,
)
from pymongo import MongoClient


# ============================================================
# LOGGING SETUP
# ============================================================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)


# ============================================================
# RENDER DUMMY SERVER
# ============================================================

def start_dummy_server():
    PORT = int(os.environ.get("PORT", 10000))

    Handler = http.server.SimpleHTTPRequestHandler

    try:
        with socketserver.TCPServer(("", PORT), Handler) as httpd:
            logger.info(f"ðŸŒ Dummy server started on port {PORT}")
            httpd.serve_forever()

    except Exception as e:
        logger.warning(f"Dummy server stopped: {e}")


# ============================================================
# MONGODB CONFIGURATION
# ============================================================

MONGO_URI = os.environ.get("MONGO_URI")

if not MONGO_URI:
    logger.error("âŒ MONGO_URI environment variable missing.")
    sys.exit(1)


try:
    db_client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000
    )

    db_client.admin.command("ping")

except Exception as e:
    logger.error(f"âŒ MongoDB connection failed: {e}")
    sys.exit(1)


db = db_client["AutoCaptionBotDB"]

# Channel-wise database
channels_col = db["connected_channels"]

# User settings database
users_col = db["user_settings"]


# ============================================================
# ADMIN SYSTEM
# ============================================================

def _parse_admin_ids():

    raw = os.environ.get("ADMIN_IDS", "").strip()

    ids = set()

    if raw:
        for part in raw.split(","):

            part = part.strip()

            if part.isdigit():
                ids.add(int(part))

    # Backward compatibility
    legacy_owner = os.environ.get("OWNER_ID", "0").strip()

    if legacy_owner.isdigit() and int(legacy_owner) != 0:
        ids.add(int(legacy_owner))

    return ids


ADMIN_IDS = _parse_admin_ids()


def is_admin(user_id: int) -> bool:

    # Agar ADMIN_IDS configure nahi hai,
    # bot open mode mein chalega.
    if not ADMIN_IDS:
        return True

    return user_id in ADMIN_IDS


# ============================================================
# LOG CHANNEL
# ============================================================

raw_log_id = os.environ.get("LOG_CHANNEL_ID")

LOG_CHANNEL_ID = (
    int(raw_log_id)
    if raw_log_id and raw_log_id.strip()
    else None
)


# ============================================================
# BOT USERNAME
# ============================================================

# Render Environment Variable:
# BOT_USERNAME = YourBotUsername
#
# Agar ye set nahi hai to neeche fallback use hoga.

BOT_USERNAME = "DG_Primebot"


# ============================================================
# PERFORMANCE / RELIABILITY
# ============================================================

# Number of caption edits allowed to run concurrently.
# 8-10 is a safe starting point for burst traffic.
CAPTION_WORKERS = int(os.environ.get("CAPTION_WORKERS", "10"))

# Retry temporary Telegram/network failures.
MAX_EDIT_RETRIES = int(os.environ.get("MAX_EDIT_RETRIES", "4"))

# Keep channel settings in RAM briefly so a burst of posts
# does not hit MongoDB for every single message.
CONFIG_CACHE_TTL = int(os.environ.get("CONFIG_CACHE_TTL", "30"))

_caption_semaphore = asyncio.Semaphore(CAPTION_WORKERS)
_config_cache = {}


def get_cached_channel_config(channel_id):
    now = time.monotonic()
    cached = _config_cache.get(channel_id)

    if cached and (now - cached["time"]) < CONFIG_CACHE_TTL:
        return cached["config"]

    config = get_channel_config(channel_id)

    _config_cache[channel_id] = {
        "time": now,
        "config": config,
    }

    return config


def invalidate_channel_config_cache(channel_id):
    _config_cache.pop(channel_id, None)


async def edit_with_retry(edit_func, *args, **kwargs):
    last_error = None

    for attempt in range(1, MAX_EDIT_RETRIES + 1):
        try:
            return await edit_func(*args, **kwargs)

        except RetryAfter as exc:
            last_error = exc
            wait_time = float(exc.retry_after) + 0.2
            logger.warning(
                f"â³ Telegram rate limit; retrying in {wait_time:.1f}s "
                f"(attempt {attempt}/{MAX_EDIT_RETRIES})"
            )
            await asyncio.sleep(wait_time)

        except (TimedOut, NetworkError) as exc:
            last_error = exc
            wait_time = min(0.5 * (2 ** (attempt - 1)), 4.0)
            logger.warning(
                f"ðŸŒ Temporary Telegram/network error; retrying in "
                f"{wait_time:.1f}s (attempt {attempt}/{MAX_EDIT_RETRIES}): {exc}"
            )
            await asyncio.sleep(wait_time)

    raise last_error




# ============================================================
# RULE HELPERS
# ============================================================

def rule_value(rule):

    if isinstance(rule, dict):
        return rule.get("new", "")

    return rule


def rule_owner(rule):

    if isinstance(rule, dict):
        return rule.get("added_by")

    return None


def rule_approved(rule):

    if isinstance(rule, dict):
        return rule.get("approved", True)

    return True


# ============================================================
# DEFAULT CHANNEL CONFIG
# ============================================================

def default_channel_config(channel_id):

    return {
        "_id": str(channel_id),

        "channel_id": channel_id,

        "owner_user_id": None,

        "active": True,

        "title": "",

        "username": "",

        "replacement_rules": {

            "MovieHub": {
                "new": "DG_Contents",
                "added_by": None,
                "approved": True,
            },

            "JoinUs": {
                "new": "SubscribeNow",
                "added_by": None,
                "approved": True,
            },
        },

        "custom_header": "",

        "custom_footer": "âš¡ï¸ Fast Download Links @DG_Contents",
    }


# ============================================================
# GET CHANNEL CONFIG
# ============================================================

def get_channel_config(channel_id):

    try:

        config = channels_col.find_one(
            {
                "_id": str(channel_id)
            }
        )

        if not config:

            config = default_channel_config(
                channel_id
            )

            channels_col.insert_one(config)

            return config

        return config

    except Exception as e:

        logger.error(
            f"âŒ MongoDB get channel config error: {e}"
        )

        return default_channel_config(
            channel_id
        )


# ============================================================
# UPDATE CHANNEL CONFIG
# ============================================================

def update_channel_config(
    channel_id,
    field_name,
    field_value
):

    try:

        channels_col.update_one(

            {
                "_id": str(channel_id)
            },

            {
                "$set": {
                    field_name: field_value
                }
            },

            upsert=True
        )

    except Exception as e:

        logger.error(
            f"âŒ MongoDB update error: {e}"
        )
    finally:
        invalidate_channel_config_cache(channel_id)


# ============================================================
# USER SELECTED CHANNEL
# ============================================================

def get_selected_channel(user_id):

    try:

        user = users_col.find_one(
            {
                "_id": str(user_id)
            }
        )

        if not user:
            return None

        return user.get(
            "selected_channel"
        )

    except Exception as e:

        logger.error(
            f"âŒ User settings error: {e}"
        )

        return None


def set_selected_channel(
    user_id,
    channel_id
):

    try:

        users_col.update_one(

            {
                "_id": str(user_id)
            },

            {
                "$set": {
                    "selected_channel": channel_id
                }
            },

            upsert=True
        )

        return True

    except Exception as e:

        logger.error(
            f"âŒ Selected channel update failed: {e}"
        )

        return False


# ============================================================
# USER OWNS CHANNEL
# ============================================================

def user_owns_channel(
    user_id,
    channel_id
):

    try:

        channel = channels_col.find_one(

            {
                "_id": str(channel_id),

                "owner_user_id": user_id,

                "active": True,
            }
        )

        return channel is not None

    except Exception as e:

        logger.error(
            f"âŒ Ownership check failed: {e}"
        )

        return False


# ============================================================
# GET USER CONNECTED CHANNELS
# ============================================================

def get_user_channels(user_id):

    try:

        return list(
            channels_col.find(
                {
                    "owner_user_id": user_id,

                    "active": True,
                }
            )
        )

    except Exception as e:

        logger.error(
            f"âŒ Getting user channels failed: {e}"
        )

        return []


# ============================================================
# CHANNEL CONNECTION HANDLER
# ============================================================

async def handle_bot_channel_status(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    chat_member_update = update.my_chat_member

    if not chat_member_update:
        return

    chat = chat_member_update.chat

    # Sirf channels
    if chat.type != ChatType.CHANNEL:
        return

    new_status = (
        chat_member_update.new_chat_member.status
    )

    old_status = (
        chat_member_update.old_chat_member.status
    )

    actor_user_id = (
        chat_member_update.from_user.id
    )

    channel_id = chat.id

    logger.info(
        f"ðŸ“¡ Channel membership update | "
        f"Channel: {channel_id} | "
        f"Old: {old_status} | "
        f"New: {new_status} | "
        f"By: {actor_user_id}"
    )


    # ========================================================
    # BOT ADDED / PROMOTED AS ADMIN
    # ========================================================

    if new_status == ChatMemberStatus.ADMINISTRATOR:

        try:

            me = await context.bot.get_me()

            bot_member = (
                await context.bot.get_chat_member(
                    chat_id=channel_id,
                    user_id=me.id
                )
            )

            if bot_member.status != ChatMemberStatus.ADMINISTRATOR:

                logger.warning(
                    f"âš ï¸ Bot is not admin in {channel_id}"
                )

                return


            title = chat.title or ""

            username = chat.username or ""


            existing = channels_col.find_one(
                {
                    "_id": str(channel_id)
                }
            )


            if existing:

                channels_col.update_one(

                    {
                        "_id": str(channel_id)
                    },

                    {
                        "$set": {

                            "active": True,

                            "title": title,

                            "username": username,

                            "owner_user_id": actor_user_id,
                        }
                    }
                )

            else:

                config = default_channel_config(
                    channel_id
                )

                config["owner_user_id"] = (
                    actor_user_id
                )

                config["title"] = title

                config["username"] = username

                channels_col.insert_one(
                    config
                )


            # Automatically select
            set_selected_channel(
                actor_user_id,
                channel_id
            )


            # =================================================
            # CONFIRMATION MESSAGE
            # =================================================

            await context.bot.send_message(

                chat_id=actor_user_id,

                text=(

                    "ðŸŽ‰ <b>CHANNEL CONNECTED!</b>\n\n"

                    f"ðŸ“¢ <b>Channel:</b> "
                    f"<code>{html.escape(title or str(channel_id))}</code>\n\n"

                    f"ðŸ†” <b>Channel ID:</b> "
                    f"<code>{channel_id}</code>\n\n"

                    "â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”\n\n"

                    "âœ… <b>Connection Status:</b> ACTIVE\n\n"

                    "ðŸŽ¯ <b>Quick Commands:</b>\n\n"

                    "â€¢ <code>/channels</code> â€” My Channels\n"
                    "â€¢ <code>/usechannel ID</code> â€” Select Channel\n"
                    "â€¢ <code>/addrule old -> new</code> â€” Add Rule\n"
                    "â€¢ <code>/status</code> â€” Channel Status\n\n"

                    "ðŸ”’ <b>Important:</b>\n"
                    "Is channel ki rules/settings "
                    "<b>sirf isi channel</b> par apply hongi.\n\n"

                    "ðŸ’¡ Multiple channels ke liye bot ko "
                    "har channel mein admin bana sakte ho."
                ),

                parse_mode="HTML"
            )


            logger.info(
                f"âœ… Channel connected: "
                f"{channel_id} "
                f"owner={actor_user_id}"
            )


        except Exception as e:

            logger.error(
                f"âŒ Channel connection failed: {e}"
            )


    # ========================================================
    # BOT REMOVED
    # ========================================================

    elif new_status in (
        ChatMemberStatus.LEFT,
        ChatMemberStatus.KICKED,
    ):

        try:

            channels_col.update_one(

                {
                    "_id": str(channel_id)
                },

                {
                    "$set": {
                        "active": False
                    }
                }
            )

            logger.info(
                f"ðŸ”´ Channel disconnected: "
                f"{channel_id}"
            )

        except Exception as e:

            logger.error(
                f"âŒ Channel disconnect error: {e}"
            )


# ============================================================
# /CHANNELS
# ============================================================

async def channels_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    user_id = update.effective_user.id

    user_channels = get_user_channels(
        user_id
    )


    if not user_channels:

        await update.message.reply_text(

            "ðŸ“­ <b>No Connected Channels</b>\n\n"

            "Pehle mujhe apne Telegram channel mein "
            "<b>Administrator</b> ke roop mein add karo.\n\n"

            "Channel connect hone ke baad yahan show hoga.",

            parse_mode="HTML"
        )

        return


    selected = get_selected_channel(
        user_id
    )


    message = (
        "ðŸ“¢ <b>YOUR CONNECTED CHANNELS</b>\n\n"
    )


    for index, channel in enumerate(
        user_channels,
        start=1
    ):

        channel_id = channel.get(
            "channel_id"
        )

        title = channel.get(
            "title",
            "Unknown Channel"
        )

        username = channel.get(
            "username",
            ""
        )


        selected_mark = (

            " ðŸŸ¢ <b>SELECTED</b>"

            if selected == channel_id

            else ""
        )


        if username:

            username_text = (
                f"@{username}"
            )

        else:

            username_text = (
                "Private Channel"
            )


        message += (

            f"{index}. ðŸ“¢ "
            f"<b>{html.escape(title)}</b>"
            f"{selected_mark}\n"

            f"   ðŸ†” <code>{channel_id}</code>\n"

            f"   ðŸ”— "
            f"{html.escape(username_text)}\n\n"
        )


    message += (

        "â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”\n\n"

        "ðŸŽ¯ <b>Select Channel:</b>\n"

        "<code>/usechannel CHANNEL_ID</code>"
    )


    await update.message.reply_text(

        message,

        parse_mode="HTML"
    )


# ============================================================
# /USECHANNEL
# ============================================================

async def use_channel(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    user_id = update.effective_user.id


    if not context.args:

        await update.message.reply_text(

            "ðŸŽ¯ <b>Select Your Channel</b>\n\n"

            "Pehle:\n"
            "<code>/channels</code>\n\n"

            "Phir:\n"
            "<code>/usechannel CHANNEL_ID</code>",

            parse_mode="HTML"
        )

        return


    raw_channel_id = (
        context.args[0].strip()
    )


    try:

        channel_id = int(
            raw_channel_id
        )

    except ValueError:

        await update.message.reply_text(
            "âŒ Invalid Channel ID.",
            parse_mode="HTML"
        )

        return


    if not user_owns_channel(
        user_id,
        channel_id
    ):

        await update.message.reply_text(

            "â›” <b>Access Denied</b>\n\n"

            "Ye channel aapke account se connected nahi hai.\n\n"

            "Apne channels dekhne ke liye:\n"
            "<code>/channels</code>",

            parse_mode="HTML"
        )

        return


    set_selected_channel(
        user_id,
        channel_id
    )


    channel = get_channel_config(
        channel_id
    )


    title = channel.get(
        "title",
        "Channel"
    )


    await update.message.reply_text(

        "âœ… <b>CHANNEL SELECTED</b>\n\n"

        f"ðŸ“¢ <b>{html.escape(title)}</b>\n"

        f"ðŸ†” <code>{channel_id}</code>\n\n"

        "â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”\n\n"

        "Ab jo bhi rules/settings aap change karoge "
        "woh <b>sirf isi channel</b> ke liye honge.",

        parse_mode="HTML"
    )


# ============================================================
# GET SELECTED COMMAND CHANNEL
# ============================================================

async def get_command_channel(
    update: Update
):

    user_id = update.effective_user.id


    channel_id = get_selected_channel(
        user_id
    )


    if not channel_id:

        await update.message.reply_text(

            "âš ï¸ <b>No Channel Selected</b>\n\n"

            "Pehle:\n"
            "<code>/channels</code>\n\n"

            "Phir:\n"
            "<code>/usechannel CHANNEL_ID</code>",

            parse_mode="HTML"
        )

        return None


    if not user_owns_channel(
        user_id,
        channel_id
    ):

        await update.message.reply_text(

            "â›” <b>Selected Channel Invalid</b>\n\n"

            "Please <code>/channels</code> se "
            "channel dobara select karo.",

            parse_mode="HTML"
        )

        return None


    return channel_id


# ============================================================
# CAPTION EDITOR
# ============================================================

async def edit_channel_caption(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):
    msg = update.channel_post or update.edited_channel_post

    if not msg:
        return

    channel_id = msg.chat_id
    message_id = msg.message_id

    # Limit only the actual edit work. Updates can still arrive
    # and wait in an asyncio queue instead of being silently lost.
    async with _caption_semaphore:

        logger.info(
            f"ðŸ“© Processing channel post | "
            f"Channel: {channel_id} | Message: {message_id}"
        )

        # Cached config = much faster during bursts.
        config = get_cached_channel_config(channel_id)

        if not config or not config.get("active", True):
            logger.info(
                f"â­ï¸ Channel {channel_id} not connected/active."
            )
            return

        replacement_rules = config.get(
            "replacement_rules",
            {}
        )

        custom_header = config.get(
            "custom_header",
            ""
        )

        custom_footer = config.get(
            "custom_footer",
            ""
        )

        text_to_check = msg.text or msg.caption

        # Media without caption has nothing to edit.
        if not text_to_check:
            logger.info(
                f"â­ï¸ Message {message_id} has no text/caption."
            )
            return

        final_text = text_to_check

        try:
            # Remove unwanted Telegram links.
            final_text = re.sub(
                r"(https?://)?t\.me/"
                r"(?!DG_Contents|dghelps_bot)"
                r"[a-zA-Z0-9_]+",
                "",
                final_text,
                flags=re.IGNORECASE
            )

            # Remove unwanted @mentions.
            final_text = re.sub(
                r"@(?!DG_Contents|dghelps_bot)"
                r"[a-zA-Z0-9_]+",
                "",
                final_text,
                flags=re.IGNORECASE
            )

            # Apply this channel's own rules.
            for old_txt, rule in replacement_rules.items():
                if not rule_approved(rule):
                    continue

                new_txt = rule_value(rule)

                if not old_txt:
                    continue

                final_text = re.compile(
                    re.escape(old_txt),
                    re.IGNORECASE
                ).sub(
                    new_txt,
                    final_text
                )

            final_text = re.sub(
                r" +",
                " ",
                final_text
            ).strip()

        except Exception as exc:
            logger.exception(
                f"âŒ Text processing failed for message {message_id}: {exc}"
            )
            return

        safe_header = html.escape(custom_header)
        safe_footer = html.escape(custom_footer)
        safe_text = html.escape(final_text)

        header_part = (
            f"<b>{safe_header}</b>\n\n"
            if custom_header else ""
        )

        footer_part = (
            f"\n\n<b>{safe_footer}</b>"
            if custom_footer else ""
        )

        final_caption = (
            f"{header_part}"
            f"<b>{safe_text}</b>"
            f"{footer_part}"
        )

        try:
            # Avoid an unnecessary Telegram API call if no actual change
            # is needed.
            if msg.caption:
                current_caption = msg.caption_html or ""

                if current_caption != final_caption:
                    await edit_with_retry(
                        context.bot.edit_message_caption,
                        chat_id=channel_id,
                        message_id=message_id,
                        caption=final_caption,
                        parse_mode="HTML"
                    )

                    logger.info(
                        f"âœ… Caption edited: {channel_id}/{message_id}"
                    )

            elif msg.text:
                current_text = msg.text_html or ""

                if current_text != final_caption:
                    await edit_with_retry(
                        context.bot.edit_message_text,
                        chat_id=channel_id,
                        message_id=message_id,
                        text=final_caption,
                        parse_mode="HTML"
                    )

                    logger.info(
                        f"âœ… Text edited: {channel_id}/{message_id}"
                    )

            # Optional logging creates a second API call, so keep it
            # separate from the main edit path.
            if LOG_CHANNEL_ID:
                try:
                    await edit_with_retry(
                        context.bot.copy_message,
                        chat_id=LOG_CHANNEL_ID,
                        from_chat_id=channel_id,
                        message_id=message_id
                    )
                except Exception as log_error:
                    logger.warning(
                        f"âš ï¸ Log copy failed for {channel_id}/{message_id}: "
                        f"{log_error}"
                    )

        except Exception as exc:
            # IMPORTANT: never silently swallow a failed message.
            logger.exception(
                f"âŒ FAILED message {channel_id}/{message_id}: {exc}"
            )


# ============================================================
# /ADDRULE
# ============================================================

async def add_rule(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    user_id = update.effective_user.id


    channel_id = await get_command_channel(
        update
    )


    if channel_id is None:
        return


    raw_args = " ".join(
        context.args
    )


    if " -> " not in raw_args:

        await update.message.reply_text(

            "âœ¨ <b>ADD REPLACEMENT RULE</b>\n\n"

            "Format:\n"
            "<code>/addrule old_text -> new_text</code>\n\n"

            "Example:\n"
            "<code>/addrule MovieHub -> DG_Contents</code>",

            parse_mode="HTML"
        )

        return


    try:

        old_part, new_part = (
            raw_args.split(
                " -> ",
                1
            )
        )


        old_part = old_part.strip()

        new_part = new_part.strip()


        if not old_part:

            await update.message.reply_text(
                "âŒ Old text empty nahi ho sakta.",
                parse_mode="HTML"
            )

            return


        if not new_part:

            await update.message.reply_text(
                "âŒ New text empty nahi ho sakta.",
                parse_mode="HTML"
            )

            return


        config = get_channel_config(
            channel_id
        )


        rules = config.get(
            "replacement_rules",
            {}
        )


        # Admin = instantly live
        approved = is_admin(
            user_id
        )


        rules[old_part] = {

            "new": new_part,

            "added_by": user_id,

            "approved": approved,
        }


        update_channel_config(

            channel_id,

            "replacement_rules",

            rules
        )


        channel_title = config.get(
            "title",
            str(channel_id)
        )


        if approved:

            await update.message.reply_text(

                "âœ… <b>RULE ADDED & LIVE!</b>\n\n"

                f"ðŸ“¢ <b>Channel:</b> "
                f"{html.escape(channel_title)}\n\n"

                f"ðŸ” <code>{html.escape(old_part)}</code>"
                " âž¡ï¸ "
                f"<code>{html.escape(new_part)}</code>\n\n"

                "ðŸ”’ Ye rule <b>sirf isi channel</b> "
                "ke posts par apply hoga.",

                parse_mode="HTML"
            )


        else:

            await update.message.reply_text(

                "ðŸ•’ <b>RULE SUBMITTED FOR REVIEW</b>\n\n"

                f"ðŸ“¢ <b>Channel:</b> "
                f"{html.escape(channel_title)}\n\n"

                f"ðŸ” <code>{html.escape(old_part)}</code>"
                " âž¡ï¸ "
                f"<code>{html.escape(new_part)}</code>\n\n"

                "Admin approval ke baad rule isi "
                "connected channel par live hoga.\n\n"

                "Status:\n"
                "<code>/status</code>",

                parse_mode="HTML"
            )


    except Exception as e:

        logger.error(
            f"âŒ Add rule error: {e}"
        )

        await update.message.reply_text(

            "âŒ <b>Error:</b> "
            "Kuch galat ho gaya.\n"
            "Format check karo.",

            parse_mode="HTML"
        )


# ============================================================
# /DELRULE
# ============================================================

async def del_rule(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    user_id = update.effective_user.id


    channel_id = await get_command_channel(
        update
    )


    if channel_id is None:
        return


    old_text = " ".join(
        context.args
    ).strip()


    if not old_text:

        await update.message.reply_text(

            "âœ¨ <b>Format:</b>\n"
            "<code>/delrule old_text</code>",

            parse_mode="HTML"
        )

        return


    config = get_channel_config(
        channel_id
    )


    rules = config.get(
        "replacement_rules",
        {}
    )


    if old_text not in rules:

        await update.message.reply_text(

            "âŒ <b>Rule Not Found</b>\n\n"

            "Exact old text use karo.",

            parse_mode="HTML"
        )

        return


    rule = rules[old_text]

    owner = rule_owner(
        rule
    )


    if (
        is_admin(user_id)
        or owner == user_id
    ):

        del rules[old_text]


        update_channel_config(

            channel_id,

            "replacement_rules",

            rules
        )


        await update.message.reply_text(

            "ðŸ—‘ï¸ <b>RULE DELETED</b>\n\n"

            f"<code>{html.escape(old_text)}</code>\n\n"

            "Rule selected channel ke database "
            "se permanently delete ho gaya.",

            parse_mode="HTML"
        )


    else:

        await update.message.reply_text(

            "â›” <b>Access Denied</b>\n\n"

            "Aap sirf apna rule delete kar sakte ho.",

            parse_mode="HTML"
        )


# ============================================================
# /APPROVE
# ============================================================

async def approve_rule(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    user_id = update.effective_user.id


    if not is_admin(user_id):

        await update.message.reply_text(

            "â›” <b>Access Denied</b>\n\n"
            "Sirf admin approve kar sakte hain.",

            parse_mode="HTML"
        )

        return


    channel_id = await get_command_channel(
        update
    )


    if channel_id is None:
        return


    old_text = " ".join(
        context.args
    ).strip()


    if not old_text:

        await update.message.reply_text(

            "âœ¨ <b>Format:</b>\n"
            "<code>/approve old_text</code>",

            parse_mode="HTML"
        )

        return


    config = get_channel_config(
        channel_id
    )


    rules = config.get(
        "replacement_rules",
        {}
    )


    if old_text not in rules:

        await update.message.reply_text(

            "âŒ Rule not found.",

            parse_mode="HTML"
        )

        return


    rule = rules[old_text]


    if not isinstance(rule, dict):

        rule = {
            "new": rule,

            "added_by": None,
        }


    rule["approved"] = True


    rules[old_text] = rule


    update_channel_config(

        channel_id,

        "replacement_rules",

        rules
    )


    await update.message.reply_text(

        "âœ… <b>RULE APPROVED!</b>\n\n"

        f"<code>{html.escape(old_text)}</code>\n\n"

        "Ab ye rule selected channel mein LIVE hai.",

        parse_mode="HTML"
    )


# ============================================================
# /PENDING
# ============================================================

async def pending(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    user_id = update.effective_user.id


    if not is_admin(user_id):

        await update.message.reply_text(

            "â›” <b>Access Denied.</b>",

            parse_mode="HTML"
        )

        return


    channel_id = await get_command_channel(
        update
    )


    if channel_id is None:
        return


    config = get_channel_config(
        channel_id
    )


    rules = config.get(
        "replacement_rules",
        {}
    )


    pending_rules = {

        old: rule

        for old, rule in rules.items()

        if not rule_approved(rule)
    }


    if not pending_rules:

        await update.message.reply_text(

            "âœ… <i>No pending rules.</i>",

            parse_mode="HTML"
        )

        return


    message = (
        "ðŸ•’ <b>PENDING RULES</b>\n\n"
    )


    for old, rule in pending_rules.items():

        owner = rule_owner(
            rule
        )


        message += (

            f"ðŸ” <code>{html.escape(old)}</code>"
            " âž¡ï¸ "

            f"<code>{html.escape(rule_value(rule))}</code>\n"

            f"ðŸ‘¤ By: <code>{owner}</code>\n\n"
        )


    message += (

        "Approve:\n"
        "<code>/approve old_text</code>"
    )


    await update.message.reply_text(

        message,

        parse_mode="HTML"
    )


# ============================================================
# /SETHEADER
# ============================================================

async def set_header(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    user_id = update.effective_user.id


    if not is_admin(user_id):

        await update.message.reply_text(

            "â›” <b>Access Denied.</b>",

            parse_mode="HTML"
        )

        return


    channel_id = await get_command_channel(
        update
    )


    if channel_id is None:
        return


    header_text = " ".join(
        context.args
    ).strip()


    update_channel_config(

        channel_id,

        "custom_header",

        header_text
    )


    await update.message.reply_text(

        "ðŸ“ <b>HEADER UPDATED</b>\n\n"

        f"<code>{html.escape(header_text) or 'EMPTY'}</code>\n\n"

        "Header sirf selected channel par apply hoga.",

        parse_mode="HTML"
    )


# ============================================================
# /SETFOOTER
# ============================================================

async def set_footer(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    user_id = update.effective_user.id


    if not is_admin(user_id):

        await update.message.reply_text(

            "â›” <b>Access Denied.</b>",

            parse_mode="HTML"
        )

        return


    channel_id = await get_command_channel(
        update
    )


    if channel_id is None:
        return


    footer_text = " ".join(
        context.args
    ).strip()


    update_channel_config(

        channel_id,

        "custom_footer",

        footer_text
    )


    await update.message.reply_text(

        "ðŸ“ <b>FOOTER UPDATED</b>\n\n"

        f"<code>{html.escape(footer_text) or 'EMPTY'}</code>\n\n"

        "Footer sirf selected channel par apply hoga.",

        parse_mode="HTML"
    )


# ============================================================
# /CLEAR
# ============================================================

async def clear_rules(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    user_id = update.effective_user.id


    if not is_admin(user_id):

        await update.message.reply_text(

            "â›” <b>Access Denied.</b>",

            parse_mode="HTML"
        )

        return


    channel_id = await get_command_channel(
        update
    )


    if channel_id is None:
        return


    update_channel_config(

        channel_id,

        "replacement_rules",

        {}
    )


    await update.message.reply_text(

        "ðŸ§¹ <b>RULES CLEARED</b>\n\n"

        "Selected channel ke saare replacement "
        "rules clear ho gaye.",

        parse_mode="HTML"
    )


# ============================================================
# /STATUS
# ============================================================

async def status(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    channel_id = await get_command_channel(
        update
    )


    if channel_id is None:
        return


    config = get_channel_config(
        channel_id
    )


    replacement_rules = config.get(
        "replacement_rules",
        {}
    )


    custom_header = config.get(
        "custom_header",
        ""
    )


    custom_footer = config.get(
        "custom_footer",
        ""
    )


    channel_title = config.get(
        "title",
        "Unknown"
    )


    channel_username = config.get(
        "username",
        ""
    )


    message = (

        "âš™ï¸ <b>AUTO CAPTION ENGINE</b>\n"
        "ðŸ“Š <b>CHANNEL DASHBOARD</b>\n\n"

        "â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”\n\n"

        f"ðŸ“¢ <b>Channel:</b> "
        f"{html.escape(channel_title)}\n"

        f"ðŸ†” <b>ID:</b> "
        f"<code>{channel_id}</code>\n"
    )


    if channel_username:

        message += (

            f"ðŸ”— <b>Username:</b> "
            f"@{html.escape(channel_username)}\n"
        )


    message += (

        "\n"

        f"ðŸ“¡ <b>Log:</b> "
        f"{'ðŸŸ¢ Connected' if LOG_CHANNEL_ID else 'ðŸ”´ Disabled'}\n"

        f"ðŸ” <b>Header:</b> "
        f"<code>"
        f"{html.escape(custom_header) if custom_header else 'None'}"
        f"</code>\n"

        f"ðŸ”š <b>Footer:</b> "
        f"<code>"
        f"{html.escape(custom_footer) if custom_footer else 'None'}"
        f"</code>\n\n"

        "ðŸ“Š <b>Replacement Rules:</b>\n"
    )


    if not replacement_rules:

        message += (
            "<i>No rules configured.</i>"
        )


    else:

        for old, rule in (
            replacement_rules.items()
        ):

            tag = (

                "ðŸŸ¢ LIVE"

                if rule_approved(rule)

                else "ðŸ•’ PENDING"
            )


            owner = rule_owner(
                rule
            )


            owner_tag = (

                f" â€” By <code>{owner}</code>"

                if owner

                else ""
            )


            message += (

                f"ðŸ” <code>{html.escape(old)}</code>"
                " âž¡ï¸ "

                f"<code>"
                f"{html.escape(rule_value(rule) or '[REMOVED]')}"
                f"</code>"

                f" â€” {tag}"

                f"{owner_tag}\n"
            )


    await update.message.reply_text(

        message,

        parse_mode="HTML"
    )


# ============================================================
# /DISCONNECT
# ============================================================

async def disconnect_channel(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    user_id = update.effective_user.id


    if not context.args:

        await update.message.reply_text(

            "âœ¨ <b>Format:</b>\n"

            "<code>/disconnect CHANNEL_ID</code>",

            parse_mode="HTML"
        )

        return


    try:

        channel_id = int(
            context.args[0]
        )

    except ValueError:

        await update.message.reply_text(
            "âŒ Invalid Channel ID.",
            parse_mode="HTML"
        )

        return


    if not is_admin(user_id):

        if not user_owns_channel(
            user_id,
            channel_id
        ):

            await update.message.reply_text(

                "â›” <b>Access Denied.</b>",

                parse_mode="HTML"
            )

            return


    update_channel_config(

        channel_id,

        "active",

        False
    )


    if (
        get_selected_channel(user_id)
        == channel_id
    ):

        users_col.update_one(

            {
                "_id": str(user_id)
            },

            {
                "$unset": {
                    "selected_channel": ""
                }
            }
        )


    await update.message.reply_text(

        "ðŸ”´ <b>CHANNEL DISCONNECTED</b>\n\n"

        f"ðŸ†” <code>{channel_id}</code>\n\n"

        "Ab is channel ke posts process nahi honge.",

        parse_mode="HTML"
    )


# ============================================================
# HELP TEXT
# ============================================================

def get_help_text():

    return (

        "â“ <b>AUTO CAPTION ENGINE â€” HELP</b>\n\n"

        "ðŸ¤– <b>Bot kya karta hai?</b>\n"

        "Auto Caption Engine aapke Telegram channel ke "
        "posts/captions ko automatically clean aur edit karta hai.\n\n"

        "â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”\n\n"

        "âž• <b>1. CHANNEL CONNECT</b>\n\n"

        "Bot ko apne Telegram channel mein "
        "<b>Administrator</b> banao.\n\n"

        "Channel automatically detect hokar connect ho jayega. âœ…\n\n"

        "â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”\n\n"

        "ðŸ“¢ <b>2. CHANNEL SELECT</b>\n\n"

        "<code>/channels</code>\n"
        "â†’ Apne connected channels dekho.\n\n"

        "<code>/usechannel CHANNEL_ID</code>\n"
        "â†’ Channel select karo.\n\n"

        "â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”\n\n"

        "ðŸ”„ <b>3. REPLACEMENT RULE</b>\n\n"

        "Example:\n"
        "<code>/addrule MovieHub -> DG_Contents</code>\n\n"

        "Ye rule <b>sirf selected channel</b> ke posts "
        "par apply hoga. ðŸ”’\n\n"

        "â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”\n\n"

        "ðŸ—‘ï¸ <b>4. DELETE RULE</b>\n\n"

        "<code>/delrule MovieHub</code>\n\n"

        "Normal users sirf apne rules delete kar sakte hain.\n\n"

        "â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”\n\n"

        "ðŸŽ¨ <b>5. HEADER / FOOTER</b>\n\n"

        "<code>/setheader Your Header</code>\n"
        "<code>/setfooter Your Footer</code>\n\n"

        "Ye settings selected channel ke liye hoti hain.\n\n"

        "â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”\n\n"

        "ðŸ“Š <b>6. STATUS</b>\n\n"

        "<code>/status</code>\n\n"

        "Channel ke rules, header, footer aur log status dekho.\n\n"

        "â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”\n\n"

        "ðŸ‘‘ <b>7. ADMIN APPROVAL</b>\n\n"

        "Public users ke naye rules approval ke liye pending rahenge.\n\n"

        "<code>/pending</code>\n"
        "<code>/approve old_text</code>\n\n"

        "â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”\n\n"

        "ðŸ§¹ <b>8. AUTOMATIC CLEANING</b>\n\n"

        "Bot unwanted Telegram links aur @mentions ko "
        "automatically clean kar sakta hai.\n\n"

        "â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”\n\n"

        "ðŸ” <b>CHANNEL ISOLATION</b>\n\n"

        "Har connected channel ki settings alag hain.\n\n"

        "Ek channel ka replacement rule "
        "doosre channel par apply nahi hota.\n\n"

        "ðŸ’¡ <i>Need help? Contact Support.</i>"
    )


# ============================================================
# HELP COMMAND
# ============================================================

async def help_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    help_text = get_help_text()


    keyboard = [

        [
            InlineKeyboardButton(
                "âž• Add Me to Your Channel",
                url=(
                    f"https://t.me/"
                    f"{BOT_USERNAME}"
                    f"?startchannel=true"
                )
            )
        ],

        [
            InlineKeyboardButton(
                "ðŸ‘¥ Support",
                url="https://t.me/dghelps_bot"
            )
        ],

        [
            InlineKeyboardButton(
                "ðŸ”™ Back",
                callback_data="back_start"
            )
        ],
    ]


    # Callback query se Help open hua
    if update.callback_query:

        await update.callback_query.edit_message_text(

            text=help_text,

            parse_mode="HTML",

            reply_markup=InlineKeyboardMarkup(
                keyboard
            )
        )

    # /help command
    elif update.message:

        await update.message.reply_text(

            text=help_text,

            parse_mode="HTML",

            reply_markup=InlineKeyboardMarkup(
                keyboard
            )
        )


# ============================================================
# START KEYBOARD
# ============================================================

def get_start_keyboard():

    return InlineKeyboardMarkup(

        [

            [
                InlineKeyboardButton(

                    "âž• Add Me to Your Channel",

                    url=(
                        f"https://t.me/"
                        f"{BOT_USERNAME}"
                        f"?startchannel=true"
                    )
                )
            ],

            [

                InlineKeyboardButton(
                    "ðŸ“¢ Channel",
                    url="https://t.me/dg_contents"
                ),

                InlineKeyboardButton(
                    "ðŸ‘¥ Support",
                    url="https://t.me/dghelps_bot"
                )
            ],

            [

                InlineKeyboardButton(
                    "â“ Help",
                    callback_data="help"
                )
            ],

        ]
    )


# ============================================================
# START COMMAND
# ============================================================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    user_id = update.effective_user.id

    user_name = (
        update.effective_user.first_name
        or "User"
    )


    # ========================================================
    # ADMIN START
    # ========================================================

    if is_admin(user_id):

        welcome_text = (

            f"âš¡ï¸ <b>Welcome, "
            f"{html.escape(user_name)}!</b> "
            f"(Admin)\n\n"

            "ðŸš€ <b>Auto Caption Engine v4.0</b>\n"
            "ðŸ“¡ <b>Channel-Isolated PRO System</b>\n\n"

            "â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”\n\n"

            "ðŸ¤– <b>What I Do</b>\n\n"

            "I automatically clean and edit captions "
            "in your connected Telegram channels.\n\n"

            "ðŸ”’ Every channel has its own separate "
            "rules and settings.\n\n"

            "â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”\n\n"

            "âš¡ <b>Quick Start</b>\n\n"

            "1ï¸âƒ£ Add me to your channel as Admin\n"
            "2ï¸âƒ£ Channel automatically connects\n"
            "3ï¸âƒ£ Use <code>/channels</code>\n"
            "4ï¸âƒ£ Select with <code>/usechannel ID</code>\n"
            "5ï¸âƒ£ Add rules\n\n"

            "ðŸ‘‡ Use the buttons below to get started."
        )


    # ========================================================
    # PUBLIC START
    # ========================================================

    else:

        welcome_text = (

            f"ðŸ‘‹ <b>Hello, "
            f"{html.escape(user_name)}!</b>\n\n"

            "ðŸš€ <b>Auto Caption Engine v4.0</b>\n"
            "âš¡ <i>Smart â€¢ Fast â€¢ Channel-Isolated</i>\n\n"

            "â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”\n\n"

            "ðŸ¤– <b>Automatically manage your captions</b>\n\n"

            "â€¢ ðŸ”„ Replace unwanted words\n"
            "â€¢ ðŸ§¹ Clean unwanted links/mentions\n"
            "â€¢ ðŸŽ¨ Add custom header/footer\n"
            "â€¢ ðŸ”’ Separate settings for every channel\n\n"

            "â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”\n\n"

            "ðŸš€ <b>Get Started</b>\n\n"

            "1ï¸âƒ£ Click <b>Add Me to Your Channel</b>\n"
            "2ï¸âƒ£ Select your channel\n"
            "3ï¸âƒ£ Give me Administrator permission\n"
            "4ï¸âƒ£ I will automatically connect your channel\n\n"

            "After connection, use:\n"
            "<code>/channels</code>\n"
            "<code>/usechannel CHANNEL_ID</code>\n"
            "<code>/addrule old -> new</code>\n\n"

            "ðŸ‘‡ Tap <b>Help</b> if you need a complete guide."
        )


    await update.message.reply_text(

        text=welcome_text,

        parse_mode="HTML",

        reply_markup=get_start_keyboard()
    )


# ============================================================
# BUTTON HANDLER
# ============================================================

async def button_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    query = update.callback_query

    await query.answer()


    # ========================================================
    # HELP
    # ========================================================

    if query.data == "help":

        await help_command(
            update,
            context
        )

        return


    # ========================================================
    # BACK TO START
    # ========================================================

    if query.data == "back_start":

        user_id = query.from_user.id

        user_name = (
            query.from_user.first_name
            or "User"
        )


        if is_admin(user_id):

            welcome_text = (

                f"âš¡ï¸ <b>Welcome, "
                f"{html.escape(user_name)}!</b> "
                f"(Admin)\n\n"

                "ðŸš€ <b>Auto Caption Engine v4.0</b>\n"
                "ðŸ“¡ <b>Channel-Isolated PRO System</b>\n\n"

                "â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”\n\n"

                "ðŸ¤– Automatically clean and edit "
                "your connected channel captions.\n\n"

                "ðŸ”’ Every channel has separate "
                "rules and settings.\n\n"

                "ðŸ‘‡ Choose an option below."
            )

        else:

            welcome_text = (

                f"ðŸ‘‹ <b>Hello, "
                f"{html.escape(user_name)}!</b>\n\n"

                "ðŸš€ <b>Auto Caption Engine v4.0</b>\n\n"

                "ðŸ¤– Smart automatic caption editor.\n\n"

                "âž• Add me to your channel as Admin "
                "and your channel will automatically connect.\n\n"

                "ðŸ”’ Your rules will work only in "
                "your connected channel.\n\n"

                "ðŸ‘‡ Choose an option below."
            )


        await query.edit_message_text(

            text=welcome_text,

            parse_mode="HTML",

            reply_markup=get_start_keyboard()
        )


# ============================================================
# MAIN
# ============================================================

def main():

    # ========================================================
    # RENDER SERVER
    # ========================================================

    threading.Thread(
        target=start_dummy_server,
        daemon=True
    ).start()


    # ========================================================
    # BOT TOKEN
    # ========================================================

    TOKEN = os.environ.get(
        "BOT_TOKEN"
    )


    if not TOKEN:

        logger.error(
            "âŒ BOT_TOKEN environment variable missing."
        )

        sys.exit(1)


    # ========================================================
    # APPLICATION
    # ========================================================

    app = (
        ApplicationBuilder()
        .token(TOKEN)
        .concurrent_updates(CAPTION_WORKERS)
        .build()
    )


    # ========================================================
    # COMMAND HANDLERS
    # ========================================================

    app.add_handler(
        CommandHandler(
            "start",
            start
        )
    )


    app.add_handler(
        CommandHandler(
            "help",
            help_command
        )
    )


    app.add_handler(
        CommandHandler(
            "channels",
            channels_command
        )
    )


    app.add_handler(
        CommandHandler(
            "usechannel",
            use_channel
        )
    )


    app.add_handler(
        CommandHandler(
            "addrule",
            add_rule
        )
    )


    app.add_handler(
        CommandHandler(
            "delrule",
            del_rule
        )
    )


    app.add_handler(
        CommandHandler(
            "setfooter",
            set_footer
        )
    )


    app.add_handler(
        CommandHandler(
            "setheader",
            set_header
        )
    )


    app.add_handler(
        CommandHandler(
            "status",
            status
        )
    )


    app.add_handler(
        CommandHandler(
            "clear",
            clear_rules
        )
    )


    app.add_handler(
        CommandHandler(
            "approve",
            approve_rule
        )
    )


    app.add_handler(
        CommandHandler(
            "pending",
            pending
        )
    )


    app.add_handler(
        CommandHandler(
            "disconnect",
            disconnect_channel
        )
    )


    # ========================================================
    # INLINE BUTTONS
    # ========================================================

    app.add_handler(
        CallbackQueryHandler(
            button_handler
        )
    )


    # ========================================================
    # CHANNEL CONNECTION DETECTOR
    # ========================================================

    app.add_handler(
        ChatMemberHandler(
            handle_bot_channel_status,
            ChatMemberHandler.MY_CHAT_MEMBER
        )
    )


    # ========================================================
    # CHANNEL POSTS
    # ========================================================

    app.add_handler(

        MessageHandler(

            filters.UpdateType.CHANNEL_POST
            | filters.UpdateType.EDITED_CHANNEL_POST,

            edit_channel_caption
        )
    )


    # ========================================================
    # START BOT
    # ========================================================

    logger.info(
        "ðŸ¤– Auto Caption Engine v4.0 starting..."
    )

    logger.info(
        "ðŸ“¡ Channel-isolated mode enabled."
    )

    logger.info(
        f"ðŸ‘‘ Admin IDs: {ADMIN_IDS}"
    )

    logger.info(
        f"ðŸ¤– Bot username: @{BOT_USERNAME}"
    )


    app.run_polling(
        allowed_updates=Update.ALL_TYPES
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()
