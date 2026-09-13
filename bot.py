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
from telegram.error import RetryAfter, TimedOut, NetworkError, BadRequest

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
            logger.info(f"\U0001f310 Dummy server started on port {PORT}")
            httpd.serve_forever()

    except Exception as e:
        logger.warning(f"Dummy server stopped: {e}")


# ============================================================
# MONGODB CONFIGURATION
# ============================================================

MONGO_URI = os.environ.get("MONGO_URI")

if not MONGO_URI:
    logger.error("\u274c MONGO_URI environment variable missing.")
    sys.exit(1)


try:
    db_client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000
    )

    db_client.admin.command("ping")

except Exception as e:
    logger.error(f"\u274c MongoDB connection failed: {e}")
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

# Keep this at 10: fast enough for small bursts while avoiding
# an unnecessarily aggressive number of simultaneous Telegram edits.
CAPTION_WORKERS = 10

# Retry temporary Telegram/network failures.
MAX_EDIT_RETRIES = 4

# Cache each channel's settings briefly.
CONFIG_CACHE_TTL = 30

_caption_semaphore = asyncio.Semaphore(CAPTION_WORKERS)
_config_cache = {}
_config_locks = {}


async def get_cached_channel_config_async(channel_id):
    """Fast channel config lookup without blocking the async event loop."""
    now = time.monotonic()
    cached = _config_cache.get(channel_id)

    if cached and (now - cached["time"]) < CONFIG_CACHE_TTL:
        return cached["config"]

    # Prevent 10 simultaneous posts from all hitting MongoDB at once.
    lock = _config_locks.setdefault(channel_id, asyncio.Lock())

    async with lock:
        now = time.monotonic()
        cached = _config_cache.get(channel_id)

        if cached and (now - cached["time"]) < CONFIG_CACHE_TTL:
            return cached["config"]

        # MongoClient is synchronous, so move it off the event loop.
        config = await asyncio.to_thread(
            get_channel_config,
            channel_id
        )

        _config_cache[channel_id] = {
            "time": time.monotonic(),
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
                f"\u23f3 Telegram rate limit for edit; "
                f"retrying in {wait_time:.1f}s "
                f"(attempt {attempt}/{MAX_EDIT_RETRIES})"
            )

            await asyncio.sleep(wait_time)

        except (TimedOut, NetworkError) as exc:
            last_error = exc
            wait_time = min(
                0.5 * (2 ** (attempt - 1)),
                4.0
            )

            logger.warning(
                f"\U0001f310 Temporary Telegram/network error; "
                f"retrying in {wait_time:.1f}s "
                f"(attempt {attempt}/{MAX_EDIT_RETRIES}): {exc}"
            )

            await asyncio.sleep(wait_time)

        except BadRequest as exc:
            # Telegram returns this when the requested content is
            # already identical. It is a successful no-op, not a failure.
            if "message is not modified" in str(exc).lower():
                logger.info(
                    "\u2139\ufe0f Message already had the requested caption/text."
                )
                return None

            raise

    if last_error:
        raise last_error

    return None


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
            },

            "JoinUs": {
                "new": "SubscribeNow",
                "added_by": None,
            },
        },

        "custom_header": "",

        "custom_footer": "\u26a1\ufe0f Fast Download Links @DG_Contents",
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
            f"\u274c MongoDB get channel config error: {e}"
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
            f"\u274c MongoDB update error: {e}"
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
            f"\u274c User settings error: {e}"
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
            f"\u274c Selected channel update failed: {e}"
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
            f"\u274c Ownership check failed: {e}"
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
            f"\u274c Getting user channels failed: {e}"
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
        f"\U0001f4e1 Channel membership update | "
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
                    f"\u26a0\ufe0f Bot is not admin in {channel_id}"
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

                            "active": False,

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

                config["active"] = False

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
                    "\U0001f389 <b>Thanks! Your channel has been received.</b>\n\n"
                    f"\U0001f4e2 <b>Channel:</b> "
                    f"{html.escape(title or str(channel_id))}\n"
                    f"\U0001f194 <b>Channel ID:</b> "
                    f"<code>{channel_id}</code>\n\n"
                    "\U0001f517 <b>Connect it:</b>\n"
                    f"<code>/connect {channel_id}</code>\n\n"
                    "\u2705 After connecting, use "
                    "<code>/addrule old -> new</code> "
                    "to create your caption rules."
                ),

                parse_mode="HTML"
            )


            logger.info(
                f"\u2705 Channel connected: "
                f"{channel_id} "
                f"owner={actor_user_id}"
            )


        except Exception as e:

            logger.error(
                f"\u274c Channel connection failed: {e}"
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
                f"\U0001f534 Channel disconnected: "
                f"{channel_id}"
            )

        except Exception as e:

            logger.error(
                f"\u274c Channel disconnect error: {e}"
            )



# ============================================================
# /CONNECT
# ============================================================

async def connect_channel(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    user_id = update.effective_user.id

    if not context.args:
        await update.message.reply_text(
            "\u2139\ufe0f <b>Connect Channel</b>\n\n"
            "Use:\n"
            "<code>/connect CHANNEL_ID</code>",
            parse_mode="HTML"
        )
        return

    try:
        channel_id = int(context.args[0].strip())
    except ValueError:
        await update.message.reply_text(
            "\u274c Invalid Channel ID.",
            parse_mode="HTML"
        )
        return

    try:
        channel = channels_col.find_one({"_id": str(channel_id)})

        if not channel:
            await update.message.reply_text(
                "\u274c <b>Channel not found.</b>\n\n"
                "Pehle bot ko channel mein Administrator banao.",
                parse_mode="HTML"
            )
            return

        if channel.get("owner_user_id") != user_id:
            await update.message.reply_text(
                "\u274c <b>Access Denied.</b>\n\n"
                "Ye channel aapke account se linked nahi hai.",
                parse_mode="HTML"
            )
            return

        me = await context.bot.get_me()
        bot_member = await context.bot.get_chat_member(
            chat_id=channel_id,
            user_id=me.id
        )

        if bot_member.status != ChatMemberStatus.ADMINISTRATOR:
            await update.message.reply_text(
                "\u274c Bot ko channel mein Administrator permission chahiye.",
                parse_mode="HTML"
            )
            return

        channels_col.update_one(
            {"_id": str(channel_id)},
            {"$set": {"active": True}}
        )

        invalidate_channel_config_cache(channel_id)
        set_selected_channel(user_id, channel_id)

        title = channel.get("title", "Channel")

        await update.message.reply_text(
            "\u2705 <b>CHANNEL CONNECTED</b>\n\n"
            f"\U0001f4e2 <b>{html.escape(title)}</b>\n"
            f"\U0001f194 <code>{channel_id}</code>\n\n"
            "\U0001f680 Ab is channel ke captions automatically process honge.\n\n"
            "Add a rule:\n"
            "<code>/addrule old -> new</code>",
            parse_mode="HTML"
        )

        logger.info(
            f"\u2705 Channel activated by /connect: "
            f"{channel_id} owner={user_id}"
        )

    except Exception as e:
        logger.exception(f"\u274c Connect command failed: {e}")

        await update.message.reply_text(
            "\u274c <b>Connection failed.</b>\n"
            "Channel ID aur bot permissions check karo.",
            parse_mode="HTML"
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

            "\U0001f4ed <b>No Connected Channels</b>\n\n"

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
        "\U0001f4e2 <b>YOUR CONNECTED CHANNELS</b>\n\n"
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

            " \U0001f7e2 <b>SELECTED</b>"

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

            f"{index}. \U0001f4e2 "
            f"<b>{html.escape(title)}</b>"
            f"{selected_mark}\n"

            f"   \U0001f194 <code>{channel_id}</code>\n"

            f"   \U0001f517 "
            f"{html.escape(username_text)}\n\n"
        )


    message += (

        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\n"

        "\U0001f3af <b>Select Channel:</b>\n"

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

            "\U0001f3af <b>Select Your Channel</b>\n\n"

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
            "\u274c Invalid Channel ID.",
            parse_mode="HTML"
        )

        return


    if not user_owns_channel(
        user_id,
        channel_id
    ):

        await update.message.reply_text(

            "\u26d4 <b>Access Denied</b>\n\n"

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

        "\u2705 <b>CHANNEL SELECTED</b>\n\n"

        f"\U0001f4e2 <b>{html.escape(title)}</b>\n"

        f"\U0001f194 <code>{channel_id}</code>\n\n"

        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\n"

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

            "\u26a0\ufe0f <b>No Channel Selected</b>\n\n"

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

            "\u26d4 <b>Selected Channel Invalid</b>\n\n"

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

    async with _caption_semaphore:
        started = time.monotonic()

        logger.info(
            f"\U0001f4e9 Processing | {channel_id}/{message_id}"
        )

        try:
            # Fast cached config. The synchronous MongoDB operation is
            # moved to a worker thread on a cache miss.
            config = await get_cached_channel_config_async(
                channel_id
            )

            if not config or not config.get("active", True):
                logger.info(
                    f"\u23ed\ufe0f Inactive/unconnected channel: {channel_id}"
                )
                return

            text_to_check = msg.text or msg.caption

            # Media without a caption has nothing to edit.
            if not text_to_check:
                logger.info(
                    f"\u23ed\ufe0f No text/caption: {channel_id}/{message_id}"
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

            final_text = text_to_check

            # Clean links and mentions.
            final_text = re.sub(
                r"(https?://)?t\.me/"
                r"(?!DG_Contents|dghelps_bot)"
                r"[a-zA-Z0-9_]+",
                "",
                final_text,
                flags=re.IGNORECASE
            )

            final_text = re.sub(
                r"@(?!DG_Contents|dghelps_bot)"
                r"[a-zA-Z0-9_]+",
                "",
                final_text,
                flags=re.IGNORECASE
            )

            # Apply every configured rule immediately.
            for old_txt, rule in replacement_rules.items():

                new_txt = rule_value(rule)

                if not old_txt:
                    continue

                final_text = re.sub(
                    re.escape(old_txt),
                    new_txt,
                    final_text,
                    flags=re.IGNORECASE
                )

            final_text = re.sub(
                r" +",
                " ",
                final_text
            ).strip()

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

            final_content = (
                f"{header_part}"
                f"<b>{safe_text}</b>"
                f"{footer_part}"
            )

            # The actual Telegram edit is the important operation.
            # Only one of these branches can run for a message.
            if msg.caption:
                try:
                    current = msg.caption_html or ""

                    if current != final_content:
                        await edit_with_retry(
                            context.bot.edit_message_caption,
                            chat_id=channel_id,
                            message_id=message_id,
                            caption=final_content,
                            parse_mode="HTML"
                        )

                except BadRequest as exc:
                    if "message is not modified" not in str(exc).lower():
                        raise

            elif msg.text:
                try:
                    current = msg.text_html or ""

                    if current != final_content:
                        await edit_with_retry(
                            context.bot.edit_message_text,
                            chat_id=channel_id,
                            message_id=message_id,
                            text=final_content,
                            parse_mode="HTML"
                        )

                except BadRequest as exc:
                    if "message is not modified" not in str(exc).lower():
                        raise

            elapsed = time.monotonic() - started

            logger.info(
                f"\u2705 Done | {channel_id}/{message_id} | "
                f"{elapsed:.2f}s"
            )

            # Logging is deliberately after the edit so it cannot delay
            # the main caption-edit operation.
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
                        f"\u26a0\ufe0f Log copy failed for "
                        f"{channel_id}/{message_id}: {log_error}"
                    )

        except Exception as exc:
            logger.exception(
                f"\u274c FAILED | {channel_id}/{message_id} | {exc}"
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

            "\u2728 <b>ADD REPLACEMENT RULE</b>\n\n"

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
                "\u274c Old text empty nahi ho sakta.",
                parse_mode="HTML"
            )

            return


        if not new_part:

            await update.message.reply_text(
                "\u274c New text empty nahi ho sakta.",
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


        rules[old_part] = {
            "new": new_part,
            "added_by": user_id,
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

        await update.message.reply_text(
            "\u2705 <b>RULE ADDED & LIVE!</b>\n\n"
            f"\U0001f4e2 <b>Channel:</b> "
            f"{html.escape(channel_title)}\n\n"
            f"\U0001f50d <code>{html.escape(old_part)}</code>"
            " \u27a1\ufe0f "
            f"<code>{html.escape(new_part)}</code>\n\n"
            "\u2705 Rule is active now.",
            parse_mode="HTML"
        )



    except Exception as e:

        logger.error(
            f"\u274c Add rule error: {e}"
        )

        await update.message.reply_text(

            "\u274c <b>Error:</b> "
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

            "\u2728 <b>Format:</b>\n"
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

            "\u274c <b>Rule Not Found</b>\n\n"

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

            "\U0001f5d1\ufe0f <b>RULE DELETED</b>\n\n"

            f"<code>{html.escape(old_text)}</code>\n\n"

            "Rule selected channel ke database "
            "se permanently delete ho gaya.",

            parse_mode="HTML"
        )


    else:

        await update.message.reply_text(

            "\u26d4 <b>Access Denied</b>\n\n"

            "Aap sirf apna rule delete kar sakte ho.",

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

            "\u26d4 <b>Access Denied.</b>",

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

        "\U0001f4dd <b>HEADER UPDATED</b>\n\n"

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

            "\u26d4 <b>Access Denied.</b>",

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

        "\U0001f4dd <b>FOOTER UPDATED</b>\n\n"

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

            "\u26d4 <b>Access Denied.</b>",

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

        "\U0001f9f9 <b>RULES CLEARED</b>\n\n"

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

        "\u2699\ufe0f <b>AUTO CAPTION ENGINE</b>\n"
        "\U0001f4ca <b>CHANNEL DASHBOARD</b>\n\n"

        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\n"

        f"\U0001f4e2 <b>Channel:</b> "
        f"{html.escape(channel_title)}\n"

        f"\U0001f194 <b>ID:</b> "
        f"<code>{channel_id}</code>\n"
    )


    if channel_username:

        message += (

            f"\U0001f517 <b>Username:</b> "
            f"@{html.escape(channel_username)}\n"
        )


    message += (

        "\n"

        f"\U0001f4e1 <b>Log:</b> "
        f"{'\U0001f7e2 Connected' if LOG_CHANNEL_ID else '\U0001f534 Disabled'}\n"

        f"\U0001f51d <b>Header:</b> "
        f"<code>"
        f"{html.escape(custom_header) if custom_header else 'None'}"
        f"</code>\n"

        f"\U0001f51a <b>Footer:</b> "
        f"<code>"
        f"{html.escape(custom_footer) if custom_footer else 'None'}"
        f"</code>\n\n"

        "\U0001f4ca <b>Replacement Rules:</b>\n"
    )


    if not replacement_rules:

        message += (
            "<i>No rules configured.</i>"
        )


    else:

        for old, rule in (
            replacement_rules.items()
        ):

            owner = rule_owner(rule)

            owner_tag = (
                f" \u2014 By <code>{owner}</code>"
                if owner
                else ""
            )

            message += (
                f"\U0001f50d <code>{html.escape(old)}</code>"
                " \u27a1\ufe0f "
                f"<code>{html.escape(rule_value(rule) or '[REMOVED]')}</code>"
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

            "\u2728 <b>Format:</b>\n"

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
            "\u274c Invalid Channel ID.",
            parse_mode="HTML"
        )

        return


    if not is_admin(user_id):

        if not user_owns_channel(
            user_id,
            channel_id
        ):

            await update.message.reply_text(

                "\u26d4 <b>Access Denied.</b>",

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

        "\U0001f534 <b>CHANNEL DISCONNECTED</b>\n\n"

        f"\U0001f194 <code>{channel_id}</code>\n\n"

        "Ab is channel ke posts process nahi honge.",

        parse_mode="HTML"
    )


# ============================================================
# HELP TEXT
# ============================================================

def get_help_text():

    return (

        "\u2753 <b>AUTO CAPTION ENGINE \u2014 HELP</b>\n\n"

        "\U0001f916 <b>Bot kya karta hai?</b>\n"

        "Auto Caption Engine aapke Telegram channel ke "
        "posts/captions ko automatically clean aur edit karta hai.\n\n"

        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\n"

        "\u2795 <b>1. CHANNEL CONNECT</b>\n\n"

        "Bot ko apne Telegram channel mein "
        "<b>Administrator</b> banao.\n\n"

        "Bot tumhe Channel ID bhejega. Us ID ke saath:\n"
        "<code>/connect CHANNEL_ID</code>\n\n"

        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\n"

        "\U0001f4e2 <b>2. CHANNEL SELECT</b>\n\n"

        "<code>/channels</code>\n"
        "\u2192 Apne connected channels dekho.\n\n"

        "<code>/usechannel CHANNEL_ID</code>\n"
        "\u2192 Channel select karo.\n\n"

        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\n"

        "\U0001f504 <b>3. REPLACEMENT RULE</b>\n\n"

        "Example:\n"
        "<code>/addrule MovieHub -> DG_Contents</code>\n\n"

        "Ye rule <b>sirf selected channel</b> ke posts "
        "par apply hoga. \U0001f512\n\n"

        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\n"

        "\U0001f5d1\ufe0f <b>4. DELETE RULE</b>\n\n"

        "<code>/delrule MovieHub</code>\n\n"

        "Normal users sirf apne rules delete kar sakte hain.\n\n"

        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\n"

        "\U0001f3a8 <b>5. HEADER / FOOTER</b>\n\n"

        "<code>/setheader Your Header</code>\n"
        "<code>/setfooter Your Footer</code>\n\n"

        "Ye settings selected channel ke liye hoti hain.\n\n"

        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\n"

        "\U0001f4ca <b>6. STATUS</b>\n\n"

        "<code>/status</code>\n\n"

        "Channel ke rules, header, footer aur log status dekho.\n\n"

        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\n"

        "\U0001f451 <b>7. ADMIN APPROVAL</b>\n\n"

        "Public users ke naye rules approval ke liye pending rahenge.\n\n"

        "<code>/pending</code>\n"
        "<code>/approve old_text</code>\n\n"

        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\n"

        "\U0001f9f9 <b>8. AUTOMATIC CLEANING</b>\n\n"

        "Bot unwanted Telegram links aur @mentions ko "
        "automatically clean kar sakta hai.\n\n"

        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\n"

        "\U0001f510 <b>CHANNEL ISOLATION</b>\n\n"

        "Har connected channel ki settings alag hain.\n\n"

        "\U0001f4a1 <i>Need help? Contact Support.</i>"
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
                "\u2795 Add Me to Your Channel",
                url=(
                    f"https://t.me/"
                    f"{BOT_USERNAME}"
                    f"?startchannel=true"
                )
            )
        ],

        [
            InlineKeyboardButton(
                "\U0001f465 Support",
                url="https://t.me/dghelps_bot"
            )
        ],

        [
            InlineKeyboardButton(
                "\U0001f519 Back",
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

                    "\u2795 Add Me to Your Channel",

                    url=(
                        f"https://t.me/"
                        f"{BOT_USERNAME}"
                        f"?startchannel=true"
                    )
                )
            ],

            [

                InlineKeyboardButton(
                    "\U0001f4e2 Channel",
                    url="https://t.me/dg_contents"
                ),

                InlineKeyboardButton(
                    "\U0001f465 Support",
                    url="https://t.me/dghelps_bot"
                )
            ],

            [

                InlineKeyboardButton(
                    "\u2753 Help",
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

            f"\u26a1\ufe0f <b>Welcome, "
            f"{html.escape(user_name)}!</b> "
            f"(Admin)\n\n"

            "\U0001f680 <b>Auto Caption Engine v4.0</b>\n"
            "\U0001f4e1 <b>Channel-Isolated PRO System</b>\n\n"

            "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\n"

            "\U0001f916 <b>What I Do</b>\n\n"

            "I automatically clean and edit captions "
            "in your connected Telegram channels.\n\n"

            "\U0001f512 Every channel has its own separate "
            "rules and settings.\n\n"

            "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\n"

            "\u26a1 <b>Quick Start</b>\n\n"

            "1\ufe0f\u20e3 Add me to your channel as Admin\n"
            "2\ufe0f\u20e3 Channel automatically connects\n"
            "3\ufe0f\u20e3 Use <code>/channels</code>\n"
            "4\ufe0f\u20e3 Select with <code>/usechannel ID</code>\n"
            "5\ufe0f\u20e3 Add rules\n\n"

            "\U0001f447 Use the buttons below to get started."
        )


    # ========================================================
    # PUBLIC START
    # ========================================================

    else:

        welcome_text = (

            f"\U0001f44b <b>Hello, "
            f"{html.escape(user_name)}!</b>\n\n"

            "\U0001f680 <b>Auto Caption Engine v4.0</b>\n"
            "\u26a1 <i>Smart \u2022 Fast \u2022 Channel-Isolated</i>\n\n"

            "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\n"

            "\U0001f916 <b>Automatically manage your captions</b>\n\n"

            "\u2022 \U0001f504 Replace unwanted words\n"
            "\u2022 \U0001f9f9 Clean unwanted links/mentions\n"
            "\u2022 \U0001f3a8 Add custom header/footer\n"
            "\u2022 \U0001f512 Separate settings for every channel\n\n"

            "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\n"

            "\U0001f680 <b>Get Started</b>\n\n"

            "1\ufe0f\u20e3 Click <b>Add Me to Your Channel</b>\n"
            "2\ufe0f\u20e3 Select your channel\n"
            "3\ufe0f\u20e3 Give me Administrator permission\n"
            "4\ufe0f\u20e3 I will send you the Channel ID\n"
            "5\ufe0f\u20e3 Send <code>/connect CHANNEL_ID</code>\n\n"
            "Then add rules with:\n"
            "<code>/addrule old -> new</code>\n\n"

            "\U0001f447 Tap <b>Help</b> if you need a complete guide."
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

                f"\u26a1\ufe0f <b>Welcome, "
                f"{html.escape(user_name)}!</b> "
                f"(Admin)\n\n"

                "\U0001f680 <b>Auto Caption Engine v4.0</b>\n"
                "\U0001f4e1 <b>Channel-Isolated PRO System</b>\n\n"

                "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\n"

                "\U0001f916 Automatically clean and edit "
                "your connected channel captions.\n\n"

                "\U0001f512 Every channel has separate "
                "rules and settings.\n\n"

                "\U0001f447 Choose an option below."
            )

        else:

            welcome_text = (

                f"\U0001f44b <b>Hello, "
                f"{html.escape(user_name)}!</b>\n\n"

                "\U0001f680 <b>Auto Caption Engine v4.0</b>\n\n"

                "\U0001f916 Smart automatic caption editor.\n\n"

                "\u2795 Add me to your channel as Admin.\n"
                "I will send you the Channel ID to connect.\n\n"

                "\U0001f447 Choose an option below."
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
            "\u274c BOT_TOKEN environment variable missing."
        )

        sys.exit(1)


    # ========================================================
    # APPLICATION
    # ========================================================

    app = (
        ApplicationBuilder()
        .token(TOKEN)
        .concurrent_updates(10)
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
            "connect",
            connect_channel
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
        "\U0001f916 Auto Caption Engine v4.0 starting..."
    )

    logger.info(
        "\U0001f4e1 Channel-isolated mode enabled."
    )

    logger.info(
        f"\U0001f451 Admin IDs: {ADMIN_IDS}"
    )

    logger.info(
        f"\U0001f916 Bot username: @{BOT_USERNAME}"
    )


    app.run_polling(
        allowed_updates=Update.ALL_TYPES
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()
