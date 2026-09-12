import os
import sys
import asyncio
import http.server
import socketserver
import threading
import re
import logging
import html

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
    ContextTypes,
    ChatMemberHandler,
    filters,
)
from pymongo import MongoClient


# ============================================================
# LOGGING
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
            logger.info(f"🌐 Dummy server started on port {PORT}")
            httpd.serve_forever()

    except Exception as e:
        logger.warning(f"Dummy server stopped: {e}")


# ============================================================
# MONGODB
# ============================================================

MONGO_URI = os.environ.get("MONGO_URI")

if not MONGO_URI:
    logger.error("❌ MONGO_URI environment variable missing.")
    sys.exit(1)


try:
    db_client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000
    )

    db_client.admin.command("ping")

except Exception as e:
    logger.error(f"❌ MongoDB connection failed: {e}")
    sys.exit(1)


db = db_client["AutoCaptionBotDB"]

# Channel-wise configuration
channels_col = db["connected_channels"]

# User currently selected channel
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

    # If no admins configured = open mode
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
# CHANNEL DATABASE HELPERS
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

        "custom_footer": "⚡️ Fast Download Links @DG_Contents",
    }


def get_channel_config(channel_id):

    try:

        config = channels_col.find_one(
            {
                "_id": str(channel_id)
            }
        )

        if not config:

            config = default_channel_config(channel_id)

            channels_col.insert_one(config)

            return config

        return config

    except Exception as e:

        logger.error(f"MongoDB get channel config error: {e}")

        return default_channel_config(channel_id)


def update_channel_config(channel_id, field_name, field_value):

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

        logger.error(f"MongoDB update error: {e}")


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

        return user.get("selected_channel")

    except Exception as e:

        logger.error(f"User settings error: {e}")

        return None


def set_selected_channel(user_id, channel_id):

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

        logger.error(f"Selected channel update failed: {e}")

        return False


# ============================================================
# CHECK CHANNEL OWNERSHIP
# ============================================================

def user_owns_channel(user_id, channel_id):

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

        logger.error(f"Ownership check failed: {e}")

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

        logger.error(f"Getting user channels failed: {e}")

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

    # Only channels
    if chat.type != ChatType.CHANNEL:
        return

    new_status = chat_member_update.new_chat_member.status

    old_status = chat_member_update.old_chat_member.status

    actor_user_id = chat_member_update.from_user.id

    channel_id = chat.id

    logger.info(
        f"📡 Channel membership update | "
        f"Channel: {channel_id} | "
        f"Old: {old_status} | "
        f"New: {new_status} | "
        f"By: {actor_user_id}"
    )

    # --------------------------------------------------------
    # BOT ADDED / PROMOTED AS ADMIN
    # --------------------------------------------------------

    if new_status == ChatMemberStatus.ADMINISTRATOR:

        try:

            # Verify bot is actually admin
            me = await context.bot.get_me()

            bot_member = await context.bot.get_chat_member(
                chat_id=channel_id,
                user_id=me.id
            )

            if bot_member.status != ChatMemberStatus.ADMINISTRATOR:

                logger.warning(
                    f"⚠️ Bot is not admin in channel {channel_id}"
                )

                return

            title = chat.title or ""

            username = chat.username or ""

            # Existing config?
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

                config = default_channel_config(channel_id)

                config["owner_user_id"] = actor_user_id
                config["title"] = title
                config["username"] = username

                channels_col.insert_one(config)

            # Automatically select this channel
            set_selected_channel(
                actor_user_id,
                channel_id
            )

            await context.bot.send_message(
                chat_id=actor_user_id,

                text=(
                    "✅ <b>CHANNEL CONNECTED SUCCESSFULLY!</b>\n\n"

                    f"📢 <b>Channel:</b> "
                    f"<code>{html.escape(title or str(channel_id))}</code>\n"

                    f"🆔 <b>Channel ID:</b> "
                    f"<code>{channel_id}</code>\n\n"

                    "🎯 Ab is channel ke liye aap:\n"
                    "• <code>/addrule old -> new</code>\n"
                    "• <code>/setheader text</code>\n"
                    "• <code>/setfooter text</code>\n"
                    "• <code>/status</code>\n\n"

                    "⚠️ Ye settings <b>sirf isi connected channel</b> "
                    "par apply hongi.\n\n"

                    "💡 Multiple channels connect karne ke liye "
                    "bot ko doosre channel mein bhi admin bana sakte ho."
                ),

                parse_mode="HTML"
            )

            logger.info(
                f"✅ Channel connected: {channel_id} "
                f"owner={actor_user_id}"
            )

        except Exception as e:

            logger.error(
                f"❌ Channel connection failed: {e}"
            )

    # --------------------------------------------------------
    # BOT REMOVED
    # --------------------------------------------------------

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
                f"🔴 Channel disconnected: {channel_id}"
            )

        except Exception as e:

            logger.error(
                f"Channel disconnect DB error: {e}"
            )


# ============================================================
# /CHANNELS
# ============================================================

async def channels_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    user_id = update.effective_user.id

    user_channels = get_user_channels(user_id)

    if not user_channels:

        await update.message.reply_text(
            "❌ <b>Koi channel connected nahi hai.</b>\n\n"

            "Bot ko apne Telegram channel mein "
            "<b>Administrator</b> ke roop mein add karo.\n\n"

            "Add karne ke baad channel automatically connect ho jayega.",

            parse_mode="HTML"
        )

        return

    selected = get_selected_channel(user_id)

    message = "📢 <b>YOUR CONNECTED CHANNELS</b>\n\n"

    for index, channel in enumerate(
        user_channels,
        start=1
    ):

        channel_id = channel.get("channel_id")

        title = channel.get(
            "title",
            "Unknown Channel"
        )

        username = channel.get(
            "username",
            ""
        )

        selected_mark = (
            " 🟢 <b>SELECTED</b>"
            if selected == channel_id
            else ""
        )

        if username:

            username_text = f"@{username}"

        else:

            username_text = "Private Channel"

        message += (
            f"{index}. 📢 <b>{html.escape(title)}</b>"
            f"{selected_mark}\n"
            f"   🆔 <code>{channel_id}</code>\n"
            f"   🔗 {html.escape(username_text)}\n\n"
        )

    message += (
        "🎯 <b>Channel select karne ke liye:</b>\n"
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
            "✨ <b>Format:</b>\n"
            "<code>/usechannel CHANNEL_ID</code>\n\n"

            "Pehle <code>/channels</code> se apne "
            "connected channel ka ID dekho.",

            parse_mode="HTML"
        )

        return

    raw_channel_id = context.args[0].strip()

    try:

        channel_id = int(raw_channel_id)

    except ValueError:

        await update.message.reply_text(
            "❌ Invalid Channel ID.",
            parse_mode="HTML"
        )

        return

    if not user_owns_channel(
        user_id,
        channel_id
    ):

        await update.message.reply_text(
            "⛔ <b>Access Denied</b>\n\n"
            "Ye channel aapke account se connected nahi hai.",

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
        "✅ <b>Channel Selected!</b>\n\n"
        f"📢 <b>{html.escape(title)}</b>\n"
        f"🆔 <code>{channel_id}</code>\n\n"
        "Ab aapke <code>/addrule</code>, "
        "<code>/setheader</code>, "
        "<code>/setfooter</code> etc. "
        "isi channel par apply honge.",

        parse_mode="HTML"
    )


# ============================================================
# GET COMMAND TARGET CHANNEL
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
            "⚠️ <b>Koi channel selected nahi hai.</b>\n\n"

            "1️⃣ <code>/channels</code> bhejo\n"
            "2️⃣ Apna channel ID dekho\n"
            "3️⃣ <code>/usechannel CHANNEL_ID</code> karo",

            parse_mode="HTML"
        )

        return None

    if not user_owns_channel(
        user_id,
        channel_id
    ):

        await update.message.reply_text(
            "⛔ <b>Selected channel invalid hai.</b>\n\n"
            "Please <code>/channels</code> se channel dobara select karo.",

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

    msg = (
        update.channel_post
        or update.edited_channel_post
    )

    if not msg:
        return

    channel_id = msg.chat_id

    logger.info(
        f"📩 Channel post mila | "
        f"Channel ID: {channel_id} | "
        f"Message ID: {msg.message_id}"
    )

    # --------------------------------------------------------
    # ONLY CONNECTED CHANNELS
    # --------------------------------------------------------

    config = channels_col.find_one(
        {
            "_id": str(channel_id),
            "active": True,
        }
    )

    if not config:

        logger.info(
            f"⏭️ Channel {channel_id} connected nahi hai. Skip."
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

    # --------------------------------------------------------
    # GET TEXT/CAPTION
    # --------------------------------------------------------

    text_to_check = (
        msg.text
        or msg.caption
    )

    if not text_to_check:

        logger.info(
            "⚠️ Post mein text/caption nahi hai."
        )

        return

    final_text = text_to_check

    # --------------------------------------------------------
    # SPAM LINK REMOVER
    # --------------------------------------------------------

    try:

        # t.me links
        final_text = re.sub(
            r"(https?://)?t\.me/(?!DG_Contents|dghelps_bot)"
            r"[a-zA-Z0-9_]+",
            "",
            final_text,
            flags=re.IGNORECASE
        )

        # @username mentions
        final_text = re.sub(
            r"@(?!DG_Contents|dghelps_bot)"
            r"[a-zA-Z0-9_]+",
            "",
            final_text,
            flags=re.IGNORECASE
        )

        # ----------------------------------------------------
        # CHANNEL-SPECIFIC REPLACEMENT RULES
        # ----------------------------------------------------

        for old_txt, rule in replacement_rules.items():

            if not rule_approved(rule):
                continue

            new_txt = rule_value(rule)

            if not old_txt:
                continue

            pattern = re.compile(
                re.escape(old_txt),
                re.IGNORECASE
            )

            final_text = pattern.sub(
                new_txt,
                final_text
            )

        # Clean spaces
        final_text = re.sub(
            r" +",
            " ",
            final_text
        ).strip()

    except Exception as e:

        logger.error(
            f"❌ Text processing failed: {e}"
        )

        return

    # --------------------------------------------------------
    # HTML ESCAPE
    # --------------------------------------------------------

    safe_header = html.escape(
        custom_header
    )

    safe_footer = html.escape(
        custom_footer
    )

    safe_text = html.escape(
        final_text
    )

    # --------------------------------------------------------
    # HEADER + FOOTER
    # --------------------------------------------------------

    header_part = (
        f"<b>{safe_header}</b>\n\n"
        if custom_header
        else ""
    )

    footer_part = (
        f"\n\n<b>{safe_footer}</b>"
        if custom_footer
        else ""
    )

    bold_text = (
        f"{header_part}"
        f"<b>{safe_text}</b>"
        f"{footer_part}"
    )

    # --------------------------------------------------------
    # EDIT MESSAGE
    # --------------------------------------------------------

    try:

        if msg.caption:

            current_caption = msg.caption_html or ""

            if current_caption == bold_text:

                logger.info(
                    "ℹ️ Caption already same hai."
                )

                return

            await context.bot.edit_message_caption(
                chat_id=channel_id,
                message_id=msg.message_id,
                caption=bold_text,
                parse_mode="HTML"
            )

            logger.info(
                "✅ Caption successfully edited."
            )

        elif msg.text:

            current_text = msg.text_html or ""

            if current_text == bold_text:

                logger.info(
                    "ℹ️ Text already same hai."
                )

                return

            await context.bot.edit_message_text(
                chat_id=channel_id,
                message_id=msg.message_id,
                text=bold_text,
                parse_mode="HTML"
            )

            logger.info(
                "✅ Text successfully edited."
            )

        # ----------------------------------------------------
        # LOG CHANNEL
        # ----------------------------------------------------

        if LOG_CHANNEL_ID:

            try:

                await context.bot.copy_message(
                    chat_id=LOG_CHANNEL_ID,
                    from_chat_id=channel_id,
                    message_id=msg.message_id
                )

            except Exception as log_error:

                logger.warning(
                    f"⚠️ Log channel copy failed: {log_error}"
                )

    except Exception as e:

        logger.error(
            f"❌ Caption/Text edit failed: {e}"
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
            "✨ <b>Sahi Format:</b>\n\n"
            "<code>/addrule purana_text -> naya_text</code>\n\n"

            "Example:\n"
            "<code>/addrule MovieHub -> DG_Contents</code>",

            parse_mode="HTML"
        )

        return

    try:

        old_part, new_part = raw_args.split(
            " -> ",
            1
        )

        old_part = old_part.strip()
        new_part = new_part.strip()

        if not old_part:

            await update.message.reply_text(
                "❌ Old text empty nahi ho sakta.",
                parse_mode="HTML"
            )

            return

        if not new_part:

            await update.message.reply_text(
                "❌ New text empty nahi ho sakta.",
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

        # Admin rule = instant live
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
                "✅ <b>Rule Added & LIVE!</b>\n\n"

                f"📢 Channel: "
                f"<b>{html.escape(channel_title)}</b>\n\n"

                f"🔍 <code>{html.escape(old_part)}</code>"
                " ➡️ "
                f"<code>{html.escape(new_part)}</code>\n\n"

                "Ye rule <b>sirf isi channel</b> ke posts "
                "par apply hoga.",

                parse_mode="HTML"
            )

        else:

            await update.message.reply_text(
                "🕒 <b>Rule Submitted for Review</b>\n\n"

                f"📢 Channel: "
                f"<b>{html.escape(channel_title)}</b>\n\n"

                f"🔍 <code>{html.escape(old_part)}</code>"
                " ➡️ "
                f"<code>{html.escape(new_part)}</code>\n\n"

                "Admin approval ke baad ye rule isi "
                "connected channel par live hoga.\n\n"

                "Status check karne ke liye:\n"
                "<code>/status</code>",

                parse_mode="HTML"
            )

    except Exception as e:

        logger.error(
            f"Add rule error: {e}"
        )

        await update.message.reply_text(
            "❌ <b>Error:</b> Kuch galat ho gaya.\n"
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
            "✨ <b>Format:</b>\n"
            "<code>/delrule purana_text</code>",

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
            "❌ <b>Rule nahi mila.</b>\n\n"
            "Case-sensitive exact old text use karo.",

            parse_mode="HTML"
        )

        return

    rule = rules[old_text]

    owner = rule_owner(
        rule
    )

    # Admin can delete anyone's rule
    # Public user can delete only own rule
    if is_admin(user_id) or owner == user_id:

        del rules[old_text]

        update_channel_config(
            channel_id,
            "replacement_rules",
            rules
        )

        await update.message.reply_text(
            "🗑️ <b>Rule Deleted!</b>\n\n"
            f"<code>{html.escape(old_text)}</code>\n\n"
            "Ye rule sirf selected channel ke database "
            "se delete hua hai.",

            parse_mode="HTML"
        )

    else:

        await update.message.reply_text(
            "⛔ <b>Access Denied</b>\n\n"
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
            "⛔ <b>Access Denied:</b>\n"
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
            "✨ <b>Format:</b>\n"
            "<code>/approve purana_text</code>",

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
            "❌ Rule nahi mila.",
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
        "✅ <b>Rule Approved!</b>\n\n"

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
            "⛔ <b>Access Denied.</b>",
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
            "✅ <i>Koi pending rule nahi hai.</i>",
            parse_mode="HTML"
        )

        return

    message = (
        "🕒 <b>PENDING RULES</b>\n\n"
    )

    for old, rule in pending_rules.items():

        owner = rule_owner(
            rule
        )

        message += (
            f"🔍 <code>{html.escape(old)}</code>"
            " ➡️ "
            f"<code>{html.escape(rule_value(rule))}</code>\n"
            f"👤 By: <code>{owner}</code>\n\n"
        )

    message += (
        "Approve:\n"
        "<code>/approve purana_text</code>"
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
            "⛔ <b>Access Denied.</b>",
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
        "📝 <b>Header Updated!</b>\n\n"
        f"<code>{html.escape(header_text) or 'EMPTY'}</code>\n\n"
        "Ye header sirf selected channel par apply hoga.",

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
            "⛔ <b>Access Denied.</b>",
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
        "📝 <b>Footer Updated!</b>\n\n"
        f"<code>{html.escape(footer_text) or 'EMPTY'}</code>\n\n"
        "Ye footer sirf selected channel par apply hoga.",

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
            "⛔ <b>Access Denied.</b>",
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
        "🧹 <b>Rules Cleared!</b>\n\n"
        "Selected channel ke saare replacement rules "
        "MongoDB se clear ho gaye.",

        parse_mode="HTML"
    )


# ============================================================
# /STATUS
# ============================================================

async def status(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    user_id = update.effective_user.id

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
        "⚙️ <b>AUTO CAPTION ENGINE — CHANNEL DASHBOARD</b>\n\n"

        f"📢 <b>Channel:</b> "
        f"{html.escape(channel_title)}\n"

        f"🆔 <b>ID:</b> "
        f"<code>{channel_id}</code>\n"
    )

    if channel_username:

        message += (
            f"🔗 <b>Username:</b> "
            f"@{html.escape(channel_username)}\n"
        )

    message += (
        "\n"
        f"📡 <b>Log Status:</b> "
        f"{'🟢 Connected' if LOG_CHANNEL_ID else '🔴 Disabled'}\n"

        f"🔝 <b>Header:</b> "
        f"<code>{html.escape(custom_header) if custom_header else 'None'}</code>\n"

        f"🔚 <b>Footer:</b> "
        f"<code>{html.escape(custom_footer) if custom_footer else 'None'}</code>\n\n"

        "📊 <b>Replacement Rules:</b>\n"
    )

    if not replacement_rules:

        message += (
            "<i>No rules configured.</i>"
        )

    else:

        for old, rule in replacement_rules.items():

            tag = (
                "🟢 LIVE"
                if rule_approved(rule)
                else "🕒 PENDING"
            )

            owner = rule_owner(
                rule
            )

            owner_tag = (
                f" — By <code>{owner}</code>"
                if owner
                else ""
            )

            message += (
                f"🔍 <code>{html.escape(old)}</code>"
                " ➡️ "
                f"<code>{html.escape(rule_value(rule) or '[REMOVED]')}</code>"
                f" — {tag}"
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
            "✨ <b>Format:</b>\n"
            "<code>/disconnect CHANNEL_ID</code>\n\n"

            "Note: Isse database connection inactive hoga. "
            "Best practice hai bot ko channel se bhi remove kar dena.",

            parse_mode="HTML"
        )

        return

    try:

        channel_id = int(
            context.args[0]
        )

    except ValueError:

        await update.message.reply_text(
            "❌ Invalid Channel ID.",
            parse_mode="HTML"
        )

        return

    # Only owner/admin
    if not is_admin(user_id):

        if not user_owns_channel(
            user_id,
            channel_id
        ):

            await update.message.reply_text(
                "⛔ <b>Access Denied.</b>",
                parse_mode="HTML"
            )

            return

    update_channel_config(
        channel_id,
        "active",
        False
    )

    # If selected, clear selection
    if get_selected_channel(
        user_id
    ) == channel_id:

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
        "🔴 <b>Channel Disconnected.</b>\n\n"
        f"Channel ID: <code>{channel_id}</code>\n\n"
        "Ab is channel ke posts process nahi honge.",

        parse_mode="HTML"
    )


# ============================================================
# /START
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

    if is_admin(user_id):

        welcome_text = (
            f"⚡️ <b>Welcome, {html.escape(user_name)}!</b> "
            "(Admin)\n\n"

            "🚀 <b>Auto Caption Engine v4.0</b>\n"
            "📡 Channel-Isolated PRO System\n\n"

            "🔐 <b>Channel System:</b>\n"
            "• Bot ko channel mein Admin banao\n"
            "• Channel automatically connect hoga\n"
            "• <code>/channels</code> se channels dekho\n"
            "• <code>/usechannel ID</code> se channel select karo\n\n"

            "🛠 <b>Admin Commands:</b>\n"
            "• <code>/addrule old -> new</code>\n"
            "• <code>/delrule old</code>\n"
            "• <code>/pending</code>\n"
            "• <code>/approve old</code>\n"
            "• <code>/setheader text</code>\n"
            "• <code>/setfooter text</code>\n"
            "• <code>/status</code>\n"
            "• <code>/clear</code>\n"
            "• <code>/channels</code>\n"
            "• <code>/usechannel ID</code>\n"
            "• <code>/disconnect ID</code>\n\n"

            "💡 <i>Har channel ki settings alag hain.</i>"
        )

    else:

        welcome_text = (
            f"👋 <b>Hello, {html.escape(user_name)}!</b>\n\n"

            "🚀 <b>Auto Caption Engine v4.0</b>\n\n"

            "🔗 <b>Sabse pehle:</b>\n"
            "Mujhe apne Telegram channel mein "
            "<b>Administrator</b> banao.\n\n"

            "Bot add hote hi channel automatically "
            "aapke account se connect ho jayega. ✅\n\n"

            "🛠 <b>Commands:</b>\n"
            "• <code>/channels</code> — Connected channels\n"
            "• <code>/usechannel ID</code> — Channel select\n"
            "• <code>/addrule old -> new</code> — Rule add\n"
            "• <code>/delrule old</code> — Apna rule delete\n"
            "• <code>/status</code> — Selected channel status\n\n"

            "🔒 <b>Important:</b>\n"
            "Aapke rules <b>sirf aapke connected channel</b> "
            "mein kaam karenge."
        )

    keyboard = [[

        InlineKeyboardButton(
            "📢 Channel",
            url="https://t.me/dg_contents"
        ),

        InlineKeyboardButton(
            "👥 Support",
            url="https://t.me/dghelps_bot"
        )

    ]]

    await update.message.reply_text(
        text=welcome_text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(
            keyboard
        )
    )


# ============================================================
# MAIN
# ============================================================

def main():

    # Render server
    threading.Thread(
        target=start_dummy_server,
        daemon=True
    ).start()

    try:

        loop = asyncio.get_event_loop()

    except RuntimeError:

        loop = asyncio.new_event_loop()

        asyncio.set_event_loop(
            loop
        )

    # --------------------------------------------------------
    # BOT TOKEN
    # --------------------------------------------------------

    TOKEN = os.environ.get(
        "BOT_TOKEN"
    )

    if not TOKEN:

        logger.error(
            "❌ BOT_TOKEN environment variable missing."
        )

        sys.exit(1)

    # --------------------------------------------------------
    # APPLICATION
    # --------------------------------------------------------

    app = (
        ApplicationBuilder()
        .token(TOKEN)
        .build()
    )

    # --------------------------------------------------------
    # COMMANDS
    # --------------------------------------------------------

    app.add_handler(
        CommandHandler(
            "start",
            start
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

    # --------------------------------------------------------
    # CHANNEL CONNECTION DETECTOR
    # --------------------------------------------------------

    app.add_handler(
        ChatMemberHandler(
            handle_bot_channel_status,
            ChatMemberHandler.MY_CHAT_MEMBER
        )
    )

    # --------------------------------------------------------
    # CHANNEL POSTS
    # --------------------------------------------------------

    app.add_handler(
        MessageHandler(
            filters.UpdateType.CHANNEL_POST
            | filters.UpdateType.EDITED_CHANNEL_POST,

            edit_channel_caption
        )
    )

    # --------------------------------------------------------
    # START
    # --------------------------------------------------------

    logger.info(
        "🤖 Auto Caption Engine v4.0 starting..."
    )

    logger.info(
        "📡 Channel-isolated mode enabled."
    )

    logger.info(
        f"👑 Admin IDs: {ADMIN_IDS}"
    )

    app.run_polling(
        allowed_updates=Update.ALL_TYPES
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()
