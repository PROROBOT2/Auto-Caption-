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
from collections import OrderedDict

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.constants import ChatMemberStatus, ChatType
from telegram.error import (
    RetryAfter,
    TimedOut,
    NetworkError,
    BadRequest,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    ChatMemberHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from pymongo import MongoClient


# ============================================================
# UTF-8
# ============================================================

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)


# ============================================================
# RENDER HEALTH SERVER
# ============================================================

def start_dummy_server():
    port = int(os.environ.get("PORT", "10000"))

    class QuietHandler(http.server.SimpleHTTPRequestHandler):
        def log_message(self, format, *args):
            pass

    try:
        with socketserver.TCPServer(("", port), QuietHandler) as httpd:
            logger.info("🌐 Health server started on port %s", port)
            httpd.serve_forever()

    except Exception as exc:
        logger.warning("Health server stopped: %s", exc)


# ============================================================
# ENVIRONMENT
# ============================================================

TOKEN = os.environ.get("BOT_TOKEN", "").strip()
MONGO_URI = os.environ.get("MONGO_URI", "").strip()

BOT_USERNAME = os.environ.get(
    "BOT_USERNAME",
    "DG_Primebot",
).strip()

if not TOKEN:
    logger.error("❌ BOT_TOKEN environment variable missing.")
    sys.exit(1)

if not MONGO_URI:
    logger.error("❌ MONGO_URI environment variable missing.")
    sys.exit(1)


# ============================================================
# MONGODB
# ============================================================

try:
    db_client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
        socketTimeoutMS=10000,
    )

    db_client.admin.command("ping")

except Exception as exc:
    logger.error("❌ MongoDB connection failed: %s", exc)
    sys.exit(1)


db = db_client["AutoCaptionBotDB"]

channels_col = db["connected_channels"]
users_col = db["user_settings"]


# ============================================================
# ADMIN
# ============================================================

def parse_admin_ids():
    raw = os.environ.get("ADMIN_IDS", "").strip()

    ids = set()

    if raw:
        for part in raw.split(","):
            part = part.strip()

            if part.isdigit():
                ids.add(int(part))

    legacy_owner = os.environ.get(
        "OWNER_ID",
        "",
    ).strip()

    if legacy_owner.isdigit():
        owner_id = int(legacy_owner)

        if owner_id != 0:
            ids.add(owner_id)

    return ids


ADMIN_IDS = parse_admin_ids()


def is_admin(user_id: int) -> bool:
    """
    If ADMIN_IDS is empty, preserve old open-mode behavior.
    """

    if not ADMIN_IDS:
        return True

    return user_id in ADMIN_IDS


# ============================================================
# OPTIONAL LOG CHANNEL
# ============================================================

raw_log_id = os.environ.get(
    "LOG_CHANNEL_ID",
    "",
).strip()

try:
    LOG_CHANNEL_ID = int(raw_log_id) if raw_log_id else None
except ValueError:
    LOG_CHANNEL_ID = None


# ============================================================
# CAPTION ENGINE CONFIG
# ============================================================

# IMPORTANT:
#
# Old system:
#
# 10 parallel edits
# 10 parallel edits
# 40 sec cooldown
#
# This can create Telegram 429 bursts.
#
# New system:
#
# ONE edit at a time
# small controlled delay
# RetryAfter respected exactly
#
# This intentionally prioritizes stability over raw burst speed.
# ============================================================

EDIT_INTERVAL_SECONDS = 1.25

MAX_EDIT_RETRIES = 4

QUEUE_MAX_SIZE = 5000

CONFIG_CACHE_TTL = 30.0

RECENT_APPLIED_LIMIT = 5000

RECENT_APPLIED_TTL = 300.0


_caption_queue = asyncio.Queue(
    maxsize=QUEUE_MAX_SIZE
)

_caption_worker_task = None


# Key:
# (channel_id, message_id)
#
# Value:
# newest job
_pending_jobs = {}


# Channel config cache
_config_cache = {}

_config_locks = {}


# Recently applied messages
_recent_applied = OrderedDict()


# ============================================================
# RECENT EDIT CACHE
# ============================================================

def mark_recent_applied(
    channel_id,
    message_id,
    final_plain,
):
    key = (
        channel_id,
        message_id,
    )

    _recent_applied[key] = {
        "text": final_plain,
        "time": time.monotonic(),
    }

    _recent_applied.move_to_end(key)

    while len(_recent_applied) > RECENT_APPLIED_LIMIT:
        _recent_applied.popitem(last=False)


def get_recent_applied(
    channel_id,
    message_id,
):
    key = (
        channel_id,
        message_id,
    )

    item = _recent_applied.get(key)

    if not item:
        return None

    if (
        time.monotonic() - item["time"]
        > RECENT_APPLIED_TTL
    ):
        _recent_applied.pop(
            key,
            None,
        )
        return None

    return item["text"]


# ============================================================
# CONFIG CACHE
# ============================================================

def invalidate_channel_config_cache(channel_id):
    _config_cache.pop(
        channel_id,
        None,
    )


async def get_cached_channel_config_async(
    channel_id,
):
    now = time.monotonic()

    cached = _config_cache.get(
        channel_id
    )

    if (
        cached
        and now - cached["time"]
        < CONFIG_CACHE_TTL
    ):
        return cached["config"]

    lock = _config_locks.setdefault(
        channel_id,
        asyncio.Lock(),
    )

    async with lock:

        now = time.monotonic()

        cached = _config_cache.get(
            channel_id
        )

        if (
            cached
            and now - cached["time"]
            < CONFIG_CACHE_TTL
        ):
            return cached["config"]

        config = await asyncio.to_thread(
            get_channel_config,
            channel_id,
        )

        _config_cache[channel_id] = {
            "time": time.monotonic(),
            "config": config,
        }

        return config


# ============================================================
# IMPORTANT LOGGING
# ============================================================

async def send_important_log(
    bot,
    title,
    body="",
    level="INFO",
):
    """
    Only operational events.
    Routine successful caption edits are NOT sent
    to the Telegram log channel.
    """

    if not LOG_CHANNEL_ID:
        return

    icons = {
        "INFO": "ℹ️",
        "SUCCESS": "✅",
        "WARNING": "⚠️",
        "ERROR": "❌",
        "START": "🟢",
        "STOP": "🔴",
        "USER": "👤",
        "CHANNEL": "📢",
    }

    icon = icons.get(
        level,
        "ℹ️",
    )

    safe_title = html.escape(
        str(title)
    )

    safe_body = html.escape(
        str(body)
    )

    text = (
        f"{icon} <b>{safe_title}</b>"
    )

    if body:
        text += (
            f"\n\n{safe_body}"
        )

    try:
        await bot.send_message(
            chat_id=LOG_CHANNEL_ID,
            text=text,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )

    except Exception as exc:
        logger.warning(
            "⚠️ Important log failed: %s",
            exc,
        )


# ============================================================
# NEW USER TRACKING
# ============================================================

async def log_new_user(
    bot,
    user,
):
    if not user:
        return

    try:
        result = await asyncio.to_thread(
            users_col.update_one,
            {
                "_id": str(user.id)
            },
            {
                "$setOnInsert": {
                    "_id": str(user.id),
                    "created_at": time.time(),
                }
            },
            upsert=True,
        )

        if result.upserted_id is not None:

            name = (
                user.first_name
                or "User"
            )

            username = (
                f"@{user.username}"
                if user.username
                else "No username"
            )

            await send_important_log(
                bot,
                "NEW USER",
                (
                    f"Name: {name}\n"
                    f"Username: {username}\n"
                    f"User ID: {user.id}"
                ),
                "USER",
            )

    except Exception as exc:
        logger.warning(
            "⚠️ New-user tracking failed: %s",
            exc,
        )


# ============================================================
# DEFAULT CHANNEL CONFIG
# ============================================================

def default_channel_config(
    channel_id,
):
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

        "custom_footer": (
            "⚡ Fast Download Links @DG_Contents"
        ),

        "created_at": time.time(),
    }


# ============================================================
# DATABASE HELPERS
# ============================================================

def get_channel_config(
    channel_id,
):
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

            channels_col.update_one(
                {
                    "_id": str(channel_id)
                },
                {
                    "$setOnInsert": config
                },
                upsert=True,
            )

            return config

        return config

    except Exception as exc:

        logger.error(
            "❌ MongoDB get channel config error: %s",
            exc,
        )

        return default_channel_config(
            channel_id
        )


def update_channel_config(
    channel_id,
    field_name,
    field_value,
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
            upsert=True,
        )

    except Exception as exc:

        logger.error(
            "❌ MongoDB update error: %s",
            exc,
        )

    finally:

        invalidate_channel_config_cache(
            channel_id
        )


def get_selected_channel(
    user_id,
):
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

    except Exception as exc:

        logger.error(
            "❌ User settings error: %s",
            exc,
        )

        return None


def set_selected_channel(
    user_id,
    channel_id,
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
            upsert=True,
        )

        return True

    except Exception as exc:

        logger.error(
            "❌ Selected channel update failed: %s",
            exc,
        )

        return False


def user_owns_channel(
    user_id,
    channel_id,
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

    except Exception as exc:

        logger.error(
            "❌ Ownership check failed: %s",
            exc,
        )

        return False


def get_user_channels(
    user_id,
):
    try:

        return list(
            channels_col.find(
                {
                    "owner_user_id": user_id,
                    "active": True,
                }
            )
        )

    except Exception as exc:

        logger.error(
            "❌ Getting user channels failed: %s",
            exc,
        )

        return []


# ============================================================
# RULE HELPERS
# ============================================================

def rule_value(rule):
    if isinstance(rule, dict):
        return rule.get(
            "new",
            "",
        )

    return str(rule)


def rule_owner(rule):
    if isinstance(rule, dict):
        return rule.get(
            "added_by"
        )

    return None


# ============================================================
# CAPTION TRANSFORM
# ============================================================

def transform_caption(
    text,
    config,
):
    """
    Returns:

    final_plain_text
    final_html
    """

    replacement_rules = config.get(
        "replacement_rules",
        {},
    )

    custom_header = config.get(
        "custom_header",
        "",
    ).strip()

    custom_footer = config.get(
        "custom_footer",
        "",
    ).strip()

    final_text = text

    # --------------------------------------------------------
    # Remove unwanted t.me links
    # --------------------------------------------------------

    final_text = re.sub(
        r"(https?://)?t\.me/"
        r"(?!DG_Contents|dghelps_bot)"
        r"[a-zA-Z0-9_]+",
        "",
        final_text,
        flags=re.IGNORECASE,
    )

    # --------------------------------------------------------
    # Remove unwanted @mentions
    # --------------------------------------------------------

    final_text = re.sub(
        r"@(?!DG_Contents|dghelps_bot)"
        r"[a-zA-Z0-9_]+",
        "",
        final_text,
        flags=re.IGNORECASE,
    )

    # --------------------------------------------------------
    # Replacement rules
    # --------------------------------------------------------

    for old_text, rule in replacement_rules.items():

        new_text = rule_value(
            rule
        )

        if not old_text:
            continue

        final_text = re.sub(
            re.escape(old_text),
            lambda _m,
            replacement=new_text: replacement,
            final_text,
            flags=re.IGNORECASE,
        )

    # --------------------------------------------------------
    # Clean spaces
    # --------------------------------------------------------

    final_text = re.sub(
        r"[ \t]+",
        " ",
        final_text,
    )

    final_text = re.sub(
        r"\n[ \t]+",
        "\n",
        final_text,
    )

    final_text = final_text.strip()

    # --------------------------------------------------------
    # Header / Body / Footer
    # --------------------------------------------------------

    parts = []

    if custom_header:
        parts.append(
            custom_header
        )

    if final_text:
        parts.append(
            final_text
        )

    if custom_footer:
        parts.append(
            custom_footer
        )

    final_plain = "\n\n".join(
        parts
    ).strip()

    # --------------------------------------------------------
    # HTML-safe output
    # --------------------------------------------------------

    final_html = html.escape(
        final_plain
    )

    final_html = (
        f"<b>{final_html}</b>"
    )

    return (
        final_plain,
        final_html,
    )


# ============================================================
# SAFE TELEGRAM EDIT
# ============================================================

async def safe_edit(
    bot,
    *,
    channel_id,
    message_id,
    content,
    is_caption,
):
    """
    Performs ONE edit.

    Important:
    RetryAfter is passed to worker.
    MessageNotModified is considered successful.
    """

    for attempt in range(
        1,
        MAX_EDIT_RETRIES + 1,
    ):

        try:

            if is_caption:

                return await bot.edit_message_caption(
                    chat_id=channel_id,
                    message_id=message_id,
                    caption=content,
                    parse_mode="HTML",
                )

            return await bot.edit_message_text(
                chat_id=channel_id,
                message_id=message_id,
                text=content,
                parse_mode="HTML",
            )

        except RetryAfter:
            raise

        except BadRequest as exc:

            error_text = str(
                exc
            ).lower()

            if (
                "message is not modified"
                in error_text
            ):
                logger.info(
                    "⏭️ Message already has requested content | %s/%s",
                    channel_id,
                    message_id,
                )

                return None

            if (
                "message to edit not found"
                in error_text
                or "message can't be edited"
                in error_text
            ):
                raise

            raise

        except (
            TimedOut,
            NetworkError,
        ) as exc:

            if attempt >= MAX_EDIT_RETRIES:
                raise

            wait_time = min(
                1.0 * (
                    2 ** (
                        attempt - 1
                    )
                ),
                8.0,
            )

            logger.warning(
                "🌐 Temporary network error; "
                "retrying in %.1fs (%d/%d): %s",
                wait_time,
                attempt,
                MAX_EDIT_RETRIES,
                exc,
            )

            await asyncio.sleep(
                wait_time
            )

    return None


# ============================================================
# RUN EDIT JOB
# ============================================================

async def run_edit_job(
    job,
):
    bot = job["bot"]

    channel_id = job[
        "channel_id"
    ]

    message_id = job[
        "message_id"
    ]

    content = job[
        "content"
    ]

    is_caption = job[
        "is_caption"
    ]

    final_plain = job[
        "final_plain"
    ]

    started = time.monotonic()

    try:

        await safe_edit(
            bot,
            channel_id=channel_id,
            message_id=message_id,
            content=content,
            is_caption=is_caption,
        )

        elapsed = (
            time.monotonic()
            - started
        )

        mark_recent_applied(
            channel_id,
            message_id,
            final_plain,
        )

        logger.info(
            "✅ EDITED | channel=%s message=%s time=%.2fs",
            channel_id,
            message_id,
            elapsed,
        )

        return (
            "ok",
            job,
            None,
        )

    except RetryAfter as exc:

        retry_after = max(
            1.0,
            float(
                exc.retry_after
            ),
        )

        logger.warning(
            "⏳ Telegram 429 | "
            "channel=%s message=%s wait=%.1fs",
            channel_id,
            message_id,
            retry_after,
        )

        return (
            "rate_limit",
            job,
            retry_after,
        )

    except BadRequest as exc:

        error_text = str(
            exc
        ).lower()

        if (
            "message is not modified"
            in error_text
        ):
            mark_recent_applied(
                channel_id,
                message_id,
                final_plain,
            )

            return (
                "ok",
                job,
                None,
            )

        logger.error(
            "❌ Telegram BadRequest | "
            "channel=%s message=%s | %s",
            channel_id,
            message_id,
            exc,
        )

        return (
            "failed",
            job,
            None,
        )

    except Exception as exc:

        logger.exception(
            "❌ Edit failed | "
            "channel=%s message=%s | %s",
            channel_id,
            message_id,
            exc,
        )

        return (
            "failed",
            job,
            None,
        )


# ============================================================
# QUEUE
# ============================================================

async def enqueue_caption_job(
    job,
):
    """
    Same message ke multiple updates ko merge karta hai.

    Example:

    message 100
    old -> new1

    then:

    message 100
    old -> new2

    Queue mein sirf newest job rahegi.
    """

    key = (
        job["channel_id"],
        job["message_id"],
    )

    if key in _pending_jobs:

        _pending_jobs[key] = job

        logger.info(
            "♻️ Replaced pending job | "
            "channel=%s message=%s",
            job["channel_id"],
            job["message_id"],
        )

        return

    _pending_jobs[key] = job

    await _caption_queue.put(
        key
    )


# ============================================================
# CAPTION WORKER
# ============================================================

async def caption_batch_worker():
    """
    Stable sequential worker.

    ONE edit at a time.

    No asyncio.gather().

    This avoids the previous 10-request burst pattern.
    """

    logger.info(
        "🚀 Stable caption worker online | interval=%.2fs",
        EDIT_INTERVAL_SECONDS,
    )

    while True:

        try:

            key = await _caption_queue.get()

            job = _pending_jobs.pop(
                key,
                None,
            )

            if job is None:

                _caption_queue.task_done()

                continue

            logger.info(
                "📤 Processing edit | "
                "channel=%s message=%s queue=%d",
                job["channel_id"],
                job["message_id"],
                _caption_queue.qsize(),
            )

            result = await run_edit_job(
                job
            )

            status_value = result[0]

            if status_value == "rate_limit":

                retry_after = result[2]

                wait_time = (
                    retry_after
                    + 1.5
                )

                logger.warning(
                    "🛑 RATE LIMIT | waiting %.1fs",
                    wait_time,
                )

                await send_important_log(
                    job["bot"],
                    "TELEGRAM RATE LIMIT",
                    (
                        f"Channel: {job['channel_id']}\n"
                        f"Message: {job['message_id']}\n"
                        f"Waiting: {wait_time:.1f}s"
                    ),
                    "WARNING",
                )

                await asyncio.sleep(
                    wait_time
                )

                # Requeue the same job
                #
                # If another newer update exists for this
                # message, don't overwrite it.

                key = (
                    job["channel_id"],
                    job["message_id"],
                )

                if key not in _pending_jobs:

                    _pending_jobs[key] = job

                    await _caption_queue.put(
                        key
                    )

            # ------------------------------------------------
            # Controlled interval
            # ------------------------------------------------

            await asyncio.sleep(
                EDIT_INTERVAL_SECONDS
            )

            _caption_queue.task_done()

        except asyncio.CancelledError:
            raise

        except Exception as exc:

            logger.exception(
                "❌ Caption worker error: %s",
                exc,
            )

            await asyncio.sleep(
                3
            )


# ============================================================
# START WORKER
# ============================================================

async def start_caption_worker(
    application,
):
    global _caption_worker_task

    if _caption_worker_task is None:

        _caption_worker_task = (
            asyncio.create_task(
                caption_batch_worker()
            )
        )

    logger.info(
        "🚀 Caption worker started."
    )

    await send_important_log(
        application.bot,
        "BOT ONLINE",
        (
            "Premium Auto Caption Engine started.\n"
            f"Edit interval: {EDIT_INTERVAL_SECONDS:.2f}s\n"
            "Mode: Stable Sequential Queue"
        ),
        "START",
    )


# ============================================================
# STOP WORKER
# ============================================================

async def stop_caption_worker(
    application,
):
    global _caption_worker_task

    await send_important_log(
        application.bot,
        "BOT OFFLINE",
        "Caption engine is shutting down.",
        "STOP",
    )

    if (
        _caption_worker_task
        is not None
    ):

        _caption_worker_task.cancel()

        try:
            await _caption_worker_task

        except asyncio.CancelledError:
            pass

        _caption_worker_task = None


# ============================================================
# CHANNEL POST PROCESSOR
# ============================================================

async def edit_channel_caption(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    msg = (
        update.channel_post
        or update.edited_channel_post
    )

    if not msg:
        return

    channel_id = msg.chat_id

    message_id = msg.message_id

    logger.info(
        "📨 RECEIVED | channel=%s message=%s",
        channel_id,
        message_id,
    )

    try:

        config = (
            await get_cached_channel_config_async(
                channel_id
            )
        )

        if not config:
            return

        if not config.get(
            "active",
            True,
        ):
            logger.info(
                "⏭️ Inactive channel: %s",
                channel_id,
            )
            return

        # ----------------------------------------------------
        # Determine original text/caption
        # ----------------------------------------------------

        text_to_check = (
            msg.caption
            if msg.caption is not None
            else msg.text
        )

        if not text_to_check:

            logger.info(
                "⏭️ No caption/text | %s/%s",
                channel_id,
                message_id,
            )

            return

        # ----------------------------------------------------
        # Transform
        # ----------------------------------------------------

        (
            final_plain,
            final_html,
        ) = transform_caption(
            text_to_check,
            config,
        )

        # ----------------------------------------------------
        # LOOP PROTECTION
        # ----------------------------------------------------

        if (
            text_to_check.strip()
            == final_plain
        ):

            logger.info(
                "⏭️ Already correct | %s/%s",
                channel_id,
                message_id,
            )

            mark_recent_applied(
                channel_id,
                message_id,
                final_plain,
            )

            return

        recent = get_recent_applied(
            channel_id,
            message_id,
        )

        if recent == final_plain:

            logger.info(
                "⏭️ Already applied recently | %s/%s",
                channel_id,
                message_id,
            )

            return

        # ----------------------------------------------------
        # Queue
        # ----------------------------------------------------

        is_caption = (
            msg.caption is not None
        )

        job = {
            "bot": context.bot,

            "channel_id": channel_id,

            "message_id": message_id,

            "content": final_html,

            "final_plain": final_plain,

            "is_caption": is_caption,
        }

        await enqueue_caption_job(
            job
        )

        logger.info(
            "📥 QUEUED | %s/%s | queue=%d",
            channel_id,
            message_id,
            _caption_queue.qsize(),
        )

    except asyncio.QueueFull:

        logger.error(
            "❌ Caption queue full. "
            "Dropping job %s/%s",
            channel_id,
            message_id,
        )

    except Exception as exc:

        logger.exception(
            "❌ Caption processing failed | "
            "%s/%s | %s",
            channel_id,
            message_id,
            exc,
        )


# ============================================================
# CHANNEL CONNECTION
# ============================================================

async def handle_bot_channel_status(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    chat_member_update = (
        update.my_chat_member
    )

    if not chat_member_update:
        return

    chat = (
        chat_member_update.chat
    )

    if chat.type != ChatType.CHANNEL:
        return

    new_status = (
        chat_member_update
        .new_chat_member.status
    )

    actor_user_id = (
        chat_member_update
        .from_user.id
    )

    channel_id = chat.id

    logger.info(
        "📡 Channel membership | "
        "channel=%s new=%s by=%s",
        channel_id,
        new_status,
        actor_user_id,
    )

    # ========================================================
    # BOT BECAME ADMIN
    # ========================================================

    if (
        new_status
        == ChatMemberStatus.ADMINISTRATOR
    ):

        try:

            me = await context.bot.get_me()

            bot_member = (
                await context.bot.get_chat_member(
                    chat_id=channel_id,
                    user_id=me.id,
                )
            )

            if (
                bot_member.status
                != ChatMemberStatus.ADMINISTRATOR
            ):

                logger.warning(
                    "⚠️ Bot is not administrator in %s",
                    channel_id,
                )

                return

            title = (
                chat.title
                or ""
            )

            username = (
                chat.username
                or ""
            )

            existing = (
                channels_col.find_one(
                    {
                        "_id": str(channel_id)
                    }
                )
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
                    },
                )

            else:

                config = (
                    default_channel_config(
                        channel_id
                    )
                )

                config[
                    "owner_user_id"
                ] = actor_user_id

                config[
                    "active"
                ] = False

                config[
                    "title"
                ] = title

                config[
                    "username"
                ] = username

                channels_col.insert_one(
                    config
                )

            invalidate_channel_config_cache(
                channel_id
            )

            set_selected_channel(
                actor_user_id,
                channel_id,
            )

            await context.bot.send_message(
                chat_id=actor_user_id,
                text=(
                    "🎉 <b>CHANNEL RECEIVED</b>\n\n"

                    f"📢 <b>Channel:</b> "
                    f"{html.escape(title or str(channel_id))}\n"

                    f"🆔 <b>Channel ID:</b> "
                    f"<code>{channel_id}</code>\n\n"

                    "━━━━━━━━━━━━━━━━━━\n\n"

                    "🔗 <b>Next Step</b>\n\n"

                    f"<code>/connect {channel_id}</code>\n\n"

                    "After connecting, open the premium "
                    "dashboard with:\n\n"

                    "<code>/panel</code>"
                ),
                parse_mode="HTML",
            )

            await send_important_log(
                context.bot,
                "CHANNEL RECEIVED",
                (
                    f"Channel: {title or channel_id}\n"
                    f"Channel ID: {channel_id}\n"
                    f"Owner ID: {actor_user_id}"
                ),
                "CHANNEL",
            )

        except Exception as exc:

            logger.exception(
                "❌ Channel connection failed: %s",
                exc,
            )

    # ========================================================
    # BOT LEFT / BANNED
    # ========================================================

    elif new_status in (
        ChatMemberStatus.LEFT,
        ChatMemberStatus.BANNED,
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
                },
            )

            invalidate_channel_config_cache(
                channel_id
            )

            await send_important_log(
                context.bot,
                "CHANNEL DISCONNECTED",
                f"Channel ID: {channel_id}",
                "STOP",
            )

        except Exception as exc:

            logger.error(
                "❌ Channel disconnect error: %s",
                exc,
            )


# ============================================================
# PREMIUM KEYBOARD
# ============================================================

def get_start_keyboard():
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "➕ Add Me to Your Channel",
                    url=(
                        f"https://t.me/{BOT_USERNAME}"
                        "?startchannel=true"
                    ),
                )
            ],

            [
                InlineKeyboardButton(
                    "📢 Channel",
                    url="https://t.me/dg_contents",
                ),

                InlineKeyboardButton(
                    "👥 Support",
                    url="https://t.me/dghelps_bot",
                ),
            ],

            [
                InlineKeyboardButton(
                    "🎛️ Open Dashboard",
                    callback_data="panel",
                )
            ],

            [
                InlineKeyboardButton(
                    "❓ Help",
                    callback_data="help",
                )
            ],
        ]
    )


def get_panel_keyboard():
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "📢 My Channels",
                    callback_data="channels",
                ),

                InlineKeyboardButton(
                    "📊 Status",
                    callback_data="status",
                ),
            ],

            [
                InlineKeyboardButton(
                    "🔄 Rules",
                    callback_data="rules",
                ),

                InlineKeyboardButton(
                    "🎨 Header/Footer",
                    callback_data="branding",
                ),
            ],

            [
                InlineKeyboardButton(
                    "❓ Help",
                    callback_data="help",
                ),

                InlineKeyboardButton(
                    "🔙 Home",
                    callback_data="home",
                ),
            ],
        ]
    )


# ============================================================
# PREMIUM PANEL TEXT
# ============================================================

def get_panel_text(
    user_id,
):
    selected = get_selected_channel(
        user_id
    )

    channels = get_user_channels(
        user_id
    )

    if selected:
        selected_text = (
            f"<code>{selected}</code>"
        )
    else:
        selected_text = (
            "<i>None selected</i>"
        )

    return (
        "⚡ <b>AUTO CAPTION ENGINE</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"

        "🎛️ <b>PREMIUM CONTROL PANEL</b>\n\n"

        f"📢 <b>Connected Channels:</b> "
        f"<code>{len(channels)}</code>\n"

        f"🎯 <b>Selected Channel:</b> "
        f"{selected_text}\n\n"

        "━━━━━━━━━━━━━━━━━━\n\n"

        "🚀 <b>Engine</b>\n"
        "• Automatic caption editing\n"
        "• Smart replacement rules\n"
        "• Link & mention cleanup\n"
        "• Custom branding\n"
        "• Channel-isolated settings\n"
        "• Stable anti-429 queue\n\n"

        "👇 <b>Choose an option</b>"
    )


# ============================================================
# /START
# ============================================================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    user = update.effective_user

    if not user:
        return

    await log_new_user(
        context.bot,
        user,
    )

    name = (
        user.first_name
        or "User"
    )

    if is_admin(user.id):

        welcome_text = (
            f"⚡ <b>Welcome, "
            f"{html.escape(name)}!</b>\n\n"

            "🚀 <b>Auto Caption Engine</b>\n"
            "<i>Premium Edition</i>\n\n"

            "━━━━━━━━━━━━━━━━━━\n\n"

            "🤖 Automatically manage your "
            "Telegram channel captions.\n\n"

            "✨ <b>Features</b>\n"
            "• Smart word replacement\n"
            "• Unwanted link cleanup\n"
            "• Mention cleanup\n"
            "• Custom header/footer\n"
            "• Multi-channel support\n"
            "• Stable anti-rate-limit engine\n\n"

            "🎯 <b>Quick Start</b>\n\n"
            "1️⃣ Add me to your channel\n"
            "2️⃣ Give Administrator permission\n"
            "3️⃣ Connect the channel\n"
            "4️⃣ Open Dashboard\n"
            "5️⃣ Configure your rules\n\n"

            "👇 <b>Use the buttons below.</b>"
        )

    else:

        welcome_text = (
            f"👋 <b>Hello, "
            f"{html.escape(name)}!</b>\n\n"

            "⚡ <b>Auto Caption Engine</b>\n"
            "<i>Premium Edition</i>\n\n"

            "━━━━━━━━━━━━━━━━━━\n\n"

            "🤖 Your automatic Telegram "
            "caption editor.\n\n"

            "✨ <b>What it can do</b>\n"
            "• 🔄 Replace unwanted words\n"
            "• 🧹 Clean unwanted links\n"
            "• 👤 Clean unwanted mentions\n"
            "• 🎨 Add custom branding\n"
            "• 📢 Manage multiple channels\n"
            "• 🛡️ Stable processing system\n\n"

            "🚀 <b>Getting Started</b>\n\n"
            "Add the bot to your channel as "
            "Administrator, then follow the "
            "instructions sent by the bot."
        )

    await update.message.reply_text(
        text=welcome_text,
        parse_mode="HTML",
        reply_markup=get_start_keyboard(),
    )


# ============================================================
# /PANEL
# ============================================================

async def panel(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    user_id = update.effective_user.id

    await update.message.reply_text(
        text=get_panel_text(
            user_id
        ),
        parse_mode="HTML",
        reply_markup=get_panel_keyboard(),
    )


# ============================================================
# /CONNECT
# ============================================================

async def connect_channel(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    user_id = update.effective_user.id

    if not context.args:

        await update.message.reply_text(
            "📢 <b>Connect Channel</b>\n\n"
            "Use:\n"
            "<code>/connect CHANNEL_ID</code>",
            parse_mode="HTML",
        )

        return

    try:

        channel_id = int(
            context.args[0].strip()
        )

    except ValueError:

        await update.message.reply_text(
            "❌ Invalid Channel ID.",
            parse_mode="HTML",
        )

        return

    try:

        channel = channels_col.find_one(
            {
                "_id": str(channel_id)
            }
        )

        if not channel:

            await update.message.reply_text(
                "❌ <b>Channel Not Found</b>\n\n"
                "Pehle bot ko channel mein "
                "Administrator banao.",
                parse_mode="HTML",
            )

            return

        if (
            channel.get(
                "owner_user_id"
            )
            != user_id
        ):

            await update.message.reply_text(
                "⛔ <b>Access Denied</b>\n\n"
                "Ye channel aapke account se "
                "linked nahi hai.",
                parse_mode="HTML",
            )

            return

        me = await context.bot.get_me()

        bot_member = (
            await context.bot.get_chat_member(
                chat_id=channel_id,
                user_id=me.id,
            )
        )

        if (
            bot_member.status
            != ChatMemberStatus.ADMINISTRATOR
        ):

            await update.message.reply_text(
                "❌ Bot ko channel mein "
                "<b>Administrator</b> permission chahiye.",
                parse_mode="HTML",
            )

            return

        channels_col.update_one(
            {
                "_id": str(channel_id)
            },
            {
                "$set": {
                    "active": True
                }
            },
        )

        invalidate_channel_config_cache(
            channel_id
        )

        set_selected_channel(
            user_id,
            channel_id,
        )

        title = channel.get(
            "title",
            "Channel",
        )

        await update.message.reply_text(
            "✅ <b>CHANNEL CONNECTED</b>\n\n"

            f"📢 <b>{html.escape(title)}</b>\n"
            f"🆔 <code>{channel_id}</code>\n\n"

            "━━━━━━━━━━━━━━━━━━\n\n"

            "🚀 Caption processing is now "
            "<b>ACTIVE</b>.\n\n"

            "🎛️ Open your dashboard:\n"
            "<code>/panel</code>",
            parse_mode="HTML",
        )

        await send_important_log(
            context.bot,
            "CHANNEL ACTIVATED",
            (
                f"Channel: {title}\n"
                f"Channel ID: {channel_id}\n"
                f"Owner: {user_id}"
            ),
            "CHANNEL",
        )

    except Exception as exc:

        logger.exception(
            "❌ Connect command failed: %s",
            exc,
        )

        await update.message.reply_text(
            "❌ <b>Connection Failed</b>\n\n"
            "Channel ID aur bot permissions "
            "check karo.",
            parse_mode="HTML",
        )


# ============================================================
# /CHANNELS
# ============================================================

async def channels_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    user_id = update.effective_user.id

    user_channels = get_user_channels(
        user_id
    )

    if not user_channels:

        await update.message.reply_text(
            "📭 <b>No Connected Channels</b>\n\n"
            "Pehle bot ko apne Telegram channel "
            "mein Administrator ke roop mein add karo.",
            parse_mode="HTML",
        )

        return

    selected = get_selected_channel(
        user_id
    )

    message = (
        "📢 <b>YOUR CHANNELS</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
    )

    keyboard = []

    for channel in user_channels:

        channel_id = channel.get(
            "channel_id"
        )

        title = channel.get(
            "title",
            "Unknown Channel",
        )

        username = channel.get(
            "username",
            "",
        )

        if selected == channel_id:
            mark = "🟢"
        else:
            mark = "⚪"

        username_text = (
            f"@{username}"
            if username
            else "Private Channel"
        )

        message += (
            f"{mark} <b>{html.escape(title)}</b>\n"
            f"   🆔 <code>{channel_id}</code>\n"
            f"   🔗 {html.escape(username_text)}\n\n"
        )

        keyboard.append(
            [
                InlineKeyboardButton(
                    f"🎯 Select {title[:25]}",
                    callback_data=(
                        f"select:{channel_id}"
                    ),
                )
            ]
        )

    keyboard.append(
        [
            InlineKeyboardButton(
                "🔙 Dashboard",
                callback_data="panel",
            )
        ]
    )

    await update.message.reply_text(
        message,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(
            keyboard
        ),
    )


# ============================================================
# /USECHANNEL
# ============================================================

async def use_channel(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    user_id = update.effective_user.id

    if not context.args:

        await update.message.reply_text(
            "🎯 <b>Select Channel</b>\n\n"
            "Use:\n"
            "<code>/usechannel CHANNEL_ID</code>",
            parse_mode="HTML",
        )

        return

    try:

        channel_id = int(
            context.args[0].strip()
        )

    except ValueError:

        await update.message.reply_text(
            "❌ Invalid Channel ID.",
            parse_mode="HTML",
        )

        return

    if not user_owns_channel(
        user_id,
        channel_id,
    ):

        await update.message.reply_text(
            "⛔ <b>Access Denied</b>\n\n"
            "Ye channel aapke account se "
            "connected nahi hai.",
            parse_mode="HTML",
        )

        return

    set_selected_channel(
        user_id,
        channel_id,
    )

    config = get_channel_config(
        channel_id
    )

    title = config.get(
        "title",
        "Channel",
    )

    await update.message.reply_text(
        "✅ <b>CHANNEL SELECTED</b>\n\n"

        f"📢 <b>{html.escape(title)}</b>\n"
        f"🆔 <code>{channel_id}</code>\n\n"

        "Ab dashboard ke settings isi "
        "channel par apply hongi.",
        parse_mode="HTML",
    )


# ============================================================
# GET COMMAND CHANNEL
# ============================================================

async def get_command_channel(
    update,
):
    user_id = update.effective_user.id

    channel_id = get_selected_channel(
        user_id
    )

    if not channel_id:

        await update.message.reply_text(
            "⚠️ <b>No Channel Selected</b>\n\n"
            "Open:\n"
            "<code>/channels</code>",
            parse_mode="HTML",
        )

        return None

    if not user_owns_channel(
        user_id,
        channel_id,
    ):

        await update.message.reply_text(
            "⛔ <b>Selected Channel Invalid</b>\n\n"
            "Please select your channel again.",
            parse_mode="HTML",
        )

        return None

    return channel_id


# ============================================================
# /ADDRULE
# ============================================================

async def add_rule(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
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
            "🔄 <b>ADD REPLACEMENT RULE</b>\n\n"
            "Format:\n"
            "<code>/addrule old -> new</code>\n\n"
            "Example:\n"
            "<code>/addrule MovieHub -> DG_Contents</code>",
            parse_mode="HTML",
        )

        return

    old_part, new_part = raw_args.split(
        " -> ",
        1,
    )

    old_part = old_part.strip()

    new_part = new_part.strip()

    if not old_part:

        await update.message.reply_text(
            "❌ Old text empty nahi ho sakta.",
            parse_mode="HTML",
        )

        return

    if not new_part:

        await update.message.reply_text(
            "❌ New text empty nahi ho sakta.",
            parse_mode="HTML",
        )

        return

    config = get_channel_config(
        channel_id
    )

    rules = config.get(
        "replacement_rules",
        {},
    )

    rules[old_part] = {
        "new": new_part,
        "added_by": user_id,
    }

    update_channel_config(
        channel_id,
        "replacement_rules",
        rules,
    )

    await update.message.reply_text(
        "✅ <b>RULE ADDED</b>\n\n"

        f"🔎 <code>{html.escape(old_part)}</code>"
        " ➡️ "
        f"<code>{html.escape(new_part)}</code>\n\n"

        "🚀 Rule is now active.",
        parse_mode="HTML",
    )


# ============================================================
# /DELRULE
# ============================================================

async def del_rule(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
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
            "🗑️ <b>Delete Rule</b>\n\n"
            "<code>/delrule old_text</code>",
            parse_mode="HTML",
        )

        return

    config = get_channel_config(
        channel_id
    )

    rules = config.get(
        "replacement_rules",
        {},
    )

    if old_text not in rules:

        await update.message.reply_text(
            "❌ <b>Rule Not Found</b>\n\n"
            "Exact old text use karo.",
            parse_mode="HTML",
        )

        return

    owner = rule_owner(
        rules[old_text]
    )

    if (
        is_admin(user_id)
        or owner == user_id
    ):

        del rules[old_text]

        update_channel_config(
            channel_id,
            "replacement_rules",
            rules,
        )

        await update.message.reply_text(
            "🗑️ <b>RULE DELETED</b>\n\n"
            f"<code>{html.escape(old_text)}</code>",
            parse_mode="HTML",
        )

    else:

        await update.message.reply_text(
            "⛔ <b>Access Denied</b>\n\n"
            "Aap sirf apna rule delete "
            "kar sakte ho.",
            parse_mode="HTML",
        )


# ============================================================
# /SETHEADER
# ============================================================

async def set_header(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    user_id = update.effective_user.id

    if not is_admin(user_id):

        await update.message.reply_text(
            "⛔ <b>Access Denied</b>",
            parse_mode="HTML",
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
        header_text,
    )

    await update.message.reply_text(
        "🎨 <b>HEADER UPDATED</b>\n\n"
        f"<code>{html.escape(header_text) or 'EMPTY'}</code>",
        parse_mode="HTML",
    )


# ============================================================
# /SETFOOTER
# ============================================================

async def set_footer(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    user_id = update.effective_user.id

    if not is_admin(user_id):

        await update.message.reply_text(
            "⛔ <b>Access Denied</b>",
            parse_mode="HTML",
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
        footer_text,
    )

    await update.message.reply_text(
        "🎨 <b>FOOTER UPDATED</b>\n\n"
        f"<code>{html.escape(footer_text) or 'EMPTY'}</code>",
        parse_mode="HTML",
    )


# ============================================================
# /CLEAR
# ============================================================

async def clear_rules(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    user_id = update.effective_user.id

    if not is_admin(user_id):

        await update.message.reply_text(
            "⛔ <b>Access Denied</b>",
            parse_mode="HTML",
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
        {},
    )

    await update.message.reply_text(
        "🧹 <b>RULES CLEARED</b>\n\n"
        "Selected channel ke saare "
        "replacement rules clear ho gaye.",
        parse_mode="HTML",
    )


# ============================================================
# STATUS BUILDER
# ============================================================

def build_status_text(
    channel_id,
):
    config = get_channel_config(
        channel_id
    )

    rules = config.get(
        "replacement_rules",
        {},
    )

    title = config.get(
        "title",
        "Unknown",
    )

    username = config.get(
        "username",
        "",
    )

    active = config.get(
        "active",
        False,
    )

    status_text = (
        "🟢 ACTIVE"
        if active
        else "🔴 INACTIVE"
    )

    message = (
        "⚡ <b>AUTO CAPTION ENGINE</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"

        "📊 <b>CHANNEL STATUS</b>\n\n"

        f"📢 <b>Channel:</b> "
        f"{html.escape(title)}\n"

        f"🆔 <b>ID:</b> "
        f"<code>{channel_id}</code>\n"
    )

    if username:
        message += (
            f"🔗 <b>Username:</b> "
            f"@{html.escape(username)}\n"
        )

    message += (
        f"⚙️ <b>Status:</b> {status_text}\n\n"

        "━━━━━━━━━━━━━━━━━━\n\n"

        f"🔄 <b>Rules:</b> "
        f"<code>{len(rules)}</code>\n"

        f"📥 <b>Queue:</b> "
        f"<code>{_caption_queue.qsize()}</code>\n"

        f"⏱️ <b>Edit Interval:</b> "
        f"<code>{EDIT_INTERVAL_SECONDS:.2f}s</code>\n\n"

        "🎨 <b>Branding</b>\n"

        f"Header: "
        f"{'🟢 ON' if config.get('custom_header') else '⚪ OFF'}\n"

        f"Footer: "
        f"{'🟢 ON' if config.get('custom_footer') else '⚪ OFF'}\n\n"

        "🛡️ <b>Protection</b>\n"
        "• Anti-loop: 🟢\n"
        "• Duplicate merge: 🟢\n"
        "• Rate-limit handling: 🟢\n"
        "• Sequential queue: 🟢\n"
    )

    return message


# ============================================================
# /STATUS
# ============================================================

async def status(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    channel_id = await get_command_channel(
        update
    )

    if channel_id is None:
        return

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "🔄 Rules",
                    callback_data="rules",
                ),
                InlineKeyboardButton(
                    "🎨 Branding",
                    callback_data="branding",
                ),
            ],
            [
                InlineKeyboardButton(
                    "🔙 Dashboard",
                    callback_data="panel",
                )
            ],
        ]
    )

    await update.message.reply_text(
        build_status_text(
            channel_id
        ),
        parse_mode="HTML",
        reply_markup=keyboard,
    )


# ============================================================
# /DISCONNECT
# ============================================================

async def disconnect_channel(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    user_id = update.effective_user.id

    if not context.args:

        await update.message.reply_text(
            "🔴 <b>Disconnect Channel</b>\n\n"
            "<code>/disconnect CHANNEL_ID</code>",
            parse_mode="HTML",
        )

        return

    try:

        channel_id = int(
            context.args[0]
        )

    except ValueError:

        await update.message.reply_text(
            "❌ Invalid Channel ID.",
            parse_mode="HTML",
        )

        return

    if not is_admin(user_id):

        if not user_owns_channel(
            user_id,
            channel_id,
        ):

            await update.message.reply_text(
                "⛔ <b>Access Denied</b>",
                parse_mode="HTML",
            )

            return

    update_channel_config(
        channel_id,
        "active",
        False,
    )

    if (
        get_selected_channel(
            user_id
        )
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
            },
        )

    await update.message.reply_text(
        "🔴 <b>CHANNEL DISCONNECTED</b>\n\n"
        f"🆔 <code>{channel_id}</code>\n\n"
        "Posts from this channel will "
        "no longer be processed.",
        parse_mode="HTML",
    )


# ============================================================
# HELP
# ============================================================

def get_help_text():
    return (
        "❓ <b>AUTO CAPTION ENGINE</b>\n"
        "<i>Premium Edition</i>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"

        "🤖 <b>What does it do?</b>\n\n"

        "Automatically processes captions "
        "in your connected Telegram channels.\n\n"

        "✨ <b>Features</b>\n\n"

        "🔄 Word replacement\n"
        "🧹 Link cleanup\n"
        "👤 Mention cleanup\n"
        "🎨 Custom header/footer\n"
        "📢 Multiple channels\n"
        "🛡️ Anti-loop protection\n"
        "⏳ Stable rate-limit handling\n\n"

        "━━━━━━━━━━━━━━━━━━\n\n"

        "📢 <b>CHANNEL</b>\n"
        "<code>/channels</code>\n"
        "<code>/usechannel CHANNEL_ID</code>\n\n"

        "🔄 <b>RULE</b>\n"
        "<code>/addrule old -> new</code>\n"
        "<code>/delrule old</code>\n\n"

        "🎨 <b>BRANDING</b>\n"
        "<code>/setheader Your Header</code>\n"
        "<code>/setfooter Your Footer</code>\n\n"

        "📊 <b>STATUS</b>\n"
        "<code>/status</code>\n\n"

        "🧹 <b>CLEAR RULES</b>\n"
        "<code>/clear</code>\n\n"

        "🎛️ <b>DASHBOARD</b>\n"
        "<code>/panel</code>"
    )


async def help_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "➕ Add Me to Channel",
                    url=(
                        f"https://t.me/{BOT_USERNAME}"
                        "?startchannel=true"
                    ),
                )
            ],

            [
                InlineKeyboardButton(
                    "👥 Support",
                    url="https://t.me/dghelps_bot",
                )
            ],

            [
                InlineKeyboardButton(
                    "🔙 Dashboard",
                    callback_data="panel",
                )
            ],
        ]
    )

    if update.callback_query:

        await update.callback_query.edit_message_text(
            text=get_help_text(),
            parse_mode="HTML",
            reply_markup=keyboard,
        )

    elif update.message:

        await update.message.reply_text(
            text=get_help_text(),
            parse_mode="HTML",
            reply_markup=keyboard,
        )


# ============================================================
# PREMIUM RULES VIEW
# ============================================================

async def rules_view(
    query,
):
    user_id = query.from_user.id

    channel_id = get_selected_channel(
        user_id
    )

    if not channel_id:

        await query.edit_message_text(
            "⚠️ <b>No Channel Selected</b>\n\n"
            "Use /channels first.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "🔙 Dashboard",
                            callback_data="panel",
                        )
                    ]
                ]
            ),
        )

        return

    config = get_channel_config(
        channel_id
    )

    rules = config.get(
        "replacement_rules",
        {},
    )

    text = (
        "🔄 <b>REPLACEMENT RULES</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
    )

    if not rules:

        text += (
            "📭 <i>No rules configured.</i>\n\n"
        )

    else:

        for old, rule in list(
            rules.items()
        )[:30]:

            new = rule_value(
                rule
            )

            text += (
                f"🔎 <code>{html.escape(str(old))}</code>\n"
                f"   ➡️ <code>{html.escape(str(new))}</code>\n\n"
            )

        if len(rules) > 30:
            text += (
                f"<i>Showing first 30 of "
                f"{len(rules)} rules.</i>\n\n"
            )

    text += (
        "➕ Add:\n"
        "<code>/addrule old -> new</code>\n\n"

        "🗑️ Delete:\n"
        "<code>/delrule old</code>"
    )

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "➕ Add Rule",
                    callback_data="rule_help",
                )
            ],

            [
                InlineKeyboardButton(
                    "📊 Status",
                    callback_data="status",
                ),

                InlineKeyboardButton(
                    "🔙 Dashboard",
                    callback_data="panel",
                ),
            ],
        ]
    )

    await query.edit_message_text(
        text=text,
        parse_mode="HTML",
        reply_markup=keyboard,
    )


# ============================================================
# BRANDING VIEW
# ============================================================

async def branding_view(
    query,
):
    user_id = query.from_user.id

    channel_id = get_selected_channel(
        user_id
    )

    if not channel_id:

        await query.edit_message_text(
            "⚠️ <b>No Channel Selected</b>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "🔙 Dashboard",
                            callback_data="panel",
                        )
                    ]
                ]
            ),
        )

        return

    config = get_channel_config(
        channel_id
    )

    header = config.get(
        "custom_header",
        "",
    )

    footer = config.get(
        "custom_footer",
        "",
    )

    header_display = (
        html.escape(header)
        if header
        else "<i>Disabled</i>"
    )

    footer_display = (
        html.escape(footer)
        if footer
        else "<i>Disabled</i>"
    )

    text = (
        "🎨 <b>BRANDING CENTER</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"

        "📌 <b>Header</b>\n"
        f"{header_display}\n\n"

        "📌 <b>Footer</b>\n"
        f"{footer_display}\n\n"

        "━━━━━━━━━━━━━━━━━━\n\n"

        "Use commands:\n\n"

        "📝 <code>/setheader Your Header</code>\n"
        "📝 <code>/setfooter Your Footer</code>\n\n"

        "Empty karne ke liye:\n"
        "<code>/setheader</code>\n"
        "<code>/setfooter</code>"
    )

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "📊 Status",
                    callback_data="status",
                )
            ],

            [
                InlineKeyboardButton(
                    "🔙 Dashboard",
                    callback_data="panel",
                )
            ],
        ]
    )

    await query.edit_message_text(
        text=text,
        parse_mode="HTML",
        reply_markup=keyboard,
    )


# ============================================================
# CALLBACK HANDLER
# ============================================================

async def button_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    query = update.callback_query

    await query.answer()

    data = query.data or ""

    user_id = query.from_user.id

    # --------------------------------------------------------
    # HOME
    # --------------------------------------------------------

    if data == "home":

        name = (
            query.from_user.first_name
            or "User"
        )

        text = (
            f"⚡ <b>Welcome, "
            f"{html.escape(name)}!</b>\n\n"

            "🚀 <b>Auto Caption Engine</b>\n"
            "<i>Premium Edition</i>\n\n"

            "🤖 Smart automatic caption "
            "management for Telegram channels.\n\n"

            "👇 <b>Choose an option</b>"
        )

        await query.edit_message_text(
            text=text,
            parse_mode="HTML",
            reply_markup=get_start_keyboard(),
        )

        return

    # --------------------------------------------------------
    # PANEL
    # --------------------------------------------------------

    if data == "panel":

        await query.edit_message_text(
            text=get_panel_text(
                user_id
            ),
            parse_mode="HTML",
            reply_markup=get_panel_keyboard(),
        )

        return

    # --------------------------------------------------------
    # HELP
    # --------------------------------------------------------

    if data == "help":

        await help_command(
            update,
            context,
        )

        return

    # --------------------------------------------------------
    # CHANNELS
    # --------------------------------------------------------

    if data == "channels":

        user_channels = get_user_channels(
            user_id
        )

        if not user_channels:

            await query.edit_message_text(
                "📭 <b>No Connected Channels</b>\n\n"
                "Add me to a channel as Administrator first.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "➕ Add Me",
                                url=(
                                    f"https://t.me/{BOT_USERNAME}"
                                    "?startchannel=true"
                                ),
                            )
                        ],
                        [
                            InlineKeyboardButton(
                                "🔙 Dashboard",
                                callback_data="panel",
                            )
                        ],
                    ]
                ),
            )

            return

        selected = get_selected_channel(
            user_id
        )

        text = (
            "📢 <b>YOUR CHANNELS</b>\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
        )

        keyboard = []

        for channel in user_channels:

            channel_id = channel.get(
                "channel_id"
            )

            title = channel.get(
                "title",
                "Channel",
            )

            mark = (
                "🟢"
                if selected == channel_id
                else "⚪"
            )

            text += (
                f"{mark} <b>{html.escape(title)}</b>\n"
                f"🆔 <code>{channel_id}</code>\n\n"
            )

            keyboard.append(
                [
                    InlineKeyboardButton(
                        f"🎯 {title[:30]}",
                        callback_data=(
                            f"select:{channel_id}"
                        ),
                    )
                ]
            )

        keyboard.append(
            [
                InlineKeyboardButton(
                    "🔙 Dashboard",
                    callback_data="panel",
                )
            ]
        )

        await query.edit_message_text(
            text=text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(
                keyboard
            ),
        )

        return

    # --------------------------------------------------------
    # SELECT CHANNEL
    # --------------------------------------------------------

    if data.startswith(
        "select:"
    ):

        try:

            channel_id = int(
                data.split(
                    ":",
                    1,
                )[1]
            )

        except Exception:

            await query.answer(
                "Invalid channel.",
                show_alert=True,
            )

            return

        if not user_owns_channel(
            user_id,
            channel_id,
        ):

            await query.answer(
                "Access denied.",
                show_alert=True,
            )

            return

        set_selected_channel(
            user_id,
            channel_id,
        )

        config = get_channel_config(
            channel_id
        )

        title = config.get(
            "title",
            "Channel",
        )

        await query.edit_message_text(
            "✅ <b>CHANNEL SELECTED</b>\n\n"
            f"📢 <b>{html.escape(title)}</b>\n"
            f"🆔 <code>{channel_id}</code>\n\n"
            "All dashboard settings will now "
            "apply to this channel.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "🎛️ Dashboard",
                            callback_data="panel",
                        )
                    ]
                ]
            ),
        )

        return

    # --------------------------------------------------------
    # STATUS
    # --------------------------------------------------------

    if data == "status":

        channel_id = get_selected_channel(
            user_id
        )

        if not channel_id:

            await query.edit_message_text(
                "⚠️ <b>No Channel Selected</b>\n\n"
                "Please select a channel first.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "📢 Channels",
                                callback_data="channels",
                            )
                        ],
                        [
                            InlineKeyboardButton(
                                "🔙 Dashboard",
                                callback_data="panel",
                            )
                        ],
                    ]
                ),
            )

            return

        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "🔄 Rules",
                        callback_data="rules",
                    ),
                    InlineKeyboardButton(
                        "🎨 Branding",
                        callback_data="branding",
                    ),
                ],
                [
                    InlineKeyboardButton(
                        "🔙 Dashboard",
                        callback_data="panel",
                    )
                ],
            ]
        )

        await query.edit_message_text(
            build_status_text(
                channel_id
            ),
            parse_mode="HTML",
            reply_markup=keyboard,
        )

        return

    # --------------------------------------------------------
    # RULES
    # --------------------------------------------------------

    if data == "rules":

        await rules_view(
            query
        )

        return

    # --------------------------------------------------------
    # RULE HELP
    # --------------------------------------------------------

    if data == "rule_help":

        await query.edit_message_text(
            "🔄 <b>ADD REPLACEMENT RULE</b>\n"
            "━━━━━━━━━━━━━━━━━━\n\n"

            "Use this command:\n\n"

            "<code>/addrule old -> new</code>\n\n"

            "Example:\n"
            "<code>/addrule MovieHub -> DG_Contents</code>\n\n"

            "💡 Matching is case-insensitive.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "🔙 Rules",
                            callback_data="rules",
                        )
                    ]
                ]
            ),
        )

        return

    # --------------------------------------------------------
    # BRANDING
    # --------------------------------------------------------

    if data == "branding":

        await branding_view(
            query
        )

        return

    # --------------------------------------------------------
    # UNKNOWN
    # --------------------------------------------------------

    await query.answer(
        "This button is no longer active.",
        show_alert=False,
    )


# ============================================================
# ERROR HANDLER
# ============================================================

async def error_handler(
    update: object,
    context: ContextTypes.DEFAULT_TYPE,
):
    error = context.error

    if isinstance(
        error,
        RetryAfter,
    ):

        logger.warning(
            "⏳ Global RetryAfter: %.1fs",
            float(
                error.retry_after
            ),
        )

        return

    if isinstance(
        error,
        BadRequest,
    ):

        if (
            "message is not modified"
            in str(error).lower()
        ):

            logger.info(
                "⏭️ Ignored MessageNotModified."
            )

            return

    logger.exception(
        "Unhandled Telegram error: %s",
        error,
    )

    try:

        bot = getattr(
            context,
            "bot",
            None,
        )

        if bot:

            await send_important_log(
                bot,
                "BOT ERROR",
                (
                    f"{type(error).__name__}: "
                    f"{error}"
                ),
                "ERROR",
            )

    except Exception as exc:

        logger.warning(
            "⚠️ Could not report global error: %s",
            exc,
        )


# ============================================================
# MAIN
# ============================================================

def main():

    # Render health server
    threading.Thread(
        target=start_dummy_server,
        daemon=True,
    ).start()

    # --------------------------------------------------------
    # Application
    # --------------------------------------------------------

    app = (
        ApplicationBuilder()
        .token(TOKEN)
        .concurrent_updates(10)
        .connection_pool_size(20)
        .pool_timeout(30.0)
        .post_init(
            start_caption_worker
        )
        .post_shutdown(
            stop_caption_worker
        )
        .build()
    )

    # --------------------------------------------------------
    # Commands
    # --------------------------------------------------------

    app.add_handler(
        CommandHandler(
            "start",
            start,
        )
    )

    app.add_handler(
        CommandHandler(
            "help",
            help_command,
        )
    )

    app.add_handler(
        CommandHandler(
            "panel",
            panel,
        )
    )

    app.add_handler(
        CommandHandler(
            "connect",
            connect_channel,
        )
    )

    app.add_handler(
        CommandHandler(
            "channels",
            channels_command,
        )
    )

    app.add_handler(
        CommandHandler(
            "usechannel",
            use_channel,
        )
    )

    app.add_handler(
        CommandHandler(
            "addrule",
            add_rule,
        )
    )

    app.add_handler(
        CommandHandler(
            "delrule",
            del_rule,
        )
    )

    app.add_handler(
        CommandHandler(
            "setheader",
            set_header,
        )
    )

    app.add_handler(
        CommandHandler(
            "setfooter",
            set_footer,
        )
    )

    app.add_handler(
        CommandHandler(
            "status",
            status,
        )
    )

    app.add_handler(
        CommandHandler(
            "clear",
            clear_rules,
        )
    )

    app.add_handler(
        CommandHandler(
            "disconnect",
            disconnect_channel,
        )
    )

    # --------------------------------------------------------
    # Inline buttons
    # --------------------------------------------------------

    app.add_handler(
        CallbackQueryHandler(
            button_handler
        )
    )

    # --------------------------------------------------------
    # Channel membership
    # --------------------------------------------------------

    app.add_handler(
        ChatMemberHandler(
            handle_bot_channel_status,
            ChatMemberHandler.MY_CHAT_MEMBER,
        )
    )

    # --------------------------------------------------------
    # Channel posts
    # --------------------------------------------------------

    app.add_handler(
        MessageHandler(
            filters.UpdateType.CHANNEL_POST
            | filters.UpdateType.EDITED_CHANNEL_POST,
            edit_channel_caption,
        )
    )

    # --------------------------------------------------------
    # Errors
    # --------------------------------------------------------

    app.add_error_handler(
        error_handler
    )

    # --------------------------------------------------------
    # Startup logs
    # --------------------------------------------------------

    logger.info(
        "🤖 Auto Caption Engine Premium starting..."
    )

    logger.info(
        "🛡️ Edit mode: Sequential Stable Queue"
    )

    logger.info(
        "⏱️ Edit interval: %.2fs",
        EDIT_INTERVAL_SECONDS,
    )

    logger.info(
        "📦 Queue max size: %d",
        QUEUE_MAX_SIZE,
    )

    logger.info(
        "👑 Admin IDs: %s",
        ADMIN_IDS,
    )

    logger.info(
        "🤖 Bot username: @%s",
        BOT_USERNAME,
    )

    # --------------------------------------------------------
    # Run
    # --------------------------------------------------------

    app.run_polling(
        allowed_updates=Update.ALL_TYPES
    )


# ============================================================
# ENTRY
# ============================================================

if __name__ == "__main__":
    main()
