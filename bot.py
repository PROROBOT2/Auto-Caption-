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

from telegram.error import RetryAfter, TimedOut, NetworkError, BadRequest
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
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
            logger.info("\U0001f310 Health server started on port %s", port)
            httpd.serve_forever()
    except Exception as exc:
        logger.warning("Health server stopped: %s", exc)


# ============================================================
# ENVIRONMENT
# ============================================================

TOKEN = os.environ.get("BOT_TOKEN", "").strip()
MONGO_URI = os.environ.get("MONGO_URI", "").strip()

BOT_USERNAME = os.environ.get("BOT_USERNAME", "DG_Primebot").strip()

if not TOKEN:
    logger.error("\U0000274c BOT_TOKEN environment variable missing.")
    sys.exit(1)

if not MONGO_URI:
    logger.error("\U0000274c MONGO_URI environment variable missing.")
    sys.exit(1)


# ============================================================
# MONGODB
# ============================================================

try:
    db_client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
    )
    db_client.admin.command("ping")
except Exception as exc:
    logger.error("\U0000274c MongoDB connection failed: %s", exc)
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

    legacy_owner = os.environ.get("OWNER_ID", "").strip()
    if legacy_owner.isdigit() and int(legacy_owner) != 0:
        ids.add(int(legacy_owner))

    return ids


ADMIN_IDS = parse_admin_ids()


def is_admin(user_id: int) -> bool:
    # Same behavior as your old code:
    # if ADMIN_IDS is empty, bot runs in open mode.
    if not ADMIN_IDS:
        return True
    return user_id in ADMIN_IDS


# ============================================================
# OPTIONAL LOG CHANNEL
# ============================================================

raw_log_id = os.environ.get("LOG_CHANNEL_ID", "").strip()

try:
    LOG_CHANNEL_ID = int(raw_log_id) if raw_log_id else None
except ValueError:
    LOG_CHANNEL_ID = None


# ============================================================
# CAPTION ENGINE
#
# IMPORTANT:
# Telegram can return edited_channel_post after our own edit.
# The old code could re-process its own edit and create another
# edit request. This version compares the final plain text first,
# which prevents that loop.
#
# Queue behavior:
#   Up to 10 jobs -> fast batch -> continuous
#
# There is NO artificial cooldown. If Telegram sends RetryAfter,
# the exact server wait is respected.
# ============================================================

BATCH_SIZE = 10

# No artificial cooldown. Jobs are processed continuously.
# Telegram RetryAfter is respected only when Telegram actually
# asks us to slow down.
EDIT_STAGGER_SECONDS = 0.05

MAX_EDIT_RETRIES = 3

QUEUE_MAX_SIZE = 5000
CONFIG_CACHE_TTL = 30.0

_caption_queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
_caption_worker_task = None

# Key = (channel_id, message_id)
# Value = newest job for that message.
_pending_jobs = {}

# Avoid duplicate configuration reads.
_config_cache = {}
_config_locks = {}

# Small in-memory marker for recently applied messages.
# It is only a safety net; MongoDB remains the source of truth.
_recent_applied = OrderedDict()
RECENT_APPLIED_LIMIT = 3000


def mark_recent_applied(channel_id, message_id, final_plain):
    key = (channel_id, message_id)
    _recent_applied[key] = final_plain
    _recent_applied.move_to_end(key)

    while len(_recent_applied) > RECENT_APPLIED_LIMIT:
        _recent_applied.popitem(last=False)


def get_recent_applied(channel_id, message_id):
    return _recent_applied.get((channel_id, message_id))


def invalidate_channel_config_cache(channel_id):
    _config_cache.pop(channel_id, None)


async def get_cached_channel_config_async(channel_id):
    now = time.monotonic()
    cached = _config_cache.get(channel_id)

    if cached and now - cached["time"] < CONFIG_CACHE_TTL:
        return cached["config"]

    lock = _config_locks.setdefault(channel_id, asyncio.Lock())

    async with lock:
        now = time.monotonic()
        cached = _config_cache.get(channel_id)

        if cached and now - cached["time"] < CONFIG_CACHE_TTL:
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


async def safe_edit(bot, *, channel_id, message_id, content, is_caption):
    """
    One Telegram edit with network retry.

    RetryAfter is deliberately NOT swallowed here.
    The batch worker must see it so the whole worker can respect
    Telegram's requested cooldown.
    """

    last_error = None

    for attempt in range(1, MAX_EDIT_RETRIES + 1):
        try:
            if is_caption:
                return await bot.edit_message_caption(
                    chat_id=channel_id,
                    message_id=message_id,
                    caption=content,
                    parse_mode="HTML",
                    read_timeout=10.0,
                    write_timeout=10.0,
                    connect_timeout=5.0,
                    pool_timeout=5.0,
                )

            return await bot.edit_message_text(
                chat_id=channel_id,
                message_id=message_id,
                text=content,
                parse_mode="HTML",
                read_timeout=10.0,
                write_timeout=10.0,
                connect_timeout=5.0,
                pool_timeout=5.0,
            )

        except RetryAfter:
            raise

        except (TimedOut, NetworkError) as exc:
            last_error = exc

            wait_time = min(
                0.5 * (2 ** (attempt - 1)),
                2.0,
            )

            logger.warning(
                "\U0001f310 Temporary Telegram/network error; "
                "retrying in %.1fs (%d/%d): %s",
                wait_time,
                attempt,
                MAX_EDIT_RETRIES,
                exc,
            )

            await asyncio.sleep(wait_time)

        except BadRequest as exc:
            text = str(exc).lower()

            if "message is not modified" in text:
                return None

            raise

    if last_error:
        raise last_error

    return None



async def send_log_event(bot, title, details="", level="INFO"):
    """Send ONLY important events to LOG_CHANNEL_ID.

    Intentionally does NOT log every received/queued/edited post.
    This keeps the Telegram log channel clean and useful.
    """
    if not LOG_CHANNEL_ID:
        return

    icons = {
        "INFO": "\U0001f7e2",
        "SUCCESS": "\U00002705",
        "WARNING": "\U000026a0\U0000fe0f",
        "ERROR": "\U0000274c",
        "ONLINE": "\U0001f916",
        "OFFLINE": "\U0001f534",
    }

    icon = icons.get(level, "\U0001f4cc")
    safe_title = html.escape(str(title))
    safe_details = html.escape(str(details)) if details else ""

    text = f"{icon} <b>{safe_title}</b>"
    if safe_details:
        text += f"\n\n{safe_details}"

    try:
        await bot.send_message(
            chat_id=LOG_CHANNEL_ID,
            text=text,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception as exc:
        logger.warning(
            "Could not send important log event: %s",
            exc,
        )


async def run_edit_job(job):
    bot = job["bot"]
    channel_id = job["channel_id"]
    message_id = job["message_id"]
    content = job["content"]
    is_caption = job["is_caption"]
    final_plain = job["final_plain"]

    started = time.monotonic()

    try:
        await safe_edit(
            bot,
            channel_id=channel_id,
            message_id=message_id,
            content=content,
            is_caption=is_caption,
        )

        elapsed = time.monotonic() - started

        mark_recent_applied(
            channel_id,
            message_id,
            final_plain,
        )

        logger.info(
            "\U00002705 EDITED | channel=%s message=%s time=%.2fs",
            channel_id,
            message_id,
            elapsed,
        )

        # Log metadata/result only \U00002014 never forward the actual media/file.
        await send_edit_log(
            bot,
            job,
            status="SUCCESS",
        )

        return "ok", job, None

    except RetryAfter as exc:
        retry_after = max(
            1.0,
            float(exc.retry_after),
        )

        logger.warning(
            "\U000023f3 Telegram 429 | channel=%s message=%s wait=%.1fs",
            channel_id,
            message_id,
            retry_after,
        )

        return "rate_limit", job, retry_after

    except BadRequest as exc:
        if "message is not modified" in str(exc).lower():
            mark_recent_applied(
                channel_id,
                message_id,
                final_plain,
            )
            return "ok", job, None

        logger.error(
            "\U0000274c Telegram BadRequest | channel=%s message=%s | %s",
            channel_id,
            message_id,
            exc,
        )
        await send_log_event(
            bot,
            "CAPTION EDIT FAILED",
            f"Channel: {channel_id}\nMessage: {message_id}\nError: {exc}",
            level="ERROR",
        )
        return "failed", job, None

    except Exception as exc:
        logger.exception(
            "\U0000274c Edit failed | channel=%s message=%s | %s",
            channel_id,
            message_id,
            exc,
        )
        await send_log_event(
            bot,
            "CAPTION EDIT FAILED",
            f"Channel: {channel_id}\nMessage: {message_id}\nError: {exc}",
            level="ERROR",
        )
        return "failed", job, None


async def enqueue_caption_job(job):
    """
    Deduplicate jobs for the same message.
    If Telegram sends several updates before the worker gets there,
    only the newest version is kept.
    """
    key = (
        job["channel_id"],
        job["message_id"],
    )

    if key in _pending_jobs:
        _pending_jobs[key] = job
        logger.info(
            "\U0000267b\U0000fe0f Replaced pending job | channel=%s message=%s",
            job["channel_id"],
            job["message_id"],
        )
        return

    _pending_jobs[key] = job
    await _caption_queue.put(key)


async def caption_batch_worker():
    """Continuously process up to BATCH_SIZE edits without an artificial cooldown."""

    while True:
        try:
            first_key = await _caption_queue.get()
            keys = [first_key]

            for _ in range(BATCH_SIZE - 1):
                try:
                    keys.append(_caption_queue.get_nowait())
                except asyncio.QueueEmpty:
                    break

            jobs = []
            for key in keys:
                job = _pending_jobs.pop(key, None)
                if job is not None:
                    jobs.append(job)

            if not jobs:
                for _ in keys:
                    _caption_queue.task_done()
                continue

            logger.info(
                "\U0001f680 EDIT BATCH | size=%d | queue=%d",
                len(jobs),
                _caption_queue.qsize(),
            )

            async def delayed_job(index, job):
                if index:
                    await asyncio.sleep(EDIT_STAGGER_SECONDS * index)
                return await run_edit_job(job)

            results = await asyncio.gather(
                *[delayed_job(i, job) for i, job in enumerate(jobs)],
                return_exceptions=False,
            )

            for _ in keys:
                _caption_queue.task_done()

            # Only jobs that Telegram explicitly rate-limited are retried.
            # A rate limit must not create an artificial 40-second pause.
            rate_limited = [
                (result[1], result[2])
                for result in results
                if result[0] == "rate_limit"
            ]

            if rate_limited:
                server_wait = max(wait for _, wait in rate_limited)
                wait_time = server_wait + 0.25

                logger.warning(
                    "\U0001f6d1 Telegram rate limit; retrying %d job(s) in %.1fs",
                    len(rate_limited),
                    wait_time,
                )

                await send_log_event(
                    jobs[0]["bot"],
                    "TELEGRAM RATE LIMIT",
                    f"{len(rate_limited)} edit(s) delayed by Telegram\nWait: {wait_time:.1f}s",
                    level="WARNING",
                )

                await asyncio.sleep(wait_time)
                for job, _ in rate_limited:
                    await enqueue_caption_job(job)

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception(
                "\U0000274c Caption worker error: %s",
                exc,
            )
            # Worker errors are important; normal post activity is not.
            try:
                if "jobs" in locals() and jobs:
                    await send_log_event(
                        jobs[0]["bot"],
                        "CAPTION WORKER ERROR",
                        str(exc),
                        level="ERROR",
                    )
            except Exception:
                pass
            await asyncio.sleep(1)

async def start_caption_worker(application):
    global _caption_worker_task

    if _caption_worker_task is None:
        _caption_worker_task = asyncio.create_task(
            caption_batch_worker()
        )

    logger.info(
        "\U0001f680 Caption worker started | continuous batch=%d | stagger=%.2fs",
        BATCH_SIZE,
        EDIT_STAGGER_SECONDS,
    )

    await send_log_event(
        application.bot,
        "BOT ONLINE",
        f"Auto Caption Engine started\nBatch: {BATCH_SIZE}\nStagger: {EDIT_STAGGER_SECONDS:.2f}s",
        level="ONLINE",
    )


async def stop_caption_worker(application):
    global _caption_worker_task

    # This catches graceful shutdowns/redeploys. A hard crash cannot send
    # a final Telegram message, so the next BOT ONLINE event is the signal.
    await send_log_event(
        application.bot,
        "BOT OFFLINE",
        "Auto Caption Engine is shutting down/restarting.",
        level="OFFLINE",
    )

    if _caption_worker_task is not None:
        _caption_worker_task.cancel()

        try:
            await _caption_worker_task
        except asyncio.CancelledError:
            pass

        _caption_worker_task = None


# ============================================================
# RULE HELPERS
# ============================================================

def rule_value(rule):
    if isinstance(rule, dict):
        return rule.get("new", "")
    return str(rule)


def rule_owner(rule):
    if isinstance(rule, dict):
        return rule.get("added_by")
    return None


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
        "custom_footer": "\U000026a1 Fast Download Links @DG_Contents",
    }


# ============================================================
# DATABASE HELPERS
# ============================================================

def get_channel_config(channel_id):
    try:
        config = channels_col.find_one(
            {"_id": str(channel_id)}
        )

        if not config:
            config = default_channel_config(channel_id)

            channels_col.update_one(
                {"_id": str(channel_id)},
                {"$setOnInsert": config},
                upsert=True,
            )

            return config

        return config

    except Exception as exc:
        logger.error(
            "\U0000274c MongoDB get channel config error: %s",
            exc,
        )
        return default_channel_config(channel_id)


def update_channel_config(channel_id, field_name, field_value):
    try:
        channels_col.update_one(
            {"_id": str(channel_id)},
            {"$set": {field_name: field_value}},
            upsert=True,
        )
    except Exception as exc:
        logger.error(
            "\U0000274c MongoDB update error: %s",
            exc,
        )
    finally:
        invalidate_channel_config_cache(channel_id)


def get_selected_channel(user_id):
    try:
        user = users_col.find_one(
            {"_id": str(user_id)}
        )

        if not user:
            return None

        return user.get("selected_channel")

    except Exception as exc:
        logger.error(
            "\U0000274c User settings error: %s",
            exc,
        )
        return None


def set_selected_channel(user_id, channel_id):
    try:
        users_col.update_one(
            {"_id": str(user_id)},
            {"$set": {"selected_channel": channel_id}},
            upsert=True,
        )
        return True
    except Exception as exc:
        logger.error(
            "\U0000274c Selected channel update failed: %s",
            exc,
        )
        return False


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
    except Exception as exc:
        logger.error(
            "\U0000274c Ownership check failed: %s",
            exc,
        )
        return False


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
    except Exception as exc:
        logger.error(
            "\U0000274c Getting user channels failed: %s",
            exc,
        )
        return []


# ============================================================
# CAPTION TRANSFORM
# ============================================================

def transform_caption(text, config):
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

    # Remove t.me links except allowed links.
    final_text = re.sub(
        r"(https?://)?t\.me/"
        r"(?!DG_Contents|dghelps_bot)"
        r"[a-zA-Z0-9_]+",
        "",
        final_text,
        flags=re.IGNORECASE,
    )

    # Remove @mentions except allowed usernames.
    final_text = re.sub(
        r"@(?!DG_Contents|dghelps_bot)"
        r"[a-zA-Z0-9_]+",
        "",
        final_text,
        flags=re.IGNORECASE,
    )

    # Replacement rules.
    for old_text, rule in replacement_rules.items():
        new_text = rule_value(rule)

        if not old_text:
            continue

        final_text = re.sub(
            re.escape(old_text),
            lambda _m, replacement=new_text: replacement,
            final_text,
            flags=re.IGNORECASE,
        )

    # Clean excessive spaces but preserve line structure.
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

    parts = []

    if custom_header:
        parts.append(custom_header)

    if final_text:
        parts.append(final_text)

    if custom_footer:
        parts.append(custom_footer)

    final_plain = "\n\n".join(parts).strip()

    # Escape all user/config text before putting it into HTML.
    final_html = html.escape(final_plain)

    # Keep the old visual behavior: complete result is bold.
    final_html = f"<b>{final_html}</b>"

    return final_plain, final_html


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
        "\U0001f4e8 RECEIVED | channel=%s message=%s",
        channel_id,
        message_id,
    )

    try:
        config = await get_cached_channel_config_async(
            channel_id
        )

        if not config or not config.get("active", True):
            logger.info(
                "\U000023ed\U0000fe0f Inactive channel: %s",
                channel_id,
            )
            return

        text_to_check = (
            msg.caption
            if msg.caption is not None
            else msg.text
        )

        if not text_to_check:
            logger.info(
                "\U000023ed\U0000fe0f No text/caption | %s/%s",
                channel_id,
                message_id,
            )
            return

        final_plain, final_html = transform_caption(
            text_to_check,
            config,
        )

        # This is the important loop-prevention check.
        # We compare plain text, not caption_html/text_html.
        if text_to_check.strip() == final_plain:
            logger.info(
                "\U000023ed\U0000fe0f Already correct | %s/%s",
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
                "\U000023ed\U0000fe0f Already applied recently | %s/%s",
                channel_id,
                message_id,
            )
            return

        is_caption = msg.caption is not None

        logger.info(
            "\U0001f50e TRANSFORMED | channel=%s message=%s | caption=%s",
            channel_id,
            message_id,
            "yes" if is_caption else "no",
        )

        job = {
            "bot": context.bot,
            "channel_id": channel_id,
            "message_id": message_id,
            "content": final_html,
            "final_plain": final_plain,
            "is_caption": is_caption,
        }

        await enqueue_caption_job(job)

        logger.info(
            "\U0001f4e5 QUEUED | %s/%s | queue=%d",
            channel_id,
            message_id,
            _caption_queue.qsize(),
        )

    except asyncio.QueueFull:
        logger.error(
            "\U0000274c Caption queue full. "
            "Dropping newest job %s/%s",
            channel_id,
            message_id,
        )

    except Exception as exc:
        logger.exception(
            "\U0000274c Caption processing failed | %s/%s | %s",
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
    chat_member_update = update.my_chat_member

    if not chat_member_update:
        return

    chat = chat_member_update.chat

    if chat.type != ChatType.CHANNEL:
        return

    new_status = chat_member_update.new_chat_member.status
    old_status = chat_member_update.old_chat_member.status
    actor_user_id = chat_member_update.from_user.id
    channel_id = chat.id

    logger.info(
        "\U0001f4e1 Channel membership | channel=%s old=%s new=%s by=%s",
        channel_id,
        old_status,
        new_status,
        actor_user_id,
    )

    if new_status == ChatMemberStatus.ADMINISTRATOR:
        try:
            me = await context.bot.get_me()

            bot_member = await context.bot.get_chat_member(
                chat_id=channel_id,
                user_id=me.id,
            )

            if bot_member.status != ChatMemberStatus.ADMINISTRATOR:
                logger.warning(
                    "\U000026a0\U0000fe0f Bot is not administrator in %s",
                    channel_id,
                )
                return

            title = chat.title or ""
            username = chat.username or ""

            existing = channels_col.find_one(
                {"_id": str(channel_id)}
            )

            if existing:
                channels_col.update_one(
                    {"_id": str(channel_id)},
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
                config = default_channel_config(
                    channel_id
                )

                config["owner_user_id"] = actor_user_id
                config["active"] = False
                config["title"] = title
                config["username"] = username

                channels_col.insert_one(config)

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
                    "\U0001f389 <b>Thanks! Your channel has been received.</b>\n\n"
                    f"\U0001f4e2 <b>Channel:</b> "
                    f"{html.escape(title or str(channel_id))}\n"
                    f"\U0001f194 <b>Channel ID:</b> "
                    f"<code>{channel_id}</code>\n\n"
                    "\U0001f517 <b>Connect it:</b>\n"
                    f"<code>/connect {channel_id}</code>\n\n"
                    "\U00002705 After connecting, use:\n"
                    "<code>/addrule old -> new</code>"
                ),
                parse_mode="HTML",
            )

            logger.info(
                "\U00002705 Channel received: %s owner=%s",
                channel_id,
                actor_user_id,
            )

            await send_log_event(
                context.bot,
                "CHANNEL ADDED",
                f"Channel: {title or channel_id}\nID: {channel_id}\nOwner: {actor_user_id}",
                level="SUCCESS",
            )

        except Exception as exc:
            logger.exception(
                "\U0000274c Channel connection failed: %s",
                exc,
            )

    elif new_status in (
        ChatMemberStatus.LEFT,
        ChatMemberStatus.BANNED,
    ):
        try:
            channels_col.update_one(
                {"_id": str(channel_id)},
                {"$set": {"active": False}},
            )

            invalidate_channel_config_cache(
                channel_id
            )

            logger.info(
                "\U0001f534 Channel disconnected: %s",
                channel_id,
            )

            await send_log_event(
                context.bot,
                "CHANNEL DISCONNECTED",
                f"Channel ID: {channel_id}\nBy: {actor_user_id}",
                level="WARNING",
            )

        except Exception as exc:
            logger.error(
                "\U0000274c Channel disconnect error: %s",
                exc,
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
            "\U00002139\U0000fe0f <b>Connect Channel</b>\n\n"
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
            "\U0000274c Invalid Channel ID.",
            parse_mode="HTML",
        )
        return

    try:
        channel = channels_col.find_one(
            {"_id": str(channel_id)}
        )

        if not channel:
            await update.message.reply_text(
                "\U0000274c <b>Channel not found.</b>\n\n"
                "Pehle bot ko channel mein Administrator banao.",
                parse_mode="HTML",
            )
            return

        if channel.get("owner_user_id") != user_id:
            await update.message.reply_text(
                "\U0000274c <b>Access Denied.</b>\n\n"
                "Ye channel aapke account se linked nahi hai.",
                parse_mode="HTML",
            )
            return

        me = await context.bot.get_me()

        bot_member = await context.bot.get_chat_member(
            chat_id=channel_id,
            user_id=me.id,
        )

        if bot_member.status != ChatMemberStatus.ADMINISTRATOR:
            await update.message.reply_text(
                "\U0000274c Bot ko channel mein Administrator permission chahiye.",
                parse_mode="HTML",
            )
            return

        channels_col.update_one(
            {"_id": str(channel_id)},
            {"$set": {"active": True}},
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
            "\U00002705 <b>CHANNEL CONNECTED</b>\n\n"
            f"\U0001f4e2 <b>{html.escape(title)}</b>\n"
            f"\U0001f194 <code>{channel_id}</code>\n\n"
            "\U0001f680 Ab is channel ke captions automatically process honge.\n\n"
            "Add a rule:\n"
            "<code>/addrule old -> new</code>",
            parse_mode="HTML",
        )

        await send_log_event(
            context.bot,
            "CHANNEL CONNECTED",
            f"Channel: {title}\nID: {channel_id}\nBy: {user_id}",
            level="SUCCESS",
        )

    except Exception as exc:
        logger.exception(
            "\U0000274c Connect command failed: %s",
            exc,
        )

        await update.message.reply_text(
            "\U0000274c <b>Connection failed.</b>\n"
            "Channel ID aur bot permissions check karo.",
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
    user_channels = get_user_channels(user_id)

    if not user_channels:
        await update.message.reply_text(
            "\U0001f4ed <b>No Connected Channels</b>\n\n"
            "Pehle mujhe apne Telegram channel mein "
            "<b>Administrator</b> ke roop mein add karo.",
            parse_mode="HTML",
        )
        return

    selected = get_selected_channel(user_id)

    message = "\U0001f4e2 <b>YOUR CONNECTED CHANNELS</b>\n\n"

    for index, channel in enumerate(
        user_channels,
        start=1,
    ):
        channel_id = channel.get("channel_id")
        title = channel.get(
            "title",
            "Unknown Channel",
        )
        username = channel.get(
            "username",
            "",
        )

        selected_mark = (
            " \U0001f7e2 <b>SELECTED</b>"
            if selected == channel_id
            else ""
        )

        username_text = (
            f"@{username}"
            if username
            else "Private Channel"
        )

        message += (
            f"{index}. \U0001f4e2 <b>{html.escape(title)}</b>"
            f"{selected_mark}\n"
            f"   \U0001f194 <code>{channel_id}</code>\n"
            f"   \U0001f517 {html.escape(username_text)}\n\n"
        )

    message += (
        "\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\n\n"
        "\U0001f3af <b>Select Channel:</b>\n"
        "<code>/usechannel CHANNEL_ID</code>"
    )

    await update.message.reply_text(
        message,
        parse_mode="HTML",
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
            "\U0001f3af <b>Select Your Channel</b>\n\n"
            "Pehle:\n"
            "<code>/channels</code>\n\n"
            "Phir:\n"
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
            "\U0000274c Invalid Channel ID.",
            parse_mode="HTML",
        )
        return

    if not user_owns_channel(
        user_id,
        channel_id,
    ):
        await update.message.reply_text(
            "\U000026d4 <b>Access Denied</b>\n\n"
            "Ye channel aapke account se connected nahi hai.",
            parse_mode="HTML",
        )
        return

    set_selected_channel(
        user_id,
        channel_id,
    )

    channel = get_channel_config(
        channel_id
    )

    title = channel.get(
        "title",
        "Channel",
    )

    await update.message.reply_text(
        "\U00002705 <b>CHANNEL SELECTED</b>\n\n"
        f"\U0001f4e2 <b>{html.escape(title)}</b>\n"
        f"\U0001f194 <code>{channel_id}</code>\n\n"
        "Ab settings sirf isi channel ke liye change hongi.",
        parse_mode="HTML",
    )


# ============================================================
# SELECTED CHANNEL
# ============================================================

async def get_command_channel(update):
    user_id = update.effective_user.id

    channel_id = get_selected_channel(user_id)

    if not channel_id:
        await update.message.reply_text(
            "\U000026a0\U0000fe0f <b>No Channel Selected</b>\n\n"
            "Pehle:\n"
            "<code>/channels</code>\n\n"
            "Phir:\n"
            "<code>/usechannel CHANNEL_ID</code>",
            parse_mode="HTML",
        )
        return None

    if not user_owns_channel(
        user_id,
        channel_id,
    ):
        await update.message.reply_text(
            "\U000026d4 <b>Selected Channel Invalid</b>\n\n"
            "Please <code>/channels</code> se channel dobara select karo.",
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

    channel_id = await get_command_channel(update)

    if channel_id is None:
        return

    raw_args = " ".join(context.args)

    if " -> " not in raw_args:
        await update.message.reply_text(
            "\U00002728 <b>ADD REPLACEMENT RULE</b>\n\n"
            "Format:\n"
            "<code>/addrule old_text -> new_text</code>\n\n"
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
            "\U0000274c Old text empty nahi ho sakta.",
            parse_mode="HTML",
        )
        return

    if not new_part:
        await update.message.reply_text(
            "\U0000274c New text empty nahi ho sakta.",
            parse_mode="HTML",
        )
        return

    config = get_channel_config(channel_id)

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
        "\U00002705 <b>RULE ADDED & LIVE!</b>\n\n"
        f"\U0001f50d <code>{html.escape(old_part)}</code>"
        " \U000027a1\U0000fe0f "
        f"<code>{html.escape(new_part)}</code>\n\n"
        "\U00002705 Rule is active now.",
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

    channel_id = await get_command_channel(update)

    if channel_id is None:
        return

    old_text = " ".join(
        context.args
    ).strip()

    if not old_text:
        await update.message.reply_text(
            "\U00002728 <b>Format:</b>\n"
            "<code>/delrule old_text</code>",
            parse_mode="HTML",
        )
        return

    config = get_channel_config(channel_id)

    rules = config.get(
        "replacement_rules",
        {},
    )

    if old_text not in rules:
        await update.message.reply_text(
            "\U0000274c <b>Rule Not Found</b>\n\n"
            "Exact old text use karo.",
            parse_mode="HTML",
        )
        return

    owner = rule_owner(
        rules[old_text]
    )

    if is_admin(user_id) or owner == user_id:
        del rules[old_text]

        update_channel_config(
            channel_id,
            "replacement_rules",
            rules,
        )

        await update.message.reply_text(
            "\U0001f5d1\U0000fe0f <b>RULE DELETED</b>\n\n"
            f"<code>{html.escape(old_text)}</code>",
            parse_mode="HTML",
        )
    else:
        await update.message.reply_text(
            "\U000026d4 <b>Access Denied</b>\n\n"
            "Aap sirf apna rule delete kar sakte ho.",
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
            "\U000026d4 <b>Access Denied.</b>",
            parse_mode="HTML",
        )
        return

    channel_id = await get_command_channel(update)

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
        "\U0001f4dd <b>HEADER UPDATED</b>\n\n"
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
            "\U000026d4 <b>Access Denied.</b>",
            parse_mode="HTML",
        )
        return

    channel_id = await get_command_channel(update)

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
        "\U0001f4dd <b>FOOTER UPDATED</b>\n\n"
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
            "\U000026d4 <b>Access Denied.</b>",
            parse_mode="HTML",
        )
        return

    channel_id = await get_command_channel(update)

    if channel_id is None:
        return

    update_channel_config(
        channel_id,
        "replacement_rules",
        {},
    )

    await update.message.reply_text(
        "\U0001f9f9 <b>RULES CLEARED</b>\n\n"
        "Selected channel ke saare replacement rules clear ho gaye.",
        parse_mode="HTML",
    )


# ============================================================
# /STATUS
# ============================================================

async def status(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    channel_id = await get_command_channel(update)

    if channel_id is None:
        return

    config = get_channel_config(channel_id)

    replacement_rules = config.get(
        "replacement_rules",
        {},
    )

    custom_header = config.get(
        "custom_header",
        "",
    )

    custom_footer = config.get(
        "custom_footer",
        "",
    )

    channel_title = config.get(
        "title",
        "Unknown",
    )

    channel_username = config.get(
        "username",
        "",
    )

    message = (
        "\U00002699\U0000fe0f <b>AUTO CAPTION ENGINE</b>\n"
        "\U0001f4ca <b>CHANNEL DASHBOARD</b>\n\n"
        "\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\n\n"
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
        f"\U0001f4e6 <b>Batch:</b> "
        f"up to {BATCH_SIZE}\n"
        f"\U0001f4cf <b>Stagger:</b> "
        f"{EDIT_STAGGER_SECONDS:.2f}s\n\n"
        "\U0001f4ca <b>Replacement Rules:</b>\n"
    )

    if not replacement_rules:
        message += "<i>No rules configured.</i>"
    else:
        for old, rule in replacement_rules.items():
            owner = rule_owner(rule)

            owner_tag = (
                f" \U00002014 By <code>{owner}</code>"
                if owner
                else ""
            )

            message += (
                f"\U0001f50d <code>{html.escape(str(old))}</code>"
                " \U000027a1\U0000fe0f "
                f"<code>{html.escape(rule_value(rule))}</code>"
                f"{owner_tag}\n"
            )

    await update.message.reply_text(
        message,
        parse_mode="HTML",
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
            "\U00002728 <b>Format:</b>\n"
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
            "\U0000274c Invalid Channel ID.",
            parse_mode="HTML",
        )
        return

    if not is_admin(user_id):
        if not user_owns_channel(
            user_id,
            channel_id,
        ):
            await update.message.reply_text(
                "\U000026d4 <b>Access Denied.</b>",
                parse_mode="HTML",
            )
            return

    update_channel_config(
        channel_id,
        "active",
        False,
    )

    if get_selected_channel(user_id) == channel_id:
        users_col.update_one(
            {"_id": str(user_id)},
            {"$unset": {"selected_channel": ""}},
        )

    await update.message.reply_text(
        "\U0001f534 <b>CHANNEL DISCONNECTED</b>\n\n"
        f"\U0001f194 <code>{channel_id}</code>\n\n"
        "Ab is channel ke posts process nahi honge.",
        parse_mode="HTML",
    )

    await send_log_event(
        context.bot,
        "CHANNEL DISCONNECTED",
        f"Channel ID: {channel_id}\nBy: {user_id}",
        level="WARNING",
    )


# ============================================================
# HELP
# ============================================================

def get_help_text():
    return (
        "\U00002753 <b>AUTO CAPTION ENGINE \U00002014 HELP</b>\n\n"
        "\U0001f916 Auto Caption Engine aapke Telegram channel "
        "posts/captions ko automatically clean aur edit karta hai.\n\n"
        "\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\n\n"
        "\U00002795 <b>CONNECT</b>\n\n"
        "Bot ko channel mein Administrator banao.\n"
        "<code>/connect CHANNEL_ID</code>\n\n"
        "\U0001f4e2 <b>CHANNELS</b>\n\n"
        "<code>/channels</code>\n"
        "<code>/usechannel CHANNEL_ID</code>\n\n"
        "\U0001f504 <b>REPLACEMENT RULE</b>\n\n"
        "<code>/addrule MovieHub -> DG_Contents</code>\n\n"
        "\U0001f5d1\U0000fe0f <b>DELETE RULE</b>\n\n"
        "<code>/delrule MovieHub</code>\n\n"
        "\U0001f3a8 <b>HEADER / FOOTER</b>\n\n"
        "<code>/setheader Your Header</code>\n"
        "<code>/setfooter Your Footer</code>\n\n"
        "\U0001f4ca <b>STATUS</b>\n\n"
        "<code>/status</code>\n\n"
        "\U0001f9f9 <b>CLEAR RULES</b>\n\n"
        "<code>/clear</code>\n\n"
        "\U0001f534 <b>DISCONNECT</b>\n\n"
        "<code>/disconnect CHANNEL_ID</code>"
    )


async def help_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    help_text = get_help_text()

    keyboard = [
        [
            InlineKeyboardButton(
                "\U00002795 Add Me to Your Channel",
                url=(
                    f"https://t.me/{BOT_USERNAME}"
                    "?startchannel=true"
                ),
            )
        ],
        [
            InlineKeyboardButton(
                "\U0001f465 Support",
                url="https://t.me/dghelps_bot",
            )
        ],
        [
            InlineKeyboardButton(
                "\U0001f519 Back",
                callback_data="back_start",
            )
        ],
    ]

    markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await update.callback_query.edit_message_text(
            text=help_text,
            parse_mode="HTML",
            reply_markup=markup,
        )
    elif update.message:
        await update.message.reply_text(
            text=help_text,
            parse_mode="HTML",
            reply_markup=markup,
        )


# ============================================================
# START KEYBOARD
# ============================================================

def get_start_keyboard():
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "\U00002795 Add Me to Your Channel",
                    url=(
                        f"https://t.me/{BOT_USERNAME}"
                        "?startchannel=true"
                    ),
                )
            ],
            [
                InlineKeyboardButton(
                    "\U0001f4e2 Channel",
                    url="https://t.me/dg_contents",
                ),
                InlineKeyboardButton(
                    "\U0001f465 Support",
                    url="https://t.me/dghelps_bot",
                ),
            ],
            [
                InlineKeyboardButton(
                    "\U00002753 Help",
                    callback_data="help",
                )
            ],
        ]
    )


# ============================================================
# /START
# ============================================================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    user_id = update.effective_user.id
    user_name = (
        update.effective_user.first_name
        or "User"
    )

    if is_admin(user_id):
        welcome_text = (
            f"\U000026a1\U0000fe0f <b>Welcome, "
            f"{html.escape(user_name)}!</b> (Admin)\n\n"
            "\U0001f680 <b>Auto Caption Engine v11.0</b>\n"
            "\U0001f4e1 <b>Controlled Batch + Anti-Loop System</b>\n\n"
            "\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\n\n"
            "\U0001f916 Automatically clean and edit captions "
            "in your connected Telegram channels.\n\n"
            "\U0001f512 Every channel has separate rules/settings.\n\n"
            "\U000026a1\U0000fe0f <b>Quick Start</b>\n\n"
            "1\U0000fe0f\U000020e3 Add me to your channel as Admin\n"
            "2\U0000fe0f\U000020e3 Channel automatically registers\n"
            "3\U0000fe0f\U000020e3 Use <code>/channels</code>\n"
            "4\U0000fe0f\U000020e3 Use <code>/usechannel ID</code>\n"
            "5\U0000fe0f\U000020e3 Add rules\n\n"
            "\U0001f447 Use the buttons below."
        )
    else:
        welcome_text = (
            f"\U0001f44b <b>Hello, "
            f"{html.escape(user_name)}!</b>\n\n"
            "\U0001f680 <b>Auto Caption Engine v11.0</b>\n"
            "\U000026a1 Smart \U00002022 Fast \U00002022 Channel-Isolated\n\n"
            "\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\U00002501\n\n"
            "\U0001f916 Automatically manage your captions.\n\n"
            "\U00002022 \U0001f504 Replace unwanted words\n"
            "\U00002022 \U0001f9f9 Clean unwanted links/mentions\n"
            "\U00002022 \U0001f3a8 Add custom header/footer\n"
            "\U00002022 \U0001f512 Separate settings for every channel\n\n"
            "\U0001f680 <b>Get Started</b>\n\n"
            "1\U0000fe0f\U000020e3 Click <b>Add Me to Your Channel</b>\n"
            "2\U0000fe0f\U000020e3 Select your channel\n"
            "3\U0000fe0f\U000020e3 Give Administrator permission\n"
            "4\U0000fe0f\U000020e3 Send <code>/connect CHANNEL_ID</code>\n\n"
            "Then:\n"
            "<code>/addrule old -> new</code>"
        )

    await update.message.reply_text(
        text=welcome_text,
        parse_mode="HTML",
        reply_markup=get_start_keyboard(),
    )


# ============================================================
# BUTTON HANDLER
# ============================================================

async def button_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    query = update.callback_query

    await query.answer()

    if query.data == "help":
        await help_command(
            update,
            context,
        )
        return

    if query.data == "back_start":
        user_name = (
            query.from_user.first_name
            or "User"
        )

        if is_admin(query.from_user.id):
            welcome_text = (
                f"\U000026a1\U0000fe0f <b>Welcome, "
                f"{html.escape(user_name)}!</b> (Admin)\n\n"
                "\U0001f680 <b>Auto Caption Engine v11.0</b>\n\n"
                "\U0001f916 Controlled caption processing.\n"
                "\U0001f512 Channel-isolated settings.\n\n"
                "\U0001f447 Choose an option below."
            )
        else:
            welcome_text = (
                f"\U0001f44b <b>Hello, "
                f"{html.escape(user_name)}!</b>\n\n"
                "\U0001f680 <b>Auto Caption Engine v11.0</b>\n\n"
                "\U0001f916 Smart automatic caption editor.\n\n"
                "\U0001f447 Choose an option below."
            )

        await query.edit_message_text(
            text=welcome_text,
            parse_mode="HTML",
            reply_markup=get_start_keyboard(),
        )


# ============================================================
# ERROR HANDLER
# ============================================================

async def error_handler(
    update: object,
    context: ContextTypes.DEFAULT_TYPE,
):
    error = context.error

    if isinstance(error, RetryAfter):
        logger.warning(
            "\U000023f3 Global RetryAfter: %.1fs",
            float(error.retry_after),
        )
        return

    logger.exception(
        "Unhandled Telegram error: %s",
        error,
    )


# ============================================================
# MAIN
# ============================================================

def main():
    threading.Thread(
        target=start_dummy_server,
        daemon=True,
    ).start()

    app = (
        ApplicationBuilder()
        .token(TOKEN)
        .concurrent_updates(20)
        .connection_pool_size(30)
        .pool_timeout(30.0)
        .post_init(start_caption_worker)
        .post_shutdown(stop_caption_worker)
        .build()
    )

    # Commands
    app.add_handler(
        CommandHandler("start", start)
    )
    app.add_handler(
        CommandHandler("help", help_command)
    )
    app.add_handler(
        CommandHandler("connect", connect_channel)
    )
    app.add_handler(
        CommandHandler("channels", channels_command)
    )
    app.add_handler(
        CommandHandler("usechannel", use_channel)
    )
    app.add_handler(
        CommandHandler("addrule", add_rule)
    )
    app.add_handler(
        CommandHandler("delrule", del_rule)
    )
    app.add_handler(
        CommandHandler("setfooter", set_footer)
    )
    app.add_handler(
        CommandHandler("setheader", set_header)
    )
    app.add_handler(
        CommandHandler("status", status)
    )
    app.add_handler(
        CommandHandler("clear", clear_rules)
    )
    app.add_handler(
        CommandHandler("disconnect", disconnect_channel)
    )

    # Buttons
    app.add_handler(
        CallbackQueryHandler(button_handler)
    )

    # Channel connection detector
    app.add_handler(
        ChatMemberHandler(
            handle_bot_channel_status,
            ChatMemberHandler.MY_CHAT_MEMBER,
        )
    )

    # Channel posts + edited channel posts
    app.add_handler(
        MessageHandler(
            filters.UpdateType.CHANNEL_POST
            | filters.UpdateType.EDITED_CHANNEL_POST,
            edit_channel_caption,
        )
    )

    app.add_error_handler(error_handler)

    logger.info("\U0001f916 Auto Caption Engine v11.0 starting...")
    logger.info(
        "\U0001f4e6 Edit schedule: continuous batches up to %d",
        BATCH_SIZE,
    )
    logger.info(
        "\U000023f1\U0000fe0f Batch stagger: %.2fs",
        EDIT_STAGGER_SECONDS,
    )
    logger.info(
        "\U0001f451 Admin IDs: %s",
        ADMIN_IDS,
    )
    logger.info(
        "\U0001f916 Bot username: @%s",
        BOT_USERNAME,
    )

    app.run_polling(
        allowed_updates=Update.ALL_TYPES
    )


if __name__ == "__main__":
    main()
