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
    MessageHandler,
    CallbackQueryHandler,
    ChatMemberHandler,
    ContextTypes,
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

logger = logging.getLogger("PremiumCaptionBot")


# ============================================================
# RENDER HEALTH SERVER
# ============================================================

def start_dummy_server():
    port = int(os.environ.get("PORT", "10000"))

    class QuietHandler(http.server.SimpleHTTPRequestHandler):
        def log_message(self, format, *args):
            pass

    try:
        with socketserver.TCPServer(("", port), QuietHandler) as server:
            logger.info("Health server started on port %s", port)
            server.serve_forever()
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
).strip().lstrip("@")

if not TOKEN:
    logger.error("BOT_TOKEN is missing.")
    sys.exit(1)

if not MONGO_URI:
    logger.error("MONGO_URI is missing.")
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
    logger.error("MongoDB connection failed: %s", exc)
    sys.exit(1)


db = db_client["AutoCaptionBotDB"]

channels_col = db["connected_channels"]
users_col = db["user_settings"]
stats_col = db["statistics"]


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

    if legacy_owner.isdigit():
        if int(legacy_owner) != 0:
            ids.add(int(legacy_owner))

    return ids


ADMIN_IDS = parse_admin_ids()


def is_admin(user_id: int) -> bool:
    """
    Secure behavior:
    If ADMIN_IDS is empty, nobody is treated as admin.
    """

    if not ADMIN_IDS:
        return False

    return user_id in ADMIN_IDS


# ============================================================
# OPTIONAL LOG CHANNEL
# ============================================================

raw_log_id = os.environ.get(
    "LOG_CHANNEL_ID",
    "",
).strip()

try:
    LOG_CHANNEL_ID = (
        int(raw_log_id)
        if raw_log_id
        else None
    )
except ValueError:
    LOG_CHANNEL_ID = None


# ============================================================
# CAPTION ENGINE SETTINGS
# ============================================================

BATCH_SIZE = 10
BATCHES_BEFORE_COOLDOWN = 2
COOLDOWN_SECONDS = 40.0
EDIT_STAGGER_SECONDS = 0.12
MAX_EDIT_RETRIES = 5

QUEUE_MAX_SIZE = 5000
CONFIG_CACHE_TTL = 30.0

_caption_queue = asyncio.Queue(
    maxsize=QUEUE_MAX_SIZE
)

_caption_worker_task = None

_pending_jobs = {}

_config_cache = {}
_config_locks = {}

_recent_applied = OrderedDict()
RECENT_APPLIED_LIMIT = 3000


# ============================================================
# UI STATE
# ============================================================

# Temporary GUI wizard state.
# Example:
# {
#   user_id: {
#       "action": "add_rule_old",
#       "channel_id": -100123
#   }
# }
#
# This is intentionally temporary.
ui_state = {}


def set_ui_state(user_id, state):
    ui_state[user_id] = state


def get_ui_state(user_id):
    return ui_state.get(user_id)


def clear_ui_state(user_id):
    ui_state.pop(user_id, None)


# ============================================================
# GENERAL HELPERS
# ============================================================

def esc(value):
    return html.escape(str(value or ""))


def short_text(value, length=35):
    value = str(value or "")

    if len(value) <= length:
        return value

    return value[:length - 3] + "..."


def channel_display(channel):
    title = channel.get("title") or "Unnamed Channel"

    username = channel.get("username") or ""

    if username:
        return f"{title} (@{username})"

    return title


def mark_recent_applied(
    channel_id,
    message_id,
    final_plain,
):
    key = (
        channel_id,
        message_id,
    )

    _recent_applied[key] = final_plain
    _recent_applied.move_to_end(key)

    while len(_recent_applied) > RECENT_APPLIED_LIMIT:
        _recent_applied.popitem(last=False)


def get_recent_applied(
    channel_id,
    message_id,
):
    return _recent_applied.get(
        (
            channel_id,
            message_id,
        )
    )


def invalidate_channel_config_cache(
    channel_id,
):
    _config_cache.pop(
        channel_id,
        None,
    )


# ============================================================
# DEFAULT CONFIG
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
        "custom_footer": "⚡ Fast Download Links @DG_Contents",
        "clean_links": True,
        "clean_mentions": True,
        "bold_output": True,
        "created_at": time.time(),
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
            config = default_channel_config(
                channel_id
            )

            channels_col.update_one(
                {"_id": str(channel_id)},
                {
                    "$setOnInsert": config
                },
                upsert=True,
            )

            return config

        return config

    except Exception as exc:
        logger.error(
            "Mongo get config error: %s",
            exc,
        )

        return default_channel_config(
            channel_id
        )


async def get_cached_channel_config_async(
    channel_id,
):
    now = time.monotonic()

    cached = _config_cache.get(
        channel_id
    )

    if cached:
        if now - cached["time"] < CONFIG_CACHE_TTL:
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

        if cached:
            if now - cached["time"] < CONFIG_CACHE_TTL:
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


def update_channel_config(
    channel_id,
    field_name,
    field_value,
):
    try:
        channels_col.update_one(
            {"_id": str(channel_id)},
            {
                "$set": {
                    field_name: field_value
                }
            },
            upsert=True,
        )

    except Exception as exc:
        logger.error(
            "Mongo update error: %s",
            exc,
        )

    finally:
        invalidate_channel_config_cache(
            channel_id
        )


def get_selected_channel(user_id):
    try:
        user = users_col.find_one(
            {"_id": str(user_id)}
        )

        if not user:
            return None

        return user.get(
            "selected_channel"
        )

    except Exception:
        return None


def set_selected_channel(
    user_id,
    channel_id,
):
    try:
        users_col.update_one(
            {"_id": str(user_id)},
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
            "Selected channel update failed: %s",
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

    except Exception:
        return False


def get_user_channels(user_id):
    try:
        return list(
            channels_col.find(
                {
                    "owner_user_id": user_id,
                    "active": True,
                }
            ).sort(
                "title",
                1,
            )
        )

    except Exception:
        return []


# ============================================================
# STATISTICS
# ============================================================

def increment_stats(
    channel_id,
    *,
    processed=0,
    changed=0,
    replacements=0,
    links_removed=0,
    mentions_removed=0,
    errors=0,
):
    try:
        stats_col.update_one(
            {"_id": str(channel_id)},
            {
                "$inc": {
                    "processed": processed,
                    "changed": changed,
                    "replacements": replacements,
                    "links_removed": links_removed,
                    "mentions_removed": mentions_removed,
                    "errors": errors,
                },
                "$set": {
                    "last_activity": time.time()
                },
            },
            upsert=True,
        )

    except Exception as exc:
        logger.warning(
            "Stats update failed: %s",
            exc,
        )


def get_channel_stats(channel_id):
    try:
        return stats_col.find_one(
            {"_id": str(channel_id)}
        ) or {}

    except Exception:
        return {}


def get_global_stats():
    try:
        pipeline = [
            {
                "$group": {
                    "_id": None,
                    "processed": {
                        "$sum": "$processed"
                    },
                    "changed": {
                        "$sum": "$changed"
                    },
                    "replacements": {
                        "$sum": "$replacements"
                    },
                    "links_removed": {
                        "$sum": "$links_removed"
                    },
                    "mentions_removed": {
                        "$sum": "$mentions_removed"
                    },
                    "errors": {
                        "$sum": "$errors"
                    },
                }
            }
        ]

        result = list(
            stats_col.aggregate(
                pipeline
            )
        )

        return result[0] if result else {}

    except Exception:
        return {}


# ============================================================
# IMPORTANT LOGGING
# ============================================================

async def send_important_log(
    bot,
    title,
    body="",
    level="INFO",
):
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

    text = (
        f"{icon} <b>{esc(title)}</b>"
    )

    if body:
        text += (
            f"\n\n{esc(body)}"
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
            "Log send failed: %s",
            exc,
        )


async def log_new_user(
    bot,
    user,
):
    if not user:
        return

    try:
        result = await asyncio.to_thread(
            users_col.update_one,
            {"_id": str(user.id)},
            {
                "$setOnInsert": {
                    "_id": str(user.id),
                    "created_at": time.time(),
                }
            },
            upsert=True,
        )

        if result.upserted_id is not None:
            name = user.first_name or "User"

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
            "User tracking failed: %s",
            exc,
        )


# ============================================================
# CAPTION TRANSFORM
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


def transform_caption(
    text,
    config,
):
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

    clean_links = config.get(
        "clean_links",
        True,
    )

    clean_mentions = config.get(
        "clean_mentions",
        True,
    )

    final_text = text

    links_removed = 0
    mentions_removed = 0
    replacements = 0

    # --------------------------------------------------------
    # Remove unwanted Telegram links
    # --------------------------------------------------------

    if clean_links:

        link_pattern = (
            r"(https?://)?t\.me/"
            r"(?!DG_Contents|dghelps_bot)"
            r"[a-zA-Z0-9_]+"
        )

        final_text, links_removed = re.subn(
            link_pattern,
            "",
            final_text,
            flags=re.IGNORECASE,
        )

    # --------------------------------------------------------
    # Remove unwanted mentions
    # --------------------------------------------------------

    if clean_mentions:

        mention_pattern = (
            r"@(?!DG_Contents|dghelps_bot)"
            r"[a-zA-Z0-9_]+"
        )

        final_text, mentions_removed = re.subn(
            mention_pattern,
            "",
            final_text,
            flags=re.IGNORECASE,
        )

    # --------------------------------------------------------
    # Replacement rules
    # --------------------------------------------------------

    for old_text, rule in replacement_rules.items():

        new_text = rule_value(rule)

        if not old_text:
            continue

        final_text, count = re.subn(
            re.escape(old_text),
            lambda _m, replacement=new_text: replacement,
            final_text,
            flags=re.IGNORECASE,
        )

        replacements += count

    # --------------------------------------------------------
    # Cleanup spaces
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
    # Header / body / footer
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

    final_html = html.escape(
        final_plain
    )

    if config.get(
        "bold_output",
        True,
    ):
        final_html = (
            f"<b>{final_html}</b>"
        )

    return {
        "plain": final_plain,
        "html": final_html,
        "replacements": replacements,
        "links_removed": links_removed,
        "mentions_removed": mentions_removed,
    }


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
    last_error = None

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

        except (
            TimedOut,
            NetworkError,
        ) as exc:

            last_error = exc

            wait_time = min(
                0.5 * (
                    2 ** (
                        attempt - 1
                    )
                ),
                5.0,
            )

            await asyncio.sleep(
                wait_time
            )

        except BadRequest as exc:

            if (
                "message is not modified"
                in str(exc).lower()
            ):
                return None

            raise

    if last_error:
        raise last_error

    return None


# ============================================================
# EDIT JOB
# ============================================================

async def run_edit_job(job):
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

    final_plain = job[
        "final_plain"
    ]

    is_caption = job[
        "is_caption"
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

        await asyncio.to_thread(
            increment_stats,
            channel_id,
            processed=1,
            changed=1,
            replacements=job.get(
                "replacements",
                0,
            ),
            links_removed=job.get(
                "links_removed",
                0,
            ),
            mentions_removed=job.get(
                "mentions_removed",
                0,
            ),
        )

        logger.info(
            "EDITED channel=%s message=%s time=%.2fs",
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

        return (
            "rate_limit",
            job,
            retry_after,
        )

    except BadRequest as exc:

        if (
            "message is not modified"
            in str(exc).lower()
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

        await asyncio.to_thread(
            increment_stats,
            channel_id,
            errors=1,
        )

        return (
            "failed",
            job,
            None,
        )

    except Exception as exc:

        logger.exception(
            "Edit failed: %s",
            exc,
        )

        await asyncio.to_thread(
            increment_stats,
            channel_id,
            errors=1,
        )

        return (
            "failed",
            job,
            None,
        )


# ============================================================
# QUEUE
# ============================================================

async def enqueue_caption_job(job):
    key = (
        job["channel_id"],
        job["message_id"],
    )

    if key in _pending_jobs:

        _pending_jobs[key] = job

        return

    _pending_jobs[key] = job

    await _caption_queue.put(
        key
    )


async def caption_batch_worker():

    batches_done = 0

    while True:

        try:

            first_key = (
                await _caption_queue.get()
            )

            keys = [
                first_key
            ]

            for _ in range(
                BATCH_SIZE - 1
            ):

                try:
                    keys.append(
                        _caption_queue.get_nowait()
                    )

                except asyncio.QueueEmpty:
                    break

            jobs = []

            for key in keys:

                job = _pending_jobs.pop(
                    key,
                    None,
                )

                if job:
                    jobs.append(
                        job
                    )

            if not jobs:

                for _ in keys:
                    _caption_queue.task_done()

                continue

            async def delayed_job(
                index,
                job,
            ):

                if index:
                    await asyncio.sleep(
                        EDIT_STAGGER_SECONDS
                        * index
                    )

                return await run_edit_job(
                    job
                )

            results = await asyncio.gather(
                *[
                    delayed_job(
                        index,
                        job,
                    )
                    for index, job
                    in enumerate(jobs)
                ],
                return_exceptions=False,
            )

            rate_limited = [
                (
                    result[1],
                    result[2],
                )
                for result in results
                if result[0]
                == "rate_limit"
            ]

            for _ in keys:
                _caption_queue.task_done()

            if rate_limited:

                server_wait = max(
                    wait
                    for _, wait
                    in rate_limited
                )

                wait_time = (
                    server_wait + 1.0
                )

                logger.warning(
                    "Telegram rate limit. Waiting %.1fs",
                    wait_time,
                )

                if rate_limited:

                    await send_important_log(
                        rate_limited[0][0]["bot"],
                        "TELEGRAM RATE LIMIT",
                        (
                            f"{len(rate_limited)} "
                            "edit job(s) delayed. "
                            f"Retrying in {wait_time:.1f}s."
                        ),
                        "WARNING",
                    )

                await asyncio.sleep(
                    wait_time
                )

                for job, _ in rate_limited:

                    await enqueue_caption_job(
                        job
                    )

                batches_done = 0

            else:

                batches_done += 1

                if (
                    batches_done
                    >= BATCHES_BEFORE_COOLDOWN
                ):

                    await asyncio.sleep(
                        COOLDOWN_SECONDS
                    )

                    batches_done = 0

        except asyncio.CancelledError:
            raise

        except Exception as exc:

            logger.exception(
                "Caption worker error: %s",
                exc,
            )

            await asyncio.sleep(
                2
            )


async def start_caption_worker(
    application,
):

    global _caption_worker_task

    if (
        _caption_worker_task
        is None
    ):

        _caption_worker_task = (
            asyncio.create_task(
                caption_batch_worker()
            )
        )

    await send_important_log(
        application.bot,
        "BOT ONLINE",
        (
            "Premium Caption Engine started.\n"
            f"Batch: {BATCH_SIZE} + {BATCH_SIZE}\n"
            f"Cooldown: {COOLDOWN_SECONDS:.0f}s"
        ),
        "START",
    )


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

    if _caption_worker_task:

        _caption_worker_task.cancel()

        try:
            await _caption_worker_task

        except asyncio.CancelledError:
            pass

        _caption_worker_task = None


# ============================================================
# PREMIUM KEYBOARDS
# ============================================================

def main_keyboard():
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "📢 My Channels",
                    callback_data="channels",
                ),
            ],
            [
                InlineKeyboardButton(
                    "⚙️ Channel Settings",
                    callback_data="settings",
                ),
                InlineKeyboardButton(
                    "🔄 Rules",
                    callback_data="rules",
                ),
            ],
            [
                InlineKeyboardButton(
                    "📊 Statistics",
                    callback_data="stats",
                ),
                InlineKeyboardButton(
                    "🎨 Design",
                    callback_data="design",
                ),
            ],
            [
                InlineKeyboardButton(
                    "❓ Help",
                    callback_data="help",
                ),
            ],
        ]
    )


def back_keyboard(
    callback="home",
):
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "⬅️ Back",
                    callback_data=callback,
                ),
                InlineKeyboardButton(
                    "🏠 Home",
                    callback_data="home",
                ),
            ]
        ]
    )


def channel_list_keyboard(
    channels,
    prefix="select",
):
    rows = []

    for channel in channels:

        channel_id = channel.get(
            "channel_id"
        )

        title = short_text(
            channel.get(
                "title",
                "Channel",
            ),
            28,
        )

        status = (
            "🟢"
            if channel.get(
                "active",
                True,
            )
            else "🔴"
        )

        rows.append(
            [
                InlineKeyboardButton(
                    f"{status} {title}",
                    callback_data=(
                        f"{prefix}:{channel_id}"
                    ),
                )
            ]
        )

    rows.append(
        [
            InlineKeyboardButton(
                "➕ Add Channel",
                url=(
                    f"https://t.me/"
                    f"{BOT_USERNAME}"
                    "?startchannel=true"
                ),
            )
        ]
    )

    rows.append(
        [
            InlineKeyboardButton(
                "⬅️ Back",
                callback_data="home",
            )
        ]
    )

    return InlineKeyboardMarkup(
        rows
    )


# ============================================================
# DASHBOARD TEXT
# ============================================================

def dashboard_text(
    user,
):
    name = esc(
        user.first_name
        or "User"
    )

    selected = get_selected_channel(
        user.id
    )

    channels = get_user_channels(
        user.id
    )

    if selected:
        selected_config = get_channel_config(
            selected
        )

        selected_name = selected_config.get(
            "title",
            "Channel",
        )

        selected_line = (
            f"📢 <b>Selected:</b> "
            f"{esc(selected_name)}"
        )

    else:
        selected_line = (
            "📢 <b>Selected:</b> None"
        )

    admin_line = ""

    if is_admin(user.id):
        admin_line = (
            "\n👑 <b>Admin:</b> Enabled\n"
        )

    return (
        "╭──────────────────────────────╮\n"
        "│  ⚡ <b>AUTO CAPTION ENGINE</b>  │\n"
        "│  <i>Premium Control Panel</i> │\n"
        "╰──────────────────────────────╯\n\n"
        f"👋 Welcome, <b>{name}</b>\n\n"
        "📊 <b>SYSTEM OVERVIEW</b>\n"
        f"├─ 📢 Channels: <b>{len(channels)}</b>\n"
        f"{selected_line}\n"
        "├─ 🟢 Engine: <b>ONLINE</b>\n"
        "└─ ⚡ Mode: <b>AUTO</b>\n"
        f"{admin_line}\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "🎯 <b>Choose an option below</b>"
    )


# ============================================================
# START
# ============================================================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not update.effective_user:
        return

    await log_new_user(
        context.bot,
        update.effective_user,
    )

    clear_ui_state(
        update.effective_user.id
    )

    await update.message.reply_text(
        dashboard_text(
            update.effective_user
        ),
        parse_mode="HTML",
        reply_markup=main_keyboard(),
    )


# ============================================================
# HOME CALLBACK
# ============================================================

async def show_home(
    query,
):
    clear_ui_state(
        query.from_user.id
    )

    await query.edit_message_text(
        dashboard_text(
            query.from_user
        ),
        parse_mode="HTML",
        reply_markup=main_keyboard(),
    )


# ============================================================
# CHANNEL MANAGER
# ============================================================

async def show_channels(
    query,
):

    channels = get_user_channels(
        query.from_user.id
    )

    if not channels:

        text = (
            "📢 <b>MY CHANNELS</b>\n\n"
            "You don't have any connected "
            "channels yet.\n\n"
            "Add me as Administrator to your "
            "Telegram channel to get started."
        )

        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "➕ Add Me to Channel",
                        url=(
                            f"https://t.me/"
                            f"{BOT_USERNAME}"
                            "?startchannel=true"
                        ),
                    )
                ],
                [
                    InlineKeyboardButton(
                        "⬅️ Back",
                        callback_data="home",
                    )
                ],
            ]
        )

        await query.edit_message_text(
            text,
            parse_mode="HTML",
            reply_markup=keyboard,
        )

        return

    selected = get_selected_channel(
        query.from_user.id
    )

    text = (
        "📢 <b>MY CHANNELS</b>\n\n"
        "Select a channel to manage it.\n\n"
    )

    for channel in channels:

        channel_id = channel.get(
            "channel_id"
        )

        title = channel.get(
            "title",
            "Channel",
        )

        status = (
            "🟢 Active"
            if channel.get(
                "active",
                True,
            )
            else "🔴 Inactive"
        )

        marker = (
            " ⭐"
            if selected == channel_id
            else ""
        )

        text += (
            f"📢 <b>{esc(title)}</b>{marker}\n"
            f"   {status}\n\n"
        )

    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=channel_list_keyboard(
            channels,
            "select",
        ),
    )


# ============================================================
# CHANNEL DASHBOARD
# ============================================================

async def show_channel_dashboard(
    query,
    channel_id,
):

    if not user_owns_channel(
        query.from_user.id,
        channel_id,
    ):
        await query.answer(
            "Access denied.",
            show_alert=True,
        )
        return

    set_selected_channel(
        query.from_user.id,
        channel_id,
    )

    channel = get_channel_config(
        channel_id
    )

    title = channel.get(
        "title",
        "Channel",
    )

    username = channel.get(
        "username",
        "",
    )

    stats = get_channel_stats(
        channel_id
    )

    rules = channel.get(
        "replacement_rules",
        {},
    )

    active = channel.get(
        "active",
        True,
    )

    status = (
        "🟢 ACTIVE"
        if active
        else "🔴 INACTIVE"
    )

    username_text = (
        f"@{esc(username)}"
        if username
        else "Private Channel"
    )

    text = (
        "📢 <b>CHANNEL DASHBOARD</b>\n\n"
        f"📢 <b>{esc(title)}</b>\n"
        f"🔗 {username_text}\n"
        f"🟢 Status: <b>{status}</b>\n\n"
        "📊 <b>QUICK STATS</b>\n"
        f"├─ 📝 Processed: <b>{stats.get('processed', 0)}</b>\n"
        f"├─ 🔄 Replacements: <b>{stats.get('replacements', 0)}</b>\n"
        f"├─ 🧹 Links: <b>{stats.get('links_removed', 0)}</b>\n"
        f"└─ 👤 Mentions: <b>{stats.get('mentions_removed', 0)}</b>\n\n"
        f"🔄 Rules: <b>{len(rules)}</b>"
    )

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "⚙️ Settings",
                    callback_data=(
                        f"chsettings:{channel_id}"
                    ),
                ),
                InlineKeyboardButton(
                    "🔄 Rules",
                    callback_data=(
                        f"chrules:{channel_id}"
                    ),
                ),
            ],
            [
                InlineKeyboardButton(
                    "🎨 Design",
                    callback_data=(
                        f"chdesign:{channel_id}"
                    ),
                ),
                InlineKeyboardButton(
                    "📊 Stats",
                    callback_data=(
                        f"chstats:{channel_id}"
                    ),
                ),
            ],
            [
                InlineKeyboardButton(
                    "🔴 Disconnect"
                    if active
                    else "🟢 Activate",
                    callback_data=(
                        f"togglechannel:{channel_id}"
                    ),
                )
            ],
            [
                InlineKeyboardButton(
                    "⬅️ Channels",
                    callback_data="channels",
                )
            ],
        ]
    )

    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=keyboard,
    )


# ============================================================
# SETTINGS
# ============================================================

async def show_settings(
    query,
    channel_id=None,
):

    if channel_id is None:
        channel_id = get_selected_channel(
            query.from_user.id
        )

    if not channel_id:
        await query.edit_message_text(
            "⚙️ <b>CHANNEL SETTINGS</b>\n\n"
            "Please select a channel first.",
            parse_mode="HTML",
            reply_markup=back_keyboard(
                "channels"
            ),
        )
        return

    if not user_owns_channel(
        query.from_user.id,
        channel_id,
    ):
        await query.answer(
            "Access denied.",
            show_alert=True,
        )
        return

    set_selected_channel(
        query.from_user.id,
        channel_id,
    )

    channel = get_channel_config(
        channel_id
    )

    title = channel.get(
        "title",
        "Channel",
    )

    clean_links = channel.get(
        "clean_links",
        True,
    )

    clean_mentions = channel.get(
        "clean_mentions",
        True,
    )

    bold = channel.get(
        "bold_output",
        True,
    )

    text = (
        "⚙️ <b>CHANNEL SETTINGS</b>\n\n"
        f"📢 <b>{esc(title)}</b>\n\n"
        "Caption Processing\n"
        f"├─ 🧹 Link Cleaner: "
        f"<b>{'ON' if clean_links else 'OFF'}</b>\n"
        f"├─ 👤 Mention Cleaner: "
        f"<b>{'ON' if clean_mentions else 'OFF'}</b>\n"
        f"└─ 🅱️ Bold Output: "
        f"<b>{'ON' if bold else 'OFF'}</b>\n\n"
        "Tap a setting to toggle it."
    )

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    f"🧹 Links: {'ON' if clean_links else 'OFF'}",
                    callback_data=(
                        f"toggle:links:{channel_id}"
                    ),
                )
            ],
            [
                InlineKeyboardButton(
                    f"👤 Mentions: {'ON' if clean_mentions else 'OFF'}",
                    callback_data=(
                        f"toggle:mentions:{channel_id}"
                    ),
                )
            ],
            [
                InlineKeyboardButton(
                    f"🅱️ Bold: {'ON' if bold else 'OFF'}",
                    callback_data=(
                        f"toggle:bold:{channel_id}"
                    ),
                )
            ],
            [
                InlineKeyboardButton(
                    "🎨 Header / Footer",
                    callback_data=(
                        f"chdesign:{channel_id}"
                    ),
                )
            ],
            [
                InlineKeyboardButton(
                    "⬅️ Back",
                    callback_data=(
                        f"select:{channel_id}"
                    ),
                )
            ],
        ]
    )

    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=keyboard,
    )


# ============================================================
# DESIGN
# ============================================================

async def show_design(
    query,
    channel_id=None,
):

    if channel_id is None:
        channel_id = get_selected_channel(
            query.from_user.id
        )

    if not channel_id:
        await query.answer(
            "Select a channel first.",
            show_alert=True,
        )
        return

    if not user_owns_channel(
        query.from_user.id,
        channel_id,
    ):
        await query.answer(
            "Access denied.",
            show_alert=True,
        )
        return

    channel = get_channel_config(
        channel_id
    )

    title = channel.get(
        "title",
        "Channel",
    )

    header = channel.get(
        "custom_header",
        "",
    )

    footer = channel.get(
        "custom_footer",
        "",
    )

    text = (
        "🎨 <b>CAPTION DESIGN</b>\n\n"
        f"📢 <b>{esc(title)}</b>\n\n"
        "📌 <b>Header</b>\n"
        f"{esc(header) if header else '<i>Disabled</i>'}\n\n"
        "📌 <b>Footer</b>\n"
        f"{esc(footer) if footer else '<i>Disabled</i>'}\n\n"
        "Use the buttons below to edit."
    )

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "📌 Edit Header",
                    callback_data=(
                        f"editheader:{channel_id}"
                    ),
                )
            ],
            [
                InlineKeyboardButton(
                    "📌 Edit Footer",
                    callback_data=(
                        f"editfooter:{channel_id}"
                    ),
                )
            ],
            [
                InlineKeyboardButton(
                    "👁 Preview",
                    callback_data=(
                        f"preview:{channel_id}"
                    ),
                )
            ],
            [
                InlineKeyboardButton(
                    "🗑 Clear Header",
                    callback_data=(
                        f"clearheader:{channel_id}"
                    ),
                ),
                InlineKeyboardButton(
                    "🗑 Clear Footer",
                    callback_data=(
                        f"clearfooter:{channel_id}"
                    ),
                ),
            ],
            [
                InlineKeyboardButton(
                    "⬅️ Back",
                    callback_data=(
                        f"select:{channel_id}"
                    ),
                )
            ],
        ]
    )

    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=keyboard,
    )


# ============================================================
# RULES
# ============================================================

async def show_rules(
    query,
    channel_id=None,
):

    if channel_id is None:
        channel_id = get_selected_channel(
            query.from_user.id
        )

    if not channel_id:
        await query.answer(
            "Select a channel first.",
            show_alert=True,
        )
        return

    if not user_owns_channel(
        query.from_user.id,
        channel_id,
    ):
        await query.answer(
            "Access denied.",
            show_alert=True,
        )
        return

    channel = get_channel_config(
        channel_id
    )

    rules = channel.get(
        "replacement_rules",
        {},
    )

    text = (
        "🔄 <b>REPLACEMENT RULES</b>\n\n"
    )

    if not rules:

        text += (
            "No replacement rules configured.\n\n"
            "Create your first rule below."
        )

    else:

        for index, (
            old,
            rule,
        ) in enumerate(
            rules.items(),
            1,
        ):

            new = rule_value(
                rule
            )

            text += (
                f"<b>{index}.</b> "
                f"<code>{esc(old)}</code>\n"
                f"   ➜ <code>{esc(new)}</code>\n\n"
            )

    keyboard = [
        [
            InlineKeyboardButton(
                "➕ Add Rule",
                callback_data=(
                    f"addrulegui:{channel_id}"
                ),
            )
        ]
    ]

    if rules:
        keyboard.append(
            [
                InlineKeyboardButton(
                    "🗑 Delete Rule",
                    callback_data=(
                        f"delrulegui:{channel_id}"
                    ),
                )
            ]
        )

    keyboard.append(
        [
            InlineKeyboardButton(
                "⬅️ Back",
                callback_data=(
                    f"select:{channel_id}"
                ),
            )
        ]
    )

    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(
            keyboard
        ),
    )


# ============================================================
# STATS
# ============================================================

async def show_stats(
    query,
    channel_id=None,
):

    if channel_id is None:
        channel_id = get_selected_channel(
            query.from_user.id
        )

    if not channel_id:
        await query.edit_message_text(
            "📊 <b>STATISTICS</b>\n\n"
            "Select a channel first.",
            parse_mode="HTML",
            reply_markup=back_keyboard(
                "channels"
            ),
        )
        return

    if not user_owns_channel(
        query.from_user.id,
        channel_id,
    ):
        await query.answer(
            "Access denied.",
            show_alert=True,
        )
        return

    channel = get_channel_config(
        channel_id
    )

    stats = get_channel_stats(
        channel_id
    )

    text = (
        "📊 <b>CHANNEL STATISTICS</b>\n\n"
        f"📢 <b>{esc(channel.get('title', 'Channel'))}</b>\n\n"
        "📝 <b>Caption Processing</b>\n"
        f"├─ Processed: <b>{stats.get('processed', 0)}</b>\n"
        f"├─ Changed: <b>{stats.get('changed', 0)}</b>\n"
        f"├─ Replacements: <b>{stats.get('replacements', 0)}</b>\n"
        f"├─ Links Removed: <b>{stats.get('links_removed', 0)}</b>\n"
        f"├─ Mentions Removed: <b>{stats.get('mentions_removed', 0)}</b>\n"
        f"└─ Errors: <b>{stats.get('errors', 0)}</b>\n"
    )

    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=back_keyboard(
            f"select:{channel_id}"
        ),
    )


# ============================================================
# ADMIN DASHBOARD
# ============================================================

async def show_admin(
    query,
):

    if not is_admin(
        query.from_user.id
    ):
        await query.answer(
            "Admin access required.",
            show_alert=True,
        )
        return

    users_count = users_col.count_documents({})
    channels_count = channels_col.count_documents({})
    active_channels = channels_col.count_documents(
        {
            "active": True
        }
    )

    stats = get_global_stats()

    text = (
        "👑 <b>ADMIN CONTROL CENTER</b>\n\n"
        "📊 <b>OVERVIEW</b>\n"
        f"├─ 👥 Users: <b>{users_count}</b>\n"
        f"├─ 📢 Channels: <b>{channels_count}</b>\n"
        f"├─ 🟢 Active: <b>{active_channels}</b>\n"
        f"├─ 📝 Processed: <b>{stats.get('processed', 0)}</b>\n"
        f"├─ 🔄 Replacements: <b>{stats.get('replacements', 0)}</b>\n"
        f"└─ ⚠️ Errors: <b>{stats.get('errors', 0)}</b>\n\n"
        "⚡ <b>ENGINE</b>\n"
        f"├─ Queue: <b>{_caption_queue.qsize()}</b>\n"
        f"├─ Batch: <b>{BATCH_SIZE} + {BATCH_SIZE}</b>\n"
        f"└─ Cooldown: <b>{COOLDOWN_SECONDS:.0f}s</b>"
    )

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "📢 All Channels",
                    callback_data="adminchannels",
                )
            ],
            [
                InlineKeyboardButton(
                    "📊 Refresh",
                    callback_data="admin",
                )
            ],
            [
                InlineKeyboardButton(
                    "⬅️ Back",
                    callback_data="home",
                )
            ],
        ]
    )

    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=keyboard,
    )


async def show_admin_channels(
    query,
):

    if not is_admin(
        query.from_user.id
    ):
        await query.answer(
            "Admin access required.",
            show_alert=True,
        )
        return

    channels = list(
        channels_col.find(
            {}
        ).sort(
            "title",
            1,
        )
    )

    text = (
        "📢 <b>ALL CONNECTED CHANNELS</b>\n\n"
    )

    if not channels:

        text += (
            "No channels registered."
        )

    else:

        for index, channel in enumerate(
            channels,
            1,
        ):

            status = (
                "🟢"
                if channel.get(
                    "active",
                    True,
                )
                else "🔴"
            )

            text += (
                f"{index}. {status} "
                f"<b>{esc(channel.get('title', 'Unknown'))}</b>\n"
                f"   ID: <code>{channel.get('channel_id')}</code>\n\n"
            )

    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=back_keyboard(
            "admin"
        ),
    )


# ============================================================
# HELP
# ============================================================

async def show_help(
    query,
):

    text = (
        "❓ <b>AUTO CAPTION ENGINE</b>\n"
        "<i>Premium Help Center</i>\n\n"
        "🚀 <b>What does this bot do?</b>\n\n"
        "Automatically cleans and edits captions "
        "inside your connected Telegram channels.\n\n"
        "✨ <b>Features</b>\n"
        "• 🔄 Word replacement\n"
        "• 🧹 Telegram link cleaner\n"
        "• 👤 Mention cleaner\n"
        "• 🎨 Custom header/footer\n"
        "• 📊 Channel statistics\n"
        "• 🔐 Channel-isolated settings\n"
        "• ⚡ Controlled processing queue\n\n"
        "📌 <b>Quick Start</b>\n"
        "1. Add the bot as channel Administrator\n"
        "2. Open My Channels\n"
        "3. Select your channel\n"
        "4. Configure Rules / Design\n"
        "5. Bot will process new captions automatically."
    )

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "➕ Add to Channel",
                    url=(
                        f"https://t.me/"
                        f"{BOT_USERNAME}"
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
                    "⬅️ Back",
                    callback_data="home",
                )
            ],
        ]
    )

    await query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=keyboard,
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

    data = query.data or ""

    try:

        # ----------------------------------------------------
        # HOME
        # ----------------------------------------------------

        if data == "home":
            await show_home(
                query
            )
            return

        # ----------------------------------------------------
        # CHANNELS
        # ----------------------------------------------------

        if data == "channels":

            await show_channels(
                query
            )

            return

        # ----------------------------------------------------
        # SELECT CHANNEL
        # ----------------------------------------------------

        if data.startswith(
            "select:"
        ):

            channel_id = int(
                data.split(
                    ":",
                    1,
                )[1]
            )

            await show_channel_dashboard(
                query,
                channel_id,
            )

            return

        # ----------------------------------------------------
        # SETTINGS
        # ----------------------------------------------------

        if data == "settings":

            await show_settings(
                query
            )

            return

        if data.startswith(
            "chsettings:"
        ):

            channel_id = int(
                data.split(
                    ":",
                    1,
                )[1]
            )

            await show_settings(
                query,
                channel_id,
            )

            return

        # ----------------------------------------------------
        # TOGGLES
        # ----------------------------------------------------

        if data.startswith(
            "toggle:"
        ):

            parts = data.split(
                ":"
            )

            setting = parts[1]
            channel_id = int(
                parts[2]
            )

            if not user_owns_channel(
                query.from_user.id,
                channel_id,
            ):
                await query.answer(
                    "Access denied.",
                    show_alert=True,
                )
                return

            channel = get_channel_config(
                channel_id
            )

            field_map = {
                "links": "clean_links",
                "mentions": "clean_mentions",
                "bold": "bold_output",
            }

            field = field_map.get(
                setting
            )

            if not field:
                return

            current = channel.get(
                field,
                True,
            )

            update_channel_config(
                channel_id,
                field,
                not current,
            )

            await show_settings(
                query,
                channel_id,
            )

            return

        # ----------------------------------------------------
        # RULES
        # ----------------------------------------------------

        if data == "rules":

            await show_rules(
                query
            )

            return

        if data.startswith(
            "chrules:"
        ):

            channel_id = int(
                data.split(
                    ":",
                    1,
                )[1]
            )

            await show_rules(
                query,
                channel_id,
            )

            return

        # ----------------------------------------------------
        # ADD RULE GUI
        # ----------------------------------------------------

        if data.startswith(
            "addrulegui:"
        ):

            channel_id = int(
                data.split(
                    ":",
                    1,
                )[1]
            )

            if not user_owns_channel(
                query.from_user.id,
                channel_id,
            ):
                await query.answer(
                    "Access denied.",
                    show_alert=True,
                )
                return

            set_ui_state(
                query.from_user.id,
                {
                    "action": "add_rule_old",
                    "channel_id": channel_id,
                },
            )

            await query.edit_message_text(
                "➕ <b>ADD REPLACEMENT RULE</b>\n\n"
                "Send the <b>old text</b> that you want "
                "to replace.\n\n"
                "Example:\n"
                "<code>MovieHub</code>\n\n"
                "Send /cancel to cancel.",
                parse_mode="HTML",
            )

            return

        # ----------------------------------------------------
        # DELETE RULE GUI
        # ----------------------------------------------------

        if data.startswith(
            "delrulegui:"
        ):

            channel_id = int(
                data.split(
                    ":",
                    1,
                )[1]
            )

            if not user_owns_channel(
                query.from_user.id,
                channel_id,
            ):
                await query.answer(
                    "Access denied.",
                    show_alert=True,
                )
                return

            channel = get_channel_config(
                channel_id
            )

            rules = channel.get(
                "replacement_rules",
                {},
            )

            if not rules:

                await query.answer(
                    "No rules to delete.",
                    show_alert=True,
                )

                return

            keyboard = []

            for old in rules.keys():

                keyboard.append(
                    [
                        InlineKeyboardButton(
                            f"🗑 {short_text(old, 30)}",
                            callback_data=(
                                "deleterule:"
                                f"{channel_id}:"
                                f"{old[:40]}"
                            ),
                        )
                    ]
                )

            keyboard.append(
                [
                    InlineKeyboardButton(
                        "⬅️ Back",
                        callback_data=(
                            f"chrules:{channel_id}"
                        ),
                    )
                ]
            )

            await query.edit_message_text(
                "🗑 <b>DELETE RULE</b>\n\n"
                "Select the rule you want to delete:",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(
                    keyboard
                ),
            )

            return

        # ----------------------------------------------------
        # ACTUAL DELETE RULE
        # ----------------------------------------------------

        if data.startswith(
            "deleterule:"
        ):

            parts = data.split(
                ":",
                2,
            )

            channel_id = int(
                parts[1]
            )

            old_text = parts[2]

            if not user_owns_channel(
                query.from_user.id,
                channel_id,
            ):
                await query.answer(
                    "Access denied.",
                    show_alert=True,
                )
                return

            channel = get_channel_config(
                channel_id
            )

            rules = channel.get(
                "replacement_rules",
                {},
            )

            # Exact lookup first.
            target = old_text

            if target not in rules:

                for key in rules:

                    if key.startswith(
                        old_text
                    ) or old_text.startswith(
                        key
                    ):
                        target = key
                        break

            if target in rules:

                del rules[target]

                update_channel_config(
                    channel_id,
                    "replacement_rules",
                    rules,
                )

                await query.answer(
                    "Rule deleted.",
                    show_alert=False,
                )

            await show_rules(
                query,
                channel_id,
            )

            return

        # ----------------------------------------------------
        # DESIGN
        # ----------------------------------------------------

        if data == "design":

            await show_design(
                query
            )

            return

        if data.startswith(
            "chdesign:"
        ):

            channel_id = int(
                data.split(
                    ":",
                    1,
                )[1]
            )

            await show_design(
                query,
                channel_id,
            )

            return

        # ----------------------------------------------------
        # HEADER EDIT
        # ----------------------------------------------------

        if data.startswith(
            "editheader:"
        ):

            channel_id = int(
                data.split(
                    ":",
                    1,
                )[1]
            )

            if not user_owns_channel(
                query.from_user.id,
                channel_id,
            ):
                await query.answer(
                    "Access denied.",
                    show_alert=True,
                )
                return

            set_ui_state(
                query.from_user.id,
                {
                    "action": "header",
                    "channel_id": channel_id,
                },
            )

            await query.edit_message_text(
                "📌 <b>EDIT HEADER</b>\n\n"
                "Send your new header.\n\n"
                "Example:\n"
                "<code>🎬 Welcome to DG Movies</code>\n\n"
                "Send /cancel to cancel.",
                parse_mode="HTML",
            )

            return

        # ----------------------------------------------------
        # FOOTER EDIT
        # ----------------------------------------------------

        if data.startswith(
            "editfooter:"
        ):

            channel_id = int(
                data.split(
                    ":",
                    1,
                )[1]
            )

            if not user_owns_channel(
                query.from_user.id,
                channel_id,
            ):
                await query.answer(
                    "Access denied.",
                    show_alert=True,
                )
                return

            set_ui_state(
                query.from_user.id,
                {
                    "action": "footer",
                    "channel_id": channel_id,
                },
            )

            await query.edit_message_text(
                "📌 <b>EDIT FOOTER</b>\n\n"
                "Send your new footer.\n\n"
                "Example:\n"
                "<code>⚡ Fast Download Links @DG_Contents</code>\n\n"
                "Send /cancel to cancel.",
                parse_mode="HTML",
            )

            return

        # ----------------------------------------------------
        # CLEAR HEADER
        # ----------------------------------------------------

        if data.startswith(
            "clearheader:"
        ):

            channel_id = int(
                data.split(
                    ":",
                    1,
                )[1]
            )

            if user_owns_channel(
                query.from_user.id,
                channel_id,
            ):

                update_channel_config(
                    channel_id,
                    "custom_header",
                    "",
                )

            await show_design(
                query,
                channel_id,
            )

            return

        # ----------------------------------------------------
        # CLEAR FOOTER
        # ----------------------------------------------------

        if data.startswith(
            "clearfooter:"
        ):

            channel_id = int(
                data.split(
                    ":",
                    1,
                )[1]
            )

            if user_owns_channel(
                query.from_user.id,
                channel_id,
            ):

                update_channel_config(
                    channel_id,
                    "custom_footer",
                    "",
                )

            await show_design(
                query,
                channel_id,
            )

            return

        # ----------------------------------------------------
        # PREVIEW
        # ----------------------------------------------------

        if data.startswith(
            "preview:"
        ):

            channel_id = int(
                data.split(
                    ":",
                    1,
                )[1]
            )

            if not user_owns_channel(
                query.from_user.id,
                channel_id,
            ):
                await query.answer(
                    "Access denied.",
                    show_alert=True,
                )
                return

            channel = get_channel_config(
                channel_id
            )

            sample = (
                "🎬 <b>Sample Movie Title</b>\n\n"
                "🎭 Action • Drama\n"
                "📅 2026\n\n"
                "This is how your caption "
                "will look after processing."
            )

            header = channel.get(
                "custom_header",
                "",
            ).strip()

            footer = channel.get(
                "custom_footer",
                "",
            ).strip()

            pieces = []

            if header:
                pieces.append(
                    esc(header)
                )

            pieces.append(
                sample
            )

            if footer:
                pieces.append(
                    esc(footer)
                )

            preview = (
                "👁 <b>CAPTION PREVIEW</b>\n\n"
                "━━━━━━━━━━━━━━━━━━\n\n"
                + "\n\n".join(pieces)
                + "\n\n━━━━━━━━━━━━━━━━━━"
            )

            await query.edit_message_text(
                preview,
                parse_mode="HTML",
                reply_markup=back_keyboard(
                    f"chdesign:{channel_id}"
                ),
            )

            return

        # ----------------------------------------------------
        # CHANNEL STATS
        # ----------------------------------------------------

        if data == "stats":

            await show_stats(
                query
            )

            return

        if data.startswith(
            "chstats:"
        ):

            channel_id = int(
                data.split(
                    ":",
                    1,
                )[1]
            )

            await show_stats(
                query,
                channel_id,
            )

            return

        # ----------------------------------------------------
        # TOGGLE CHANNEL ACTIVE
        # ----------------------------------------------------

        if data.startswith(
            "togglechannel:"
        ):

            channel_id = int(
                data.split(
                    ":",
                    1,
                )[1]
            )

            if not user_owns_channel(
                query.from_user.id,
                channel_id,
            ):
                await query.answer(
                    "Access denied.",
                    show_alert=True,
                )
                return

            channel = get_channel_config(
                channel_id
            )

            active = channel.get(
                "active",
                True,
            )

            update_channel_config(
                channel_id,
                "active",
                not active,
            )

            await query.answer(
                "Channel activated."
                if not active
                else "Channel disconnected.",
            )

            await show_channel_dashboard(
                query,
                channel_id,
            )

            return

        # ----------------------------------------------------
        # ADMIN
        # ----------------------------------------------------

        if data == "admin":

            await show_admin(
                query
            )

            return

        if data == "adminchannels":

            await show_admin_channels(
                query
            )

            return

        # ----------------------------------------------------
        # HELP
        # ----------------------------------------------------

        if data == "help":

            await show_help(
                query
            )

            return

    except Exception as exc:

        logger.exception(
            "Button handler error: %s",
            exc,
        )

        try:
            await query.answer(
                "Something went wrong.",
                show_alert=True,
            )
        except Exception:
            pass


# ============================================================
# PRIVATE TEXT WIZARD
# ============================================================

async def private_text_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not update.message:
        return

    user_id = update.effective_user.id

    state = get_ui_state(
        user_id
    )

    if not state:
        return

    text = (
        update.message.text or ""
    ).strip()

    if not text:
        return

    # --------------------------------------------------------
    # CANCEL
    # --------------------------------------------------------

    if text.lower() in (
        "/cancel",
        "cancel",
    ):

        clear_ui_state(
            user_id
        )

        await update.message.reply_text(
            "❌ <b>Cancelled.</b>",
            parse_mode="HTML",
            reply_markup=main_keyboard(),
        )

        return

    action = state.get(
        "action"
    )

    channel_id = state.get(
        "channel_id"
    )

    if not channel_id:
        clear_ui_state(
            user_id
        )
        return

    if not user_owns_channel(
        user_id,
        channel_id,
    ):
        clear_ui_state(
            user_id
        )

        await update.message.reply_text(
            "⛔ Access denied.",
            parse_mode="HTML",
        )

        return

    # --------------------------------------------------------
    # ADD RULE - OLD
    # --------------------------------------------------------

    if action == "add_rule_old":

        set_ui_state(
            user_id,
            {
                "action": "add_rule_new",
                "channel_id": channel_id,
                "old_text": text,
            },
        )

        await update.message.reply_text(
            "🔄 <b>ADD REPLACEMENT RULE</b>\n\n"
            f"Old text:\n"
            f"<code>{esc(text)}</code>\n\n"
            "Now send the <b>new replacement text</b>.\n\n"
            "Example:\n"
            "<code>DG_Contents</code>",
            parse_mode="HTML",
        )

        return

    # --------------------------------------------------------
    # ADD RULE - NEW
    # --------------------------------------------------------

    if action == "add_rule_new":

        old_text = state.get(
            "old_text",
            "",
        )

        new_text = text

        if not old_text or not new_text:

            await update.message.reply_text(
                "❌ Both values are required.",
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

        rules[old_text] = {
            "new": new_text,
            "added_by": user_id,
        }

        update_channel_config(
            channel_id,
            "replacement_rules",
            rules,
        )

        clear_ui_state(
            user_id
        )

        await update.message.reply_text(
            "✅ <b>RULE ADDED</b>\n\n"
            f"🔍 <code>{esc(old_text)}</code>\n"
            f"➜ <code>{esc(new_text)}</code>\n\n"
            "⚡ Rule is live now.",
            parse_mode="HTML",
            reply_markup=main_keyboard(),
        )

        return

    # --------------------------------------------------------
    # HEADER
    # --------------------------------------------------------

    if action == "header":

        update_channel_config(
            channel_id,
            "custom_header",
            text,
        )

        clear_ui_state(
            user_id
        )

        await update.message.reply_text(
            "✅ <b>HEADER UPDATED</b>\n\n"
            f"{esc(text)}",
            parse_mode="HTML",
            reply_markup=main_keyboard(),
        )

        return

    # --------------------------------------------------------
    # FOOTER
    # --------------------------------------------------------

    if action == "footer":

        update_channel_config(
            channel_id,
            "custom_footer",
            text,
        )

        clear_ui_state(
            user_id
        )

        await update.message.reply_text(
            "✅ <b>FOOTER UPDATED</b>\n\n"
            f"{esc(text)}",
            parse_mode="HTML",
            reply_markup=main_keyboard(),
        )

        return


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

    try:

        config = await get_cached_channel_config_async(
            channel_id
        )

        if not config:
            return

        if not config.get(
            "active",
            True,
        ):
            return

        text_to_check = (
            msg.caption
            if msg.caption is not None
            else msg.text
        )

        if not text_to_check:
            return

        transformed = transform_caption(
            text_to_check,
            config,
        )

        final_plain = transformed[
            "plain"
        ]

        final_html = transformed[
            "html"
        ]

        # Prevent self-edit loops.
        if (
            text_to_check.strip()
            == final_plain
        ):
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
            return

        job = {
            "bot": context.bot,
            "channel_id": channel_id,
            "message_id": message_id,
            "content": final_html,
            "final_plain": final_plain,
            "is_caption": (
                msg.caption is not None
            ),
            "replacements": transformed[
                "replacements"
            ],
            "links_removed": transformed[
                "links_removed"
            ],
            "mentions_removed": transformed[
                "mentions_removed"
            ],
        }

        await enqueue_caption_job(
            job
        )

    except asyncio.QueueFull:

        logger.error(
            "Caption queue full."
        )

    except Exception as exc:

        logger.exception(
            "Caption processing error: %s",
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

    chat = chat_member_update.chat

    if chat.type != ChatType.CHANNEL:
        return

    new_status = (
        chat_member_update
        .new_chat_member
        .status
    )

    actor_user_id = (
        chat_member_update
        .from_user
        .id
    )

    channel_id = chat.id

    # --------------------------------------------------------
    # ADDED AS ADMIN
    # --------------------------------------------------------

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
                return

            title = (
                chat.title or ""
            )

            username = (
                chat.username or ""
            )

            existing = channels_col.find_one(
                {"_id": str(channel_id)}
            )

            if existing:

                channels_col.update_one(
                    {
                        "_id": str(channel_id)
                    },
                    {
                        "$set": {
                            "title": title,
                            "username": username,
                            "owner_user_id": actor_user_id,
                            "active": existing.get(
                                "active",
                                False,
                            ),
                        }
                    },
                )

            else:

                config = default_channel_config(
                    channel_id
                )

                config[
                    "owner_user_id"
                ] = actor_user_id

                config[
                    "active"
                ] = True

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
                    "🎉 <b>CHANNEL CONNECTED!</b>\n\n"
                    f"📢 <b>{esc(title or 'Your Channel')}</b>\n"
                    f"🆔 <code>{channel_id}</code>\n\n"
                    "⚡ Your Premium Caption Engine "
                    "is ready.\n\n"
                    "Open /start and configure your "
                    "channel from the dashboard."
                ),
                parse_mode="HTML",
            )

            await send_important_log(
                context.bot,
                "CHANNEL CONNECTED",
                (
                    f"Channel: {title}\n"
                    f"Channel ID: {channel_id}\n"
                    f"Owner: {actor_user_id}"
                ),
                "CHANNEL",
            )

        except Exception as exc:

            logger.exception(
                "Channel connection failed: %s",
                exc,
            )

    # --------------------------------------------------------
    # REMOVED / BANNED
    # --------------------------------------------------------

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

            logger.warning(
                "Disconnect handling failed: %s",
                exc,
            )


# ============================================================
# COMMAND: CONNECT
# ============================================================

async def connect_channel(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    user_id = update.effective_user.id

    if not context.args:

        await update.message.reply_text(
            "📢 <b>CONNECT CHANNEL</b>\n\n"
            "Use:\n"
            "<code>/connect CHANNEL_ID</code>",
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

    channel = channels_col.find_one(
        {
            "_id": str(channel_id)
        }
    )

    if not channel:

        await update.message.reply_text(
            "❌ <b>Channel not found.</b>\n\n"
            "First add me as Administrator "
            "to your channel.",
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
            "This channel isn't connected "
            "to your account.",
            parse_mode="HTML",
        )

        return

    try:

        me = await context.bot.get_me()

        member = (
            await context.bot.get_chat_member(
                chat_id=channel_id,
                user_id=me.id,
            )
        )

        if (
            member.status
            != ChatMemberStatus.ADMINISTRATOR
        ):

            await update.message.reply_text(
                "❌ Bot must be an Administrator "
                "in this channel.",
                parse_mode="HTML",
            )

            return

        update_channel_config(
            channel_id,
            "active",
            True,
        )

        set_selected_channel(
            user_id,
            channel_id,
        )

        await update.message.reply_text(
            "✅ <b>CHANNEL ACTIVATED</b>\n\n"
            f"📢 <b>{esc(channel.get('title', 'Channel'))}</b>\n"
            f"🆔 <code>{channel_id}</code>\n\n"
            "⚡ Automatic caption processing "
            "is now active.",
            parse_mode="HTML",
            reply_markup=main_keyboard(),
        )

    except Exception as exc:

        logger.exception(
            "Connect error: %s",
            exc,
        )

        await update.message.reply_text(
            "❌ Connection failed. "
            "Check bot permissions.",
        )


# ============================================================
# COMMAND: CHANNELS
# ============================================================

async def channels_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    channels = get_user_channels(
        update.effective_user.id
    )

    if not channels:

        await update.message.reply_text(
            "📢 <b>MY CHANNELS</b>\n\n"
            "No connected channels yet.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "➕ Add Me to Channel",
                            url=(
                                f"https://t.me/"
                                f"{BOT_USERNAME}"
                                "?startchannel=true"
                            ),
                        )
                    ]
                ]
            ),
        )

        return

    selected = get_selected_channel(
        update.effective_user.id
    )

    text = (
        "📢 <b>MY CHANNELS</b>\n\n"
    )

    for channel in channels:

        marker = (
            " ⭐ SELECTED"
            if selected
            == channel.get(
                "channel_id"
            )
            else ""
        )

        status = (
            "🟢"
            if channel.get(
                "active",
                True,
            )
            else "🔴"
        )

        text += (
            f"{status} <b>{esc(channel.get('title', 'Channel'))}</b>"
            f"{marker}\n"
            f"🆔 <code>{channel.get('channel_id')}</code>\n\n"
        )

    await update.message.reply_text(
        text,
        parse_mode="HTML",
        reply_markup=channel_list_keyboard(
            channels
        ),
    )


# ============================================================
# COMMAND: USECHANNEL
# ============================================================

async def use_channel(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not context.args:

        await update.message.reply_text(
            "Use:\n"
            "<code>/usechannel CHANNEL_ID</code>",
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

    if not user_owns_channel(
        update.effective_user.id,
        channel_id,
    ):

        await update.message.reply_text(
            "⛔ This channel is not connected "
            "to your account.",
            parse_mode="HTML",
        )

        return

    set_selected_channel(
        update.effective_user.id,
        channel_id,
    )

    channel = get_channel_config(
        channel_id
    )

    await update.message.reply_text(
        "✅ <b>CHANNEL SELECTED</b>\n\n"
        f"📢 <b>{esc(channel.get('title', 'Channel'))}</b>\n\n"
        "All settings will now apply to "
        "this channel.",
        parse_mode="HTML",
        reply_markup=main_keyboard(),
    )


# ============================================================
# COMMAND: ADDRULE
# ============================================================

async def add_rule(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    channel_id = get_selected_channel(
        update.effective_user.id
    )

    if not channel_id:

        await update.message.reply_text(
            "⚠️ Select a channel first.",
            parse_mode="HTML",
        )

        return

    if not user_owns_channel(
        update.effective_user.id,
        channel_id,
    ):

        await update.message.reply_text(
            "⛔ Access denied.",
            parse_mode="HTML",
        )

        return

    raw = " ".join(
        context.args
    )

    if " -> " not in raw:

        await update.message.reply_text(
            "🔄 <b>ADD RULE</b>\n\n"
            "<code>/addrule old -> new</code>\n\n"
            "Example:\n"
            "<code>/addrule MovieHub -> DG_Contents</code>",
            parse_mode="HTML",
        )

        return

    old_text, new_text = raw.split(
        " -> ",
        1,
    )

    old_text = old_text.strip()
    new_text = new_text.strip()

    if not old_text or not new_text:

        await update.message.reply_text(
            "❌ Both values are required.",
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

    rules[old_text] = {
        "new": new_text,
        "added_by": update.effective_user.id,
    }

    update_channel_config(
        channel_id,
        "replacement_rules",
        rules,
    )

    await update.message.reply_text(
        "✅ <b>RULE ADDED</b>\n\n"
        f"🔍 <code>{esc(old_text)}</code>\n"
        f"➜ <code>{esc(new_text)}</code>",
        parse_mode="HTML",
    )


# ============================================================
# COMMAND: DELRULE
# ============================================================

async def del_rule(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    channel_id = get_selected_channel(
        update.effective_user.id
    )

    if not channel_id:
        return

    old_text = " ".join(
        context.args
    ).strip()

    if not old_text:

        await update.message.reply_text(
            "Use:\n"
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
            "❌ Rule not found.",
            parse_mode="HTML",
        )

        return

    rule = rules[
        old_text
    ]

    owner = rule_owner(
        rule
    )

    if (
        not is_admin(
            update.effective_user.id
        )
        and owner != update.effective_user.id
    ):

        await update.message.reply_text(
            "⛔ You can only delete your own rule.",
            parse_mode="HTML",
        )

        return

    del rules[
        old_text
    ]

    update_channel_config(
        channel_id,
        "replacement_rules",
        rules,
    )

    await update.message.reply_text(
        "🗑 <b>RULE DELETED</b>\n\n"
        f"<code>{esc(old_text)}</code>",
        parse_mode="HTML",
    )


# ============================================================
# COMMAND: SETHEADER
# ============================================================

async def set_header(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    channel_id = get_selected_channel(
        update.effective_user.id
    )

    if not channel_id:
        return

    if not user_owns_channel(
        update.effective_user.id,
        channel_id,
    ):
        return

    text = " ".join(
        context.args
    ).strip()

    update_channel_config(
        channel_id,
        "custom_header",
        text,
    )

    await update.message.reply_text(
        "✅ <b>HEADER UPDATED</b>",
        parse_mode="HTML",
    )


# ============================================================
# COMMAND: SETFOOTER
# ============================================================

async def set_footer(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    channel_id = get_selected_channel(
        update.effective_user.id
    )

    if not channel_id:
        return

    if not user_owns_channel(
        update.effective_user.id,
        channel_id,
    ):
        return

    text = " ".join(
        context.args
    ).strip()

    update_channel_config(
        channel_id,
        "custom_footer",
        text,
    )

    await update.message.reply_text(
        "✅ <b>FOOTER UPDATED</b>",
        parse_mode="HTML",
    )


# ============================================================
# COMMAND: STATUS
# ============================================================

async def status_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    channel_id = get_selected_channel(
        update.effective_user.id
    )

    if not channel_id:

        await update.message.reply_text(
            "⚠️ Select a channel first.",
            parse_mode="HTML",
        )

        return

    channel = get_channel_config(
        channel_id
    )

    stats = get_channel_stats(
        channel_id
    )

    rules = channel.get(
        "replacement_rules",
        {},
    )

    text = (
        "⚡ <b>AUTO CAPTION ENGINE</b>\n"
        "<i>Channel Status</i>\n\n"
        f"📢 <b>{esc(channel.get('title', 'Channel'))}</b>\n\n"
        f"🟢 Status: "
        f"<b>{'ACTIVE' if channel.get('active', True) else 'INACTIVE'}</b>\n\n"
        "📊 <b>STATISTICS</b>\n"
        f"├─ Processed: <b>{stats.get('processed', 0)}</b>\n"
        f"├─ Replacements: <b>{stats.get('replacements', 0)}</b>\n"
        f"├─ Links Removed: <b>{stats.get('links_removed', 0)}</b>\n"
        f"└─ Errors: <b>{stats.get('errors', 0)}</b>\n\n"
        f"🔄 Rules: <b>{len(rules)}</b>\n"
        f"📦 Queue: <b>{_caption_queue.qsize()}</b>"
    )

    await update.message.reply_text(
        text,
        parse_mode="HTML",
        reply_markup=main_keyboard(),
    )


# ============================================================
# COMMAND: STATS
# ============================================================

async def stats_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    channel_id = get_selected_channel(
        update.effective_user.id
    )

    if not channel_id:

        await update.message.reply_text(
            "⚠️ Select a channel first.",
            parse_mode="HTML",
        )

        return

    class DummyQuery:
        pass

    # Direct text response for command use.
    channel = get_channel_config(
        channel_id
    )

    stats = get_channel_stats(
        channel_id
    )

    text = (
        "📊 <b>STATISTICS</b>\n\n"
        f"📢 <b>{esc(channel.get('title', 'Channel'))}</b>\n\n"
        f"📝 Processed: <b>{stats.get('processed', 0)}</b>\n"
        f"🔄 Replacements: <b>{stats.get('replacements', 0)}</b>\n"
        f"🧹 Links Removed: <b>{stats.get('links_removed', 0)}</b>\n"
        f"👤 Mentions Removed: <b>{stats.get('mentions_removed', 0)}</b>\n"
        f"⚠️ Errors: <b>{stats.get('errors', 0)}</b>"
    )

    await update.message.reply_text(
        text,
        parse_mode="HTML",
    )


# ============================================================
# COMMAND: CLEAR
# ============================================================

async def clear_rules(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    channel_id = get_selected_channel(
        update.effective_user.id
    )

    if not channel_id:
        return

    if not user_owns_channel(
        update.effective_user.id,
        channel_id,
    ):
        return

    update_channel_config(
        channel_id,
        "replacement_rules",
        {},
    )

    await update.message.reply_text(
        "🧹 <b>ALL RULES CLEARED</b>\n\n"
        "The selected channel now has "
        "no replacement rules.",
        parse_mode="HTML",
    )


# ============================================================
# COMMAND: DISCONNECT
# ============================================================

async def disconnect_channel(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    user_id = update.effective_user.id

    if not context.args:

        await update.message.reply_text(
            "Use:\n"
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

    if not is_admin(
        user_id
    ):

        if not user_owns_channel(
            user_id,
            channel_id,
        ):

            await update.message.reply_text(
                "⛔ Access denied.",
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
        "Automatic processing is now disabled.",
        parse_mode="HTML",
        reply_markup=main_keyboard(),
    )


# ============================================================
# COMMAND: ADMIN
# ============================================================

async def admin_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not is_admin(
        update.effective_user.id
    ):

        await update.message.reply_text(
            "⛔ Admin access required.",
            parse_mode="HTML",
        )

        return

    users_count = users_col.count_documents({})
    channels_count = channels_col.count_documents({})
    active_channels = channels_col.count_documents(
        {
            "active": True
        }
    )

    stats = get_global_stats()

    text = (
        "👑 <b>ADMIN CONTROL CENTER</b>\n\n"
        f"👥 Users: <b>{users_count}</b>\n"
        f"📢 Channels: <b>{channels_count}</b>\n"
        f"🟢 Active: <b>{active_channels}</b>\n"
        f"📝 Processed: <b>{stats.get('processed', 0)}</b>\n"
        f"🔄 Replacements: <b>{stats.get('replacements', 0)}</b>\n"
        f"⚠️ Errors: <b>{stats.get('errors', 0)}</b>\n\n"
        f"📦 Queue: <b>{_caption_queue.qsize()}</b>"
    )

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "📊 Dashboard",
                    callback_data="admin",
                )
            ],
            [
                InlineKeyboardButton(
                    "📢 Channels",
                    callback_data="adminchannels",
                )
            ],
            [
                InlineKeyboardButton(
                    "🏠 Home",
                    callback_data="home",
                )
            ],
        ]
    )

    await update.message.reply_text(
        text,
        parse_mode="HTML",
        reply_markup=keyboard,
    )


# ============================================================
# COMMAND: HELP
# ============================================================

async def help_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    text = (
        "❓ <b>AUTO CAPTION ENGINE</b>\n\n"
        "⚡ Premium automatic caption editor.\n\n"
        "📢 <b>CHANNEL</b>\n"
        "<code>/channels</code>\n"
        "<code>/usechannel CHANNEL_ID</code>\n"
        "<code>/connect CHANNEL_ID</code>\n\n"
        "🔄 <b>RULES</b>\n"
        "<code>/addrule old -> new</code>\n"
        "<code>/delrule old</code>\n"
        "<code>/clear</code>\n\n"
        "🎨 <b>DESIGN</b>\n"
        "<code>/setheader text</code>\n"
        "<code>/setfooter text</code>\n\n"
        "📊 <b>MONITORING</b>\n"
        "<code>/status</code>\n"
        "<code>/stats</code>\n\n"
        "🏠 Use /start for the Premium Dashboard."
    )

    await update.message.reply_text(
        text,
        parse_mode="HTML",
        reply_markup=main_keyboard(),
    )


# ============================================================
# ERROR HANDLER
# ============================================================

async def error_handler(
    update,
    context: ContextTypes.DEFAULT_TYPE,
):

    error = context.error

    if isinstance(
        error,
        RetryAfter,
    ):

        logger.warning(
            "Global RetryAfter: %.1fs",
            float(
                error.retry_after
            ),
        )

        return

    logger.exception(
        "Unhandled Telegram error: %s",
        error,
    )

    try:

        if context.bot:

            await send_important_log(
                context.bot,
                "BOT ERROR",
                (
                    f"{type(error).__name__}: "
                    f"{error}"
                ),
                "ERROR",
            )

    except Exception:
        pass


# ============================================================
# MAIN
# ============================================================

def main():

    # Render health server.
    threading.Thread(
        target=start_dummy_server,
        daemon=True,
    ).start()

    application = (
        ApplicationBuilder()
        .token(TOKEN)
        .concurrent_updates(20)
        .connection_pool_size(30)
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

    application.add_handler(
        CommandHandler(
            "start",
            start,
        )
    )

    application.add_handler(
        CommandHandler(
            "help",
            help_command,
        )
    )

    application.add_handler(
        CommandHandler(
            "connect",
            connect_channel,
        )
    )

    application.add_handler(
        CommandHandler(
            "channels",
            channels_command,
        )
    )

    application.add_handler(
        CommandHandler(
            "usechannel",
            use_channel,
        )
    )

    application.add_handler(
        CommandHandler(
            "addrule",
            add_rule,
        )
    )

    application.add_handler(
        CommandHandler(
            "delrule",
            del_rule,
        )
    )

    application.add_handler(
        CommandHandler(
            "setheader",
            set_header,
        )
    )

    application.add_handler(
        CommandHandler(
            "setfooter",
            set_footer,
        )
    )

    application.add_handler(
        CommandHandler(
            "status",
            status_command,
        )
    )

    application.add_handler(
        CommandHandler(
            "stats",
            stats_command,
        )
    )

    application.add_handler(
        CommandHandler(
            "clear",
            clear_rules,
        )
    )

    application.add_handler(
        CommandHandler(
            "disconnect",
            disconnect_channel,
        )
    )

    application.add_handler(
        CommandHandler(
            "admin",
            admin_command,
        )
    )

    # --------------------------------------------------------
    # Inline buttons
    # --------------------------------------------------------

    application.add_handler(
        CallbackQueryHandler(
            button_handler
        )
    )

    # --------------------------------------------------------
    # Channel membership
    # --------------------------------------------------------

    application.add_handler(
        ChatMemberHandler(
            handle_bot_channel_status,
            ChatMemberHandler.MY_CHAT_MEMBER,
        )
    )

    # --------------------------------------------------------
    # Channel posts
    # --------------------------------------------------------

    application.add_handler(
        MessageHandler(
            filters.UpdateType.CHANNEL_POST
            | filters.UpdateType.EDITED_CHANNEL_POST,
            edit_channel_caption,
        )
    )

    # --------------------------------------------------------
    # Private GUI wizard messages
    # --------------------------------------------------------

    application.add_handler(
        MessageHandler(
            filters.ChatType.PRIVATE
            & filters.TEXT
            & ~filters.COMMAND,
            private_text_handler,
        )
    )

    # --------------------------------------------------------
    # Error handler
    # --------------------------------------------------------

    application.add_error_handler(
        error_handler
    )

    logger.info(
        "⚡ Premium Auto Caption Engine starting..."
    )

    logger.info(
        "Batch: %d + %d | Cooldown: %.0fs",
        BATCH_SIZE,
        BATCH_SIZE,
        COOLDOWN_SECONDS,
    )

    logger.info(
        "Admins: %s",
        ADMIN_IDS,
    )

    logger.info(
        "Bot username: @%s",
        BOT_USERNAME,
    )

    application.run_polling(
        allowed_updates=Update.ALL_TYPES
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()
