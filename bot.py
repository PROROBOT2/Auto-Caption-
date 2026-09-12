import os
import sys
import asyncio
import http.server
import socketserver
import threading
import re
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters
from pymongo import MongoClient

# LOGGING SETUP (Render logs mein dikhega — debugging ke liye zaroori)
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# 1. Render Dummy Server (Keep Alive 24/7)
def start_dummy_server():
    PORT = int(os.environ.get("PORT", 10000))
    Handler = http.server.SimpleHTTPRequestHandler
    try:
        with socketserver.TCPServer(("", PORT), Handler) as httpd:
            httpd.serve_forever()
    except Exception:
        pass

# MONGO DB CONFIGURATION
MONGO_URI = os.environ.get("MONGO_URI")
if not MONGO_URI:
    sys.exit(1)

try:
    db_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db_client.admin.command('ping')
except Exception:
    sys.exit(1)

db = db_client["AutoCaptionBotDB"]
settings_col = db["bot_settings"]

# 🔐 SECURITY SETUP: GET ADMIN IDs (multiple admins supported)
# ADMIN_IDS env var format: "123456,987654,555555"
# Backward compatible: agar ADMIN_IDS nahi hai to purana OWNER_ID bhi chalega
def _parse_admin_ids():
    raw = os.environ.get("ADMIN_IDS", "").strip()
    ids = set()
    if raw:
        for part in raw.split(","):
            part = part.strip()
            if part.isdigit():
                ids.add(int(part))
    # backward compatibility with old OWNER_ID setup
    legacy_owner = os.environ.get("OWNER_ID", "0").strip()
    if legacy_owner.isdigit() and int(legacy_owner) != 0:
        ids.add(int(legacy_owner))
    return ids

ADMIN_IDS = _parse_admin_ids()

def is_admin(user_id: int) -> bool:
    # Agar koi admin configure hi nahi kiya, to koi restriction nahi (open mode)
    if not ADMIN_IDS:
        return True
    return user_id in ADMIN_IDS

# Har rule ab ek dict hoti hai: {"new": naya_text, "added_by": user_id, "approved": bool}
# Purane (legacy) plain-string rules ke saath backward compatible helpers:
def rule_value(rule):
    if isinstance(rule, dict):
        return rule.get("new", "")
    return rule

def rule_owner(rule):
    if isinstance(rule, dict):
        return rule.get("added_by")
    return None  # legacy/system rule -> admin-owned mana jaata hai

def rule_approved(rule):
    if isinstance(rule, dict):
        return rule.get("approved", True)
    return True  # legacy rules already-live the

def get_bot_settings():
    default_settings = {
        "_id": "config",
        "replacement_rules": {
            "MovieHub": {"new": "DG_Contents", "added_by": None, "approved": True},
            "JoinUs": {"new": "SubscribeNow", "added_by": None, "approved": True},
        },
        "custom_header": "",
        "custom_footer": "⚡️ Fast Download Links @DG_Contents"
    }
    try:
        config = settings_col.find_one({"_id": "config"})
        if not config:
            settings_col.insert_one(default_settings)
            return default_settings
        return config
    except Exception:
        return default_settings

def update_bot_settings(field_name, field_value):
    try:
        settings_col.update_one({"_id": "config"}, {"$set": {field_name: field_value}}, upsert=True)
    except Exception:
        pass

raw_log_id = os.environ.get("LOG_CHANNEL_ID")
LOG_CHANNEL_ID = int(raw_log_id) if raw_log_id and raw_log_id.strip() else None

# 2. Premium Auto-Cleaner & Caption Editor Logic (Owner ke channels ke liye)
async def edit_channel_caption(update: Update, context: ContextTypes.DEFAULT_TYPE):
    config = get_bot_settings()
    replacement_rules = config.get("replacement_rules", {})
    custom_header = config.get("custom_header", "")
    custom_footer = config.get("custom_footer", "")

    msg = update.channel_post or update.edited_channel_post
    if not msg:
        return

    logger.info(f"📩 Channel post mila. Chat ID: {msg.chat_id}, Message ID: {msg.message_id}")

    text_to_check = msg.text or msg.caption
    if not text_to_check:
        logger.info("⚠️ Is post mein na text hai na caption — skip kar raha hoon.")
        return

    final_text = text_to_check

    try:
        # AUTOMATIC SPAM REMOVER
        final_text = re.sub(r'(https?://)?t\.me/(?!DG_Contents|dghelps_bot)[a-zA-Z0-9_]+', '', final_text)
        final_text = re.sub(r'@(?!DG_Contents|dghelps_bot)[a-zA-Z0-9_]+', '', final_text)

        # REPLACEMENT LOOP (old_txt ko literal text treat karte hain, regex nahi —
        # warna special characters wale rules crash kar dete the)
        # Sirf APPROVED rules hi live channel caption pe apply hote hain —
        # public users ke pending (unapproved) rules yahan skip ho jaate hain.
        for old_txt, rule in replacement_rules.items():
            if not rule_approved(rule):
                continue
            new_txt = rule_value(rule)
            pattern = re.compile(re.escape(old_txt), re.IGNORECASE)
            final_text = pattern.sub(new_txt, final_text)

        final_text = re.sub(r' +', ' ', final_text).strip()
    except Exception as e:
        logger.error(f"❌ Text processing FAIL ho gaya (replacement rules check karo): {e}")
        return

    header_part = f"<b>{custom_header}</b>\n\n" if custom_header else ""
    footer_part = f"\n\n<b>{custom_footer}</b>" if custom_footer else ""
    bold_text = f"{header_part}<b>{final_text}</b>{footer_part}"

    try:
        if msg.caption:
            if msg.caption_html == bold_text:
                logger.info("ℹ️ Caption already same hai, edit skip kar raha hoon.")
                return
            await context.bot.edit_message_caption(chat_id=msg.chat_id, message_id=msg.message_id, caption=bold_text, parse_mode="HTML")
            logger.info("✅ Caption successfully edit ho gaya.")
        elif msg.text:
            if msg.text_html == bold_text:
                logger.info("ℹ️ Text already same hai, edit skip kar raha hoon.")
                return
            await context.bot.edit_message_text(chat_id=msg.chat_id, message_id=msg.message_id, text=bold_text, parse_mode="HTML")
            logger.info("✅ Text successfully edit ho gaya.")

        if LOG_CHANNEL_ID:
            try:
                await context.bot.copy_message(chat_id=LOG_CHANNEL_ID, from_chat_id=msg.chat_id, message_id=msg.message_id)
            except Exception as log_err:
                logger.warning(f"⚠️ Log channel mein copy nahi ho paya: {log_err}")
    except Exception as e:
        logger.error(f"❌ Caption/Text edit FAIL ho gaya: {e}")

# 3. Commands Setup
# add_rule: HAR user use kar sakta hai. Admin ka rule turant LIVE ho jaata hai.
# Public user ka rule "pending" mein jaata hai jab tak koi admin /approve na kare.
async def add_rule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    config = get_bot_settings()
    rules = config.get("replacement_rules", {})
    raw_args = " ".join(context.args)
    if " -> " not in raw_args:
        await update.message.reply_text("✨ <b>Sahi Format:</b>\n<code>/addrule purana_text -> naya_text</code>", parse_mode="HTML")
        return
    try:
        old_part, new_part = raw_args.split(" -> ", 1)
        old_part, new_part = old_part.strip(), new_part.strip()
        approved = is_admin(user_id)
        rules[old_part] = {"new": new_part, "added_by": user_id, "approved": approved}
        update_bot_settings("replacement_rules", rules)

        if approved:
            await update.message.reply_text("✅ <b>Success:</b> Filter rule live ho gaya aur MongoDB mein save ho gaya!", parse_mode="HTML")
        else:
            await update.message.reply_text(
                "🕒 <b>Submitted for review:</b> Aapka rule save ho gaya hai, lekin channel mein tab tak apply nahi hoga jab tak koi admin ise approve na kare.\n"
                "<code>/status</code> se aap iska status check kar sakte hain.",
                parse_mode="HTML"
            )
    except Exception:
        await update.message.reply_text("❌ <b>Error:</b> Kuch galat ho gaya, format check karo.", parse_mode="HTML")

# del_rule: admin kisi ka bhi rule delete kar sakta hai; public user sirf apna hi.
async def del_rule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    config = get_bot_settings()
    rules = config.get("replacement_rules", {})
    old_text = " ".join(context.args).strip()

    if old_text not in rules:
        await update.message.reply_text("❌ <b>Error:</b> Word nahi mila.", parse_mode="HTML")
        return

    rule = rules[old_text]
    if is_admin(user_id) or rule_owner(rule) == user_id:
        del rules[old_text]
        update_bot_settings("replacement_rules", rules)
        await update.message.reply_text("🗑️ <b>Success:</b> Rule permanently deleted from cloud database.", parse_mode="HTML")
    else:
        await update.message.reply_text("⛔ <b>Access Denied:</b> Yeh rule kisi aur ne add kiya hai, aap ise delete nahi kar sakte.", parse_mode="HTML")

# approve_rule: ADMIN ONLY — public user ke pending rule ko live karta hai.
async def approve_rule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ <b>Access Denied:</b> Sirf admin approve kar sakte hain.", parse_mode="HTML")
        return
    config = get_bot_settings()
    rules = config.get("replacement_rules", {})
    old_text = " ".join(context.args).strip()
    if old_text not in rules:
        await update.message.reply_text("❌ <b>Error:</b> Word nahi mila.", parse_mode="HTML")
        return
    rule = rules[old_text]
    if not isinstance(rule, dict):
        rule = {"new": rule, "added_by": None}
    rule["approved"] = True
    rules[old_text] = rule
    update_bot_settings("replacement_rules", rules)
    await update.message.reply_text(f"✅ <b>Approved:</b> <code>{old_text}</code> ab live hai channel mein.", parse_mode="HTML")

# pending: ADMIN ONLY — saare unapproved (public-submitted) rules dikhata hai.
async def pending(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ <b>Access Denied.</b>", parse_mode="HTML")
        return
    config = get_bot_settings()
    rules = config.get("replacement_rules", {})
    pending_rules = {old: r for old, r in rules.items() if not rule_approved(r)}

    if not pending_rules:
        await update.message.reply_text("✅ <i>Koi pending rule nahi hai — sab clear!</i>", parse_mode="HTML")
        return

    msg = "🕒 <b>Pending Rules (Review Chahiye):</b>\n\n"
    for old, r in pending_rules.items():
        owner = rule_owner(r)
        msg += f"🔍 <code>{old}</code> ➡️ <code>{rule_value(r)}</code>\n   👤 By: <code>{owner}</code>\n\n"
    msg += "Approve karne ke liye: <code>/approve purana_text</code>"
    await update.message.reply_text(msg, parse_mode="HTML")

async def set_footer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    footer_text = " ".join(context.args).strip()
    update_bot_settings("custom_footer", footer_text)
    await update.message.reply_text(f"📝 <b>Footer Set:</b>\n<code>{footer_text}</code>", parse_mode="HTML")

async def set_header(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    header_text = " ".join(context.args).strip()
    update_bot_settings("custom_header", header_text)
    await update.message.reply_text(f"📝 <b>Header Set:</b>\n<code>{header_text}</code>", parse_mode="HTML")

async def clear_rules(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    update_bot_settings("replacement_rules", {})
    await update.message.reply_text("🧹 <b>Database Reset:</b> Saare rules MongoDB se clear ho gaye!", parse_mode="HTML")

# /status — ADMIN: full dashboard (sab rules + settings). PUBLIC: sirf apne rules.
async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    config = get_bot_settings()
    replacement_rules = config.get("replacement_rules", {})
    custom_header = config.get("custom_header", "")
    custom_footer = config.get("custom_footer", "")

    if is_admin(user_id):
        status_msg = "⚙️ <b>𝖯𝖱𝖮 𝖡𝖮𝖲𝖲 𝖣𝖠𝖲𝖧𝖡𝖮𝖠𝖱𝖣 (📡 MongoDB Connected)</b>\n\n"
        status_msg += f"📢 <b>Log Status:</b> {'🟢 Connected' if LOG_CHANNEL_ID else '🔴 Disconnected'}\n"
        status_msg += f"🔝 <b>Active Header:</b> <code>{custom_header if custom_header else 'None'}</code>\n"
        status_msg += f"🔚 <b>Active Footer:</b> <code>{custom_footer if custom_footer else 'None'}</code>\n\n"
        status_msg += "📊 <b>Saare Word Replacement Rules:</b>\n"

        if not replacement_rules:
            status_msg += "<i>No active filters loaded in database.</i>"
        else:
            for old, rule in replacement_rules.items():
                tag = "🟢 Live" if rule_approved(rule) else "🕒 Pending"
                owner = rule_owner(rule)
                owner_tag = f" (by {owner})" if owner else ""
                status_msg += f"🔍 <code>{old}</code> ➡️ <code>{rule_value(rule) or '[REMOVED]'}</code> — {tag}{owner_tag}\n"
    else:
        my_rules = {old: r for old, r in replacement_rules.items() if rule_owner(r) == user_id}
        status_msg = "📊 <b>Aapke Rules:</b>\n\n"
        if not my_rules:
            status_msg += "<i>Aapne abhi tak koi rule add nahi kiya. </i><code>/addrule purana -> naya</code><i> se add karein.</i>"
        else:
            for old, rule in my_rules.items():
                tag = "🟢 Live" if rule_approved(rule) else "🕒 Pending (admin approval baaki hai)"
                status_msg += f"🔍 <code>{old}</code> ➡️ <code>{rule_value(rule)}</code> — {tag}\n"

    await update.message.reply_text(status_msg, parse_mode="HTML")

# PRIVATE WELCOME MESSAGE (admin vs public dono ke liye alag)
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_name = update.effective_user.first_name

    if is_admin(user_id):
        welcome_text = (
            f"⚡️ <b>Welcome, {user_name}!</b> (Admin)\n\n"
            f"🚀 <b>Auto Caption Engine v3.0 [PRO]</b>\n"
            f"⚡️ Status: 🟢 Online & Polling\n\n"
            f"🛠 <b>Admin Commands:</b>\n"
            f"• <b>/addrule</b> purana -> naya - Add filter (turant live)\n"
            f"• <b>/delrule</b> purana - Kisi bhi rule ko delete karo\n"
            f"• <b>/pending</b> - Public users ke pending rules dekho\n"
            f"• <b>/approve</b> purana - Ek pending rule ko live karo\n"
            f"• <b>/setheader</b> - Change top caption text\n"
            f"• <b>/setfooter</b> - Change bottom signature text\n"
            f"• <b>/status</b> - Full dashboard\n"
            f"• <b>/clear</b> - Reset all rules\n\n"
            f"ℹ️ <i>Just add me to your channel as admin, I will handle the rest with supersonic speed.</i>"
        )
    else:
        welcome_text = (
            f"👋 <b>Hello, {user_name}!</b>\n\n"
            f"🚀 <b>Auto Caption Engine v3.0</b>\n\n"
            f"🛠 <b>Aap ye kar sakte hain:</b>\n"
            f"• <b>/addrule</b> purana -> naya - Apna filter suggest karo (admin approval ke baad live hoga)\n"
            f"• <b>/delrule</b> purana - Apna khud ka submitted rule delete karo\n"
            f"• <b>/status</b> - Apne submitted rules aur unka status dekho\n\n"
            f"ℹ️ <i>Baaki settings (header/footer/global rules) sirf admin change kar sakte hain.</i>"
        )

    keyboard = [[
        InlineKeyboardButton("📢 Channel", url="https://t.me/dg_contents"),
        InlineKeyboardButton("👥 Support", url="https://t.me/dghelps_bot")
    ]]
    await update.message.reply_text(text=welcome_text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))

def main():
    threading.Thread(target=start_dummy_server, daemon=True).start()
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    TOKEN = os.environ.get("BOT_TOKEN")
    if not TOKEN:
        print("❌ BOT_TOKEN environment variable set nahi hai. Bot start nahi ho sakta.")
        sys.exit(1)

    app = ApplicationBuilder().token(TOKEN).build()

    # REGISTER COMMAND HANDLERS
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("addrule", add_rule))
    app.add_handler(CommandHandler("delrule", del_rule))
    app.add_handler(CommandHandler("setfooter", set_footer))
    app.add_handler(CommandHandler("setheader", set_header))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("clear", clear_rules))
    app.add_handler(CommandHandler("approve", approve_rule))
    app.add_handler(CommandHandler("pending", pending))

    # REGISTER MESSAGE HANDLER (channel posts + edited channel posts, naya add hua)
    app.add_handler(MessageHandler(
        filters.UpdateType.CHANNEL_POST | filters.UpdateType.EDITED_CHANNEL_POST,
        edit_channel_caption
    ))

    logger.info("🤖 Auto Caption Bot is starting... Polling active.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
