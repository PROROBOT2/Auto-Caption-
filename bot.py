import os
import sys
import asyncio
import http.server
import socketserver
import threading
import re
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters
from pymongo import MongoClient

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

# 🔐 SECURITY SETUP: GET OWNER ID
try:
    OWNER_ID = int(os.environ.get("OWNER_ID", 0))
except (ValueError, TypeError):
    OWNER_ID = 0

def get_bot_settings():
    default_settings = {
        "_id": "config",
        "replacement_rules": {"MovieHub": "DG_Contents", "JoinUs": "SubscribeNow"},
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
    if not msg: return

    text_to_check = msg.text or msg.caption
    if not text_to_check: return

    final_text = text_to_check
    
    # AUTOMATIC SPAM REMOVER
    final_text = re.sub(r'(https?://)?t\.me/(?!DG_Contents|dghelps_bot)[a-zA-Z0-9_]+', '', final_text)
    final_text = re.sub(r'@(?!DG_Contents|dghelps_bot)[a-zA-Z0-9_]+', '', final_text)

    # REPLACEMENT LOOP
    for old_txt, new_txt in replacement_rules.items():
        if re.search(old_txt, final_text, re.IGNORECASE):
            final_text = re.compile(old_txt, re.IGNORECASE).sub(new_txt, final_text)

    final_text = re.sub(r' +', ' ', final_text).strip()
    header_part = f"<b>{custom_header}</b>\n\n" if custom_header else ""
    footer_part = f"\n\n<b>{custom_footer}</b>" if custom_footer else ""
    bold_text = f"{header_part}<b>{final_text}</b>{footer_part}"

    try:
        if msg.caption:
            if msg.caption_html == bold_text: return
            await context.bot.edit_message_caption(chat_id=msg.chat_id, message_id=msg.message_id, caption=bold_text, parse_mode="HTML")
        elif msg.text:
            if msg.text_html == bold_text: return
            await context.bot.edit_message_text(chat_id=msg.chat_id, message_id=msg.message_id, text=bold_text, parse_mode="HTML")
        if LOG_CHANNEL_ID:
            try: await context.bot.copy_message(chat_id=LOG_CHANNEL_ID, from_chat_id=msg.chat_id, message_id=msg.message_id)
            except Exception: pass
    except Exception:
        pass

# 3. Dynamic Admin Commands Setup (STRICTLY OWNER ONLY)
async def add_rule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if OWNER_ID != 0 and update.effective_user.id != OWNER_ID:
        await update.message.reply_text("⛔ <b>Access Denied:</b> Yeh ek private bot hai.", parse_mode="HTML")
        return
        
    config = get_bot_settings()
    rules = config.get("replacement_rules", {})
    raw_args = " ".join(context.args)
    if " -> " not in raw_args:
        await update.message.reply_text("✨ <b>Sahi Format:</b>\n<code>/addrule purana_text -> naya_text</code>", parse_mode="HTML")
        return
    try:
        old_part, new_part = raw_args.split(" -> ", 1)
        rules[old_part.strip()] = new_part.strip()
        update_bot_settings("replacement_rules", rules)
        await update.message.reply_text("✅ <b>Success:</b> Filter rule active and saved permanently to MongoDB!", parse_mode="HTML")
    except Exception: pass

async def del_rule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if OWNER_ID != 0 and update.effective_user.id != OWNER_ID: return
    config = get_bot_settings()
    rules = config.get("replacement_rules", {})
    old_text = " ".join(context.args).strip()
    if old_text in rules:
        del rules[old_text]
        update_bot_settings("replacement_rules", rules)
        await update.message.reply_text("🗑️ <b>Success:</b> Rule permanently deleted from cloud database.", parse_mode="HTML")
    else:
        await update.message.reply_text("❌ <b>Error:</b> Word nahi mila.", parse_mode="HTML")

async def set_footer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if OWNER_ID != 0 and update.effective_user.id != OWNER_ID: return
    footer_text = " ".join(context.args).strip()
    update_bot_settings("custom_footer", footer_text)
    await update.message.reply_text(f"📝 <b>Footer Set:</b>\n<code>{footer_text}</code>", parse_mode="HTML")

async def set_header(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if OWNER_ID != 0 and update.effective_user.id != OWNER_ID: return
    header_text = " ".join(context.args).strip()
    update_bot_settings("custom_header", header_text)
    await update.message.reply_text(f"📝 <b>Header Set:</b>\n<code>{header_text}</code>", parse_mode="HTML")

async def clear_rules(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if OWNER_ID != 0 and update.effective_user.id != OWNER_ID: return
    update_bot_settings("replacement_rules", {})
    await update.message.reply_text("🧹 <b>Database Reset:</b> Saare rules MongoDB se clear ho gaye!", parse_mode="HTML")

# FULL DETAILED STATUS (/status)
async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if OWNER_ID != 0 and update.effective_user.id != OWNER_ID:
        await update.message.reply_text("⛔ <b>Access Denied:</b> Diagnostics panel locked.", parse_mode="HTML")
        return
        
    config = get_bot_settings()
    replacement_rules = config.get("replacement_rules", {})
    custom_header = config.get("custom_header", "")
    custom_footer = config.get("custom_footer", "")
    
    status_msg = "⚙️ <b>𝖯𝖱𝖮 𝖡𝖮𝖲𝖲 𝖣𝖠𝖲𝖧𝖡𝖮𝖠𝖱𝖣 (📡 MongoDB Connected)</b>\n\n"
    status_msg += f"📢 <b>Log Status:</b> {'🟢 Connected' if LOG_CHANNEL_ID else '🔴 Disconnected'}\n"
    status_msg += f"🔝 <b>Active Header:</b> <code>{custom_header if custom_header else 'None'}</code>\n"
    status_msg += f"🔚 <b>Active Footer:</b> <code>{custom_footer if custom_footer else 'None'}</code>\n\n"
    status_msg += "📊 <b>Word Replacement Rules:</b>\n"
    
    if not replacement_rules:
        status_msg += "<i>No active filters loaded in database.</i>"
    else:
        for old, new in replacement_rules.items():
            status_msg += f"🔍 <code>{old}</code> ➡️ <code>{new if new else '[REMOVED]'}</code>\n"
            
    await update.message.reply_text(status_msg, parse_mode="HTML")

# PRIVATE WELCOME MESSAGE
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_name = update.effective_user.first_name
    
    if OWNER_ID != 0 and user_id != OWNER_ID:
        await update.message.reply_text(
            f"🔒 <b>Hello, {user_name}!</b>\n\n"
            f"ℹ️ <i>Yeh ek private bot hai aur sirf owner ke authorized channels ke liye kaam karta hai. Aap iske controls use nahi kar sakte.</i>"
        )
        return

    welcome_text = (
        f"⚡️ <b>Welcome, {user_name}!</b>\n\n"
        f"🚀 <b>Auto Caption Engine v3.0 [PRO]</b>\n"
        f"⚡️ Status: 🟢 Online & Polling\n\n"
        f"🛠 <b>Commands Center:</b>\n"
        f"• <b>/addrule</b> - Add text filter / replacement\n"
        f"• <b>/delrule</b> - Delete any active filter\n"
        f"• <b>/setheader</b> - Change top caption text\n"
        f"• <b>/setfooter</b> - Change bottom signature text\n"
        f"• <b>/status</b> - Open professional monitoring panel\n"
        f"• <b>/clear</b> - Reset all database configurations\n\n"
        f"ℹ️ <i>Just add me to your channel as admin, I will handle the rest with supersonic speed.</i>"
    )
    keyboard = [[
        InlineKeyboardButton("📢 Channel", url="https://t.me/dg_contents"), 
        InlineKeyboardButton("👥 Support", url="https://t.me/dghelps_bot")
    ]]
    await update.message.reply_text(text=welcome_text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))

def main():
    threading.Thread(target=start_dummy_server, daemon=True).start()
    try: loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
    TOKEN = os.environ.get("BOT_TOKEN")
    app = ApplicationBuilder().token(TOKEN).build()
    
    # REGISTER COMMAND HANDLERS
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("addrule", add_rule))
    app.add_handler(CommandHandler("delrule", del_rule))
    app.add_handler(CommandHandler("setfooter", set_footer))
    app.add_handler(CommandHandler("setheader", set_header))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("clear", clear_rules))
