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

def get_bot_settings():
    default_settings = {
        "_id": "config",
        "replacement_rules": {"MovieHub": "DG_Contents", "JoinUs": "SubscribeNow"},
        "custom_header": "",
        "custom_footer": "⚡ Fast Download Links @DG_Contents"
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

# 2. Premium Auto-Cleaner & Caption Editor Logic
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
    
    # 🛑 AUTOMATIC SPAM REMOVER PATTERN
    final_text = re.sub(r'(https?://)?t\.me/(?!DG_Contents|dghelps_bot)[a-zA-Z0-9_]+', '', final_text)
    final_text = re.sub(r'@(?!DG_Contents|dghelps_bot)[a-zA-Z0-9_]+', '', final_text)

    # 🔍 FILTER REPLACEMENT LOOP
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

# 3. Dynamic Admin Commands Setup
async def add_rule(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        await update.message.reply_text("✅ <b>Success:</b> Rule permanently synchronized to cloud database.", parse_mode="HTML")
    except Exception: pass

async def del_rule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    config = get_bot_settings()
    rules = config.get("replacement_rules", {})
    old_text = " ".join(context.args).strip()
    if old_text in rules:
        del rules[old_text]
        update_bot_settings("replacement_rules", rules)
        await update.message.reply_text("🗑️ <b>Success:</b> Rule removed from cloud database.", parse_mode="HTML")
    else:
        await update.message.reply_text("❌ <b>Error:</b> Targeted filter rule not active.", parse_mode="HTML")

async def set_footer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    footer_text = " ".join(context.args).strip()
    update_bot_settings("custom_footer", footer_text)
    await update.message.reply_text(f"📝 <b>Global Footer Set:</b>\n<code>{footer_text}</code>", parse_mode="HTML")

async def set_header(update: Update, context: ContextTypes.DEFAULT_TYPE):
    header_text = " ".join(context.args).strip()
    update_bot_settings("custom_header", header_text)
    await update.message.reply_text(f"📝 <b>Global Header Set:</b>\n<code>{header_text}</code>", parse_mode="HTML")

async def clear_rules(update: Update, context: ContextTypes.DEFAULT_TYPE):
    update_bot_settings("replacement_rules", {})
    await update.message.reply_text("🧹 <b>Database Reset:</b> All rules flushed out successfully.", parse_mode="HTML")

# 🔥 1. HIGH-END CORE MONITORING DASHBOARD (/status)
async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    config = get_bot_settings()
    replacement_rules = config.get("replacement_rules", {})
    custom_header = config.get("custom_header", "")
    custom_footer = config.get("custom_footer", "")
    
    status_msg = (
        "⚙️ <b><u>AUTOMATION SYSTEM DIAGNOSTICS</u></b>\n\n"
        f"🌐 <b>Database Status:</b> <code>🟢 MongoDB Connected</code>\n"
        f"📢 <b>Log Sync Status:</b> <code>{'🟢 Active' if LOG_CHANNEL_ID else '🔴 Inactive'}</code>\n\n"
        f"▪️ <b>Global Header:</b>\n<code>{custom_header if custom_header else '[Not Defined]'}</code>\n\n"
        f"▪️ <b>Global Footer:</b>\n<code>{custom_footer if custom_footer else '[Not Defined]'}</code>\n\n"
        f"📊 <b>Active Filters Matrix ({len(replacement_rules)} rules loaded):</b>\n"
    )
    
    if not replacement_rules:
        status_msg += "<code>[No active filter matrices currently loaded in database]</code>"
    else:
        for idx, (old, new) in enumerate(replacement_rules.items(), start=1):
            status_msg += f" {idx:02d} • <code>{old}</code> ⚡️ <code>{new if new else '[FLUSHED]'}</code>\n"
            
    await update.message.reply_text(status_msg, parse_mode="HTML")

# 🔥 2. PREMIUM MINIMALIST SAAS START MESSAGE (/start)
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_name = update.effective_user.first_name
    welcome_text = (
        f"⚡️ <b>Welcome back, {user_name}!</b>\n\n"
        f"🤖 <b>ENGINE:</b> <code>Auto Caption System v3.0 [PRO]</code>\n"
        f"📡 <b>STATUS:</b> <code>🟢 System Online & Active</code>\n\n"
        f"🛠 <b><u>SYSTEM CONTROL TERMINAL:</u></b>\n\n"
        f"• <code>/addrule [x -> y]</code> ➔ Register new text replacement filter\n"
        f"• <code>/delrule [word]</code> ➔ Wipe out specific filter target\n"
        f"• <code>/setheader [text]</code> ➔ Define dynamic upper block formatting\n"
        f"• <code>/setfooter [text]</code> ➔ Define permanent signature block attachment\n"
        f"• <code>/status</code> ➔ Open real-time core monitoring dashboard\n"
        f"• <code>/clear</code> ➔ Complete database initialization reset\n\n"
        f"ℹ️ <i>Configuration Hint: Simply appoint me as an administrator in your channel. Supersonic filtering engine is fully active by default.</i>"
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
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("addrule", add_rule))
    app.add_handler(CommandHandler("delrule", del_rule))
    app.add_handler(CommandHandler("setfooter", set_footer))
    app.add_handler(CommandHandler("setheader", set_header))
    app.add_handler(CommandHandler("clear", clear_rules))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(MessageHandler(filters.ChatType.CHANNEL, edit_channel_caption))
    print("🚀 Bot is polling cleanly...")
    app.run_polling()

if __name__ == '__main__':
    main()
