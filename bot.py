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
user_settings_col = db["user_configs"]

try:
    OWNER_ID = int(os.environ.get("OWNER_ID", 0))
except ValueError:
    OWNER_ID = 0

# Har user/channel ke liye database se dynamic configs pull karne ka helper
def get_user_settings(user_id):
    config_id = str(user_id)
    default_settings = {
        "_id": config_id,
        "replacement_rules": {"MovieHub": "DG_Contents"},
        "custom_header": "",
        "custom_footer": "⚡ Fast Download Links"
    }
    try:
        config = user_settings_col.find_one({"_id": config_id})
        if not config:
            user_settings_col.insert_one(default_settings)
            return default_settings
        return config
    except Exception:
        return default_settings

def update_user_settings(user_id, field_name, field_value):
    config_id = str(user_id)
    try:
        user_settings_col.update_one(
            {"_id": config_id},
            {"$set": {field_name: field_value}},
            upsert=True
        )
    except Exception:
        pass

raw_log_id = os.environ.get("LOG_CHANNEL_ID")
LOG_CHANNEL_ID = int(raw_log_id) if raw_log_id and raw_log_id.strip() else None

# 2. Premium Auto-Cleaner & Caption Editor Logic
async def edit_channel_caption(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.channel_post or update.edited_channel_post
    if not msg: return

    # Channel post ke liye channel ki settings nikalenge database se
    config = get_user_settings(msg.chat_id)
    replacement_rules = config.get("replacement_rules", {})
    custom_header = config.get("custom_header", "")
    custom_footer = config.get("custom_footer", "")

    text_to_check = msg.text or msg.caption
    if not text_to_check: return

    final_text = text_to_check
    
    # 🛑 SPAM REMOVER
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

# 3. Dynamic User Commands Setup (Apna apna space)
async def add_rule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    raw_args = " ".join(context.args)
    if " -> " not in raw_args:
        await update.message.reply_text("✨ <b>Sahi Format:</b>\n<code>/addrule purana_text -> naya_text</code>", parse_mode="HTML")
        return
    try:
        old_part, new_part = raw_args.split(" -> ", 1)
        target_id = update.message.chat_id if update.message.chat.type != "private" else user_id
        
        config = get_user_settings(target_id)
        rules = config.get("replacement_rules", {})
        
        rules[old_part.strip()] = new_part.strip()
        update_user_settings(target_id, "replacement_rules", rules)
        await update.message.reply_text("✅ <b>Success:</b> Aapka filter rule successfully save ho gaya hai!", parse_mode="HTML")
    except Exception: pass

async def del_rule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    target_id = update.message.chat_id if update.message.chat.type != "private" else user_id
    
    config = get_user_settings(target_id)
    rules = config.get("replacement_rules", {})
    old_text = " ".join(context.args).strip()
    
    if old_text in rules:
        del rules[old_text]
        update_user_settings(target_id, "replacement_rules", rules)
        await update.message.reply_text("🗑️ <b>Success:</b> Rule aapki list se hataya gaya.", parse_mode="HTML")
    else:
        await update.message.reply_text("❌ <b>Error:</b> Yeh rule aapki active list mein nahi mila.", parse_mode="HTML")

async def set_footer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    target_id = update.message.chat_id if update.message.chat.type != "private" else user_id
    footer_text = " ".join(context.args).strip()
    
    update_user_settings(target_id, "custom_footer", footer_text)
    await update.message.reply_text(f"📝 <b>Aapka Custom Footer Set:</b>\n<code>{footer_text}</code>", parse_mode="HTML")

async def set_header(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    target_id = update.message.chat_id if update.message.chat.type != "private" else user_id
    header_text = " ".join(context.args).strip()
    
    update_user_settings(target_id, "custom_header", header_text)
    await update.message.reply_text(f"📝 <b>Aapka Custom Header Set:</b>\n<code>{header_text}</code>", parse_mode="HTML")

async def clear_rules(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    target_id = update.message.chat_id if update.message.chat.type != "private" else user_id
    update_user_settings(target_id, "replacement_rules", {})
    await update.message.reply_text("🧹 <b>Database Reset:</b> Aapke saare rules clear ho gaye hain.", parse_mode="HTML")

# DYNAMIC MONITORING DASHBOARD
async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    target_id = update.message.chat_id if update.message.chat.type != "private" else user_id
    
    config = get_user_settings(target_id)
    replacement_rules = config.get("replacement_rules", {})
    custom_header = config.get("custom_header", "")
    custom_footer = config.get("custom_footer", "")
    
    if user_id == OWNER_ID:
        status_msg = "⚙️ <b>𝖯𝖱𝖮 𝖡𝖮𝖲𝖲 𝖣𝖠𝖲𝖧𝖡𝖮𝖠𝖱𝖣 (👑 Global Master Mode)</b>\n\n"
    else:
        status_msg = "⚙️ <b>𝖴𝖲𝖤𝖱 𝖢𝖮𝖭𝖥𝖨𝖦𝖴𝖱A𝖳𝖨𝖮𝖭 𝖯𝖠𝖭𝖤𝖫 (☁️ Cloud Sync)</b>\n\n"
        
    status_msg += f"📡 <b>Database:</b> <code>🟢 Connected (Your Space)</code>\n"
    status_msg += f"🔝 <b>Your Header:</b> <code>{custom_header if custom_header else 'None'}</code>\n"
    status_msg += f"🔚 <b>Your Footer:</b> <code>{custom_footer if custom_footer else 'None'}</code>\n\n"
    status_msg += "📊 <b>Your Personal Filter Rules:</b>\n"
    
    if not replacement_rules:
        status_msg += "<i>Aapne abhi tak koi filter save nahi kiya hai.</i>"
    else:
        for idx, (old, new) in enumerate(replacement_rules.items(), start=1):
            status_msg += f" {idx:02d} • <code>{old}</code> ➔ <code>{new if new else '[REMOVED]'}</code>\n"
            
    await update.message.reply_text(status_msg, parse_mode="HTML")

# UNIVERSAL WELCOME MESSAGE
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_name = update.effective_user.first_name
    welcome_text = (
        f"⚡️ <b>Welcome, {user_name}!</b>\n\n"
        f"🚀 <b>Auto Caption Engine v3.0 [PRO Multi-User]</b>\n"
        f"⚡️ Status: <code>🟢 Online & Active In Your Space</code>\n\n"
        f"🛠 <b>Commands Center (Aapka Personal Panel):</b>\n"
        f"• <code>/addrule</code> - Add your text filter / replacement\n"
        f"• <code>/delrule</code> - Delete your active filter\n"
        f"• <code>/setheader</code> - Set your top caption text\n"
        f"• <code>/setfooter</code> - Set your bottom signature text\n"
        f"• <code>/status</code> - Open your cloud diagnostics panel\n"
        f"• <code>/clear</code> - Reset your configurations\n\n"
        f"ℹ️ <b>How to use:</b>\n"
        f"1. Bot ko apne channel me **Admin** banayein.\n"
        f"2. Agar direct channel ki setting karni hai, toh channel ke andar hi ye commands send karein!"
    )
    keyboard = [[
        InlineKeyboardButton("📢 Main Channel", url="https://t.me/dg_contents"), 
        InlineKeyboardButton("👥 Developer Support", url="https://t.me/dghelps_bot")
    ]]
    await update.message.reply_text(text=welcome_text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))

def main():
    threading.Thread(target=start_dummy_server, daemon=True).start()
    try: loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
