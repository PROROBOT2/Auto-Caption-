import os
import sys

# 🔥 DYNAMIC DEBUG LOGS (Render par root cause pakadne ke liye)
print("==================================================")
print("🚀 [DEBUG] Render ne bot.py file ko read aur run kar liya hai!")
print(f"📂 [DEBUG] Current Directory Files: {os.listdir('.')}")
print("==================================================")

import asyncio
import http.server
import socketserver
import threading
import re
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters
from pymongo import MongoClient

# 1. Render Dummy Server (Bot ko 24x7 online rakhne ke liye)
def start_dummy_server():
    PORT = int(os.environ.get("PORT", 10000))
    Handler = http.server.SimpleHTTPRequestHandler
    try:
        with socketserver.TCPServer(("", PORT), Handler) as httpd:
            httpd.serve_forever()
    except Exception as e:
        print(f"⚠️ [DUMMY SERVER WARN] {e}")

# 🗄️ MONGO DB CONNECTION SETUP
MONGO_URI = os.environ.get("MONGO_URI")
if not MONGO_URI:
    print("❌ Error: Render Config Vars me MONGO_URI nahi mila!")
    sys.exit(1)

print("📡 [DEBUG] MongoDB Connect karne ki koshish ho rahi hai...")
try:
    db_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    # Connection test karne ke liye command run kar rahe hain
    db_client.admin.command('ping')
    print("✅ [DEBUG] MongoDB successfully connect ho gaya!")
except Exception as mongo_err:
    print(f"❌ MongoDB Connection Error: {mongo_err}")
    sys.exit(1)

db = db_client["AutoCaptionBotDB"]
settings_col = db["bot_settings"]

# Database se settings load karne ka helper function
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
    except Exception as e:
        print(f"⚠️ Settings load error: {e}")
        return default_settings

# Database me dynamic changes save karne ka helper function
def update_bot_settings(field_name, field_value):
    try:
        settings_col.update_one(
            {"_id": "config"},
            {"$set": {field_name: field_value}},
            upsert=True
        )
    except Exception as e:
        print(f"⚠️ Settings update error: {e}")

raw_log_id = os.environ.get("LOG_CHANNEL_ID")
LOG_CHANNEL_ID = int(raw_log_id) if raw_log_id and raw_log_id.strip() else None

# 2. Premium Channel Editor & Auto-Cleaner Logic
async def edit_channel_caption(update: Update, context: ContextTypes.DEFAULT_TYPE):
    config = get_bot_settings()
    replacement_rules = config.get("replacement_rules", {})
    custom_header = config.get("custom_header", "")
    custom_footer = config.get("custom_footer", "")
    
    msg = update.channel_post or update.edited_channel_post
    if not msg:
        return

    text_to_check = msg.text or msg.caption
    if not text_to_check:
        return

    final_text = text_to_check

    # 🛑 1. AUTOMATIC SPAM REMOVER
    final_text = re.sub(r'(https?://)?t\.me/(?!DG_Contents|dghelps_bot)[a-zA-Z0-9_]+', '', final_text)
    final_text = re.sub(r'@(?!DG_Contents|dghelps_bot)[a-zA-Z0-9_]+', '', final_text)

    # 🔍 2. REPLACEMENT LOOP
    for old_txt, new_txt in replacement_rules.items():
        if re.search(old_txt, final_text, re.IGNORECASE):
            pattern = re.compile(old_txt, re.IGNORECASE)
            final_text = pattern.sub(new_txt, final_text)

    # Clean double spaces
    final_text = re.sub(r' +', ' ', final_text).strip()

    # 📝 3. HEADER & FOOTER ATTACHMENT
    header_part = f"<b>{custom_header}</b>\n\n" if custom_header else ""
    footer_part = f"\n\n<b>{custom_footer}</b>" if custom_footer else ""
    
    bold_text = f"{header_part}<b>{final_text}</b>{footer_part}"

    try:
        if msg.caption:
            if msg.caption_html == bold_text:
                return
            await context.bot.edit_message_caption(
                chat_id=msg.chat_id,
                message_id=msg.message_id,
                caption=bold_text,
                parse_mode="HTML"
            )
        elif msg.text:
            if msg.text_html == bold_text:
                return
            await context.bot.edit_message_text(
                chat_id=msg.chat_id,
                message_id=msg.message_id,
                text=bold_text,
                parse_mode="HTML"
            )
        print("Success: Caption Processed Cleanly!")

        # 🚀 LOG CHANNEL FORWARDING
        if LOG_CHANNEL_ID:
            try:
                await context.bot.copy_message(
                    chat_id=LOG_CHANNEL_ID,
                    from_chat_id=msg.chat_id,
                    message_id=msg.message_id
                )
                print("Success: Logged to backup channel!")
            except Exception as log_error:
                print(f"Log Channel Error: {log_error}")

    except Exception as e:
        print(f"Main Error Log: {e}")

# 3. Dynamic Admin Control Commands (Direct MongoDB Sync)
async def add_rule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    config = get_bot_settings()
    rules = config.get("replacement_rules", {})
    
    raw_args = " ".join(context.args)
    if " -> " not in raw_args:
        await update.message.reply_text("✨ <b>Sahi Format:</b>\n<code>/addrule purana_text -> naya_text</code>\n\n<i>Tip: Kisi word ko completely remove karne ke liye naye text ki jagah khaali chhod dein!</i>", parse_mode="HTML")
        return
    try:
        old_part, new_part = raw_args.split(" -> ", 1)
        rules[old_part.strip()] = new_part.strip()
        
        update_bot_settings("replacement_rules", rules)
        await update.message.reply_text("✅ <b>Success:</b> Replacement rule successfully active and saved to MongoDB!", parse_mode="HTML")
    except Exception:
        await update.message.reply_text("❌ <b>Error:</b> Format sahi nahi hai.")

async def del_rule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    config = get_bot_settings()
    rules = config.get("replacement_rules", {})
    
    old_text = " ".join(context.args).strip()
    if old_text in rules:
        del rules[old_text]
        
        update_bot_settings("replacement_rules", rules)
        await update.message.reply_text("🗑️ <b>Success:</b> Rule deleted successfully from MongoDB.", parse_mode="HTML")
    else:
        await update.message.reply_text("❌ <b>Error:</b> Yeh word list mein nahi mila.")

async def set_footer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    footer_text = " ".join(context.args).strip()
    update_bot_settings("custom_footer", footer_text)
    
    if not footer_text:
        await update.message.reply_text("🧹 <b>Footer Removed:</b> Ab posts ke niche koi footer nahi aayega.", parse_mode="HTML")
    else:
        await update.message.reply_text(f"📝 <b>Footer Set:</b>\n<code>{footer_text}</code>", parse_mode="HTML")

async def set_header(update: Update, context: ContextTypes.DEFAULT_TYPE):
    header_text = " ".join(context.args).strip()
    update_bot_settings("custom_header", header_text)
    
    if not header_text:
        await update.message.reply_text("🧹 <b>Header Removed:</b> Ab posts ke upar koi extra line nahi aayegi.", parse_mode="HTML")
    else:
        await update.message.reply_text(f"📝 <b>Header Set:</b>\n<code>{header_text}</code>", parse_mode="HTML")

async def clear_rules(update: Update, context: ContextTypes.DEFAULT_TYPE):
    update_bot_settings("replacement_rules", {})
    await update.message.reply_text("🧹 <b>Database Cleared:</b> Saare rules MongoDB se clear ho gaye!", parse_mode="HTML")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        status_msg += "<i>No active filters loaded.</i>"
    else:
        for old, new in replacement_rules.items():
            status_msg += f"🔍 <code>{old}</code> ➡️ <code>{new if new else '[REMOVED]'}</code>\n"
            
    await update.message.reply_text(status_msg, parse_mode="HTML")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_name = update.effective_user.first_name
    welcome_text = (
        f"⚡️ <b>Welcome, {user_name}!</b>\n\n"
        f"🚀 <b>Auto Caption Engine v3.0 [PRO + MongoDB]</b>\n"
        f"⚡ Status: <code>🟢 Online & Saved Permanent</code>\n\n"
        f"🛠️ <b>COMMANDS CENTER:</b>\n"
        f"• <code>/addrule</code> - Add text filter / replacement\n"
        f"• <code>/delrule</code> - Delete any active filter\n"
        f"• <code>/setheader</code> - Change top caption text\n"
        f"• <code>/setfooter</code> - Change bottom signature text\n"
        f"• <code>/status</code> - Open professional monitoring panel\n"
        f"• <code>/clear</code> - Reset all database configurations\n\n"
        f"ℹ️ <i>Just add me to your channel as admin, I will handle the rest with supersonic speed.</i>"
    )
    keyboard = [[InlineKeyboardButton("📢 Channel", url="https://t.me"),
