import asyncio
import os
import http.server
import socketserver
import threading
import re
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters

# 1. Render Dummy Server
def start_dummy_server():
    PORT = int(os.environ.get("PORT", 10000))
    Handler = http.server.SimpleHTTPRequestHandler
    try:
        with socketserver.TCPServer(("", PORT), Handler) as httpd:
            httpd.serve_forever()
    except Exception:
        pass

# ⚠️ GLOBAL CONFIGURATIONS
REPLACEMENT_RULES = {
    "MovieHub": "DG_Contents",
    "JoinUs": "SubscribeNow"
}

# Dynamic Header/Footer Storage (In-Memory)
CUSTOM_HEADER = ""
CUSTOM_FOOTER = "⚡️ Fast Download Links @DG_Contents"

raw_log_id = os.environ.get("LOG_CHANNEL_ID")
LOG_CHANNEL_ID = int(raw_log_id) if raw_log_id and raw_log_id.strip() else None

# 2. Premium Channel Editor & Auto-Cleaner Logic
async def edit_channel_caption(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global REPLACEMENT_RULES, CUSTOM_HEADER, CUSTOM_FOOTER
    
    msg = update.channel_post or update.edited_channel_post
    if not msg:
        return

    text_to_check = msg.text or msg.caption
    if not text_to_check:
        return

    final_text = text_to_check

    # 🛑 1. AUTOMATIC SPAM REMOVER (Removes external links and random @usernames)
    # Yeh aapke khud ke channel ko chhodkar baki sabhi links aur usernames ko clean kar dega
    final_text = re.sub(r'(https?://)?t\.me/(?!DG_Contents|dghelps_bot)[a-zA-Z0-0_]+', '', final_text)
    final_text = re.sub(r'@(?!DG_Contents|dghelps_bot)[a-zA-Z0-9_]+', '', final_text)

    # 🔍 2. REPLACEMENT LOOP
    for old_txt, new_txt in REPLACEMENT_RULES.items():
        if re.search(old_txt, final_text, re.IGNORECASE):
            pattern = re.compile(old_txt, re.IGNORECASE)
            final_text = pattern.sub(new_txt, final_text)

    # Clean double spaces caused by deletion
    final_text = re.sub(r' +', ' ', final_text).strip()

    # 📝 3. HEADER & FOOTER ATTACHMENT
    header_part = f"<b>{CUSTOM_HEADER}</b>\n\n" if CUSTOM_HEADER else ""
    footer_part = f"\n\n<b>{CUSTOM_FOOTER}</b>" if CUSTOM_FOOTER else ""
    
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

# 3. Dynamic Admin Control Commands
async def add_rule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global REPLACEMENT_RULES
    raw_args = " ".join(context.args)
    if " -> " not in raw_args:
        await update.message.reply_text("✨ <b>Sahi Format:</b>\n<code>/addrule purana_text -> naya_text</code>\n\n<i>Tip: Kisi word ko completely remove karne ke liye naye text ki jagah khaali chhod dein!</i>", parse_mode="HTML")
        return
    try:
        old_part, new_part = raw_args.split(" -> ", 1)
        REPLACEMENT_RULES[old_part.strip()] = new_part.strip()
        await update.message.reply_text("✅ <b>Success:</b> Replacement rule successfully active!", parse_mode="HTML")
    except Exception:
        await update.message.reply_text("❌ <b>Error:</b> Format sahi nahi hai.")

async def del_rule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global REPLACEMENT_RULES
    old_text = " ".join(context.args).strip()
    if old_text in REPLACEMENT_RULES:
        del REPLACEMENT_RULES[old_text]
        await update.message.reply_text("🗑️ <b>Success:</b> Rule deleted successfully.", parse_mode="HTML")
    else:
        await update.message.reply_text("❌ <b>Error:</b> Yeh word list mein nahi mila.")

async def set_footer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global CUSTOM_FOOTER
    footer_text = " ".join(context.args).strip()
    if not footer_text:
        CUSTOM_FOOTER = ""
        await update.message.reply_text("🧹 <b>Footer Removed:</b> Ab posts ke niche koi footer nahi aayega.", parse_mode="HTML")
    else:
        CUSTOM_FOOTER = footer_text
        await update.message.reply_text(f"📝 <b>Footer Set:</b>\n<code>{CUSTOM_FOOTER}</code>", parse_mode="HTML")

async def set_header(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global CUSTOM_HEADER
    header_text = " ".join(context.args).strip()
    if not header_text:
        CUSTOM_HEADER = ""
        await update.message.reply_text("🧹 <b>Header Removed:</b> Ab posts ke upar koi extra line nahi aayegi.", parse_mode="HTML")
    else:
        CUSTOM_HEADER = header_text
        await update.message.reply_text(f"📝 <b>Header Set:</b>\n<code>{CUSTOM_HEADER}</code>", parse_mode="HTML")

async def clear_rules(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global REPLACEMENT_RULES
    REPLACEMENT_RULES.clear()
    await update.message.reply_text("🧹 <b>Database Cleared:</b> Saare rules clear ho gaye!", parse_mode="HTML")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global REPLACEMENT_RULES, CUSTOM_HEADER, CUSTOM_FOOTER
    status_msg = "⚙️ <b>𝖯𝖱𝖮 𝖡𝖮𝖳 𝖣𝖠𝖲𝖧𝖡𝖮𝖠𝖱𝖣</b>\n\n"
    status_msg += f"📢 <b>Log Status:</b> {'🟢 Connected' if LOG_CHANNEL_ID else '🔴 Disconnected'}\n"
    status_msg += f"🔝 <b>Active Header:</b> <code>{CUSTOM_HEADER if CUSTOM_HEADER else 'None'}</code>\n"
    status_msg += f"🔚 <b>Active Footer:</b> <code>{CUSTOM_FOOTER if CUSTOM_FOOTER else 'None'}</code>\n\n"
    status_msg += "📊 <b>Word Replacement Rules:</b>\n"
    
    if not REPLACEMENT_RULES:
        status_msg += "<i>No active filters loaded.</i>"
    else:
        for old, new in REPLACEMENT_RULES.items():
            status_msg += f"🔍 <code>{old}</code> ➡️ <code>{new if new else '[REMOVED]'}</code>\n"
            
    await update.message.reply_text(status_msg, parse_mode="HTML")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_name = update.effective_user.first_name
    welcome_text = (
        f"⚡️ <b>𝖶𝖾𝗅𝖼𝗈𝗆𝖾, {user_name}!</b>\n\n"
        f"🚀 <b>Auto Caption Engine v3.0 [PRO]</b>\n"
        f"⚡ 𝖲𝗍𝖺𝗍𝗎𝗌: <code>🟢 𝖮𝗇𝗅𝗂𝗇𝖾 & 𝖯𝗈𝗅𝗅𝗂𝗇𝗀</code>\n\n"
        f"🛠️ <b>𝖢𝗈𝗆𝗆𝖺𝗇𝖽𝗌 𝖢𝖾𝗇𝗍𝖾𝗋:</b>\n"
        f"• <code>/addrule</code> - Add text filter / replacement\n"
        f"• <code>/delrule</code> - Delete any active filter\n"
        f"• <code>/setheader</code> - Change top caption text\n"
        f"• <code>/setfooter</code> - Change bottom signature text\n"
        f"• <code>/status</code> - Open professional monitoring panel\n"
        f"• <code>/clear</code> - Reset all database configurations\n\n"
        f"ℹ️ <i>Just add me to your channel as admin, I will handle the rest with supersonic speed.</i>"
    )
    keyboard = [[InlineKeyboardButton("📢 Channel", url="https://t.me/dg_contents"),
                 InlineKeyboardButton("👥 Support", url="https://t.me/DGHelps_bot")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(text=welcome_text, parse_mode='HTML', reply_markup=reply_markup)

def main():
    threading.Thread(target=start_dummy_server, daemon=True).start()
    try:
        loop = asyncio.get_event_loop()
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
    
    print("Bot is polling cleanly...")
    app.run_polling()

if __name__ == '__main__':
    main()
