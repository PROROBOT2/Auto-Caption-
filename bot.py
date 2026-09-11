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

# ⚠️ MULTIPLE TEXT STORAGE
REPLACEMENT_RULES = {
    "MovieHub": "DG_Contents",
    "JoinUs": "SubscribeNow"
}

# 📢 LOG CHANNEL ID (Render variable ko text se integer number mein convert karne ke liye fixes)
raw_log_id = os.environ.get("LOG_CHANNEL_ID")
LOG_CHANNEL_ID = int(raw_log_id) if raw_log_id and raw_log_id.strip() else None

# 2. Heavy Duty Channel Editor & Logger Logic
async def edit_channel_caption(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global REPLACEMENT_RULES
    
    msg = update.channel_post or update.edited_channel_post
    if not msg:
        return

    text_to_check = msg.text or msg.caption
    if not text_to_check:
        return

    final_text = text_to_check

    # Replacement loop
    for old_txt, new_txt in REPLACEMENT_RULES.items():
        if re.search(old_txt, final_text, re.IGNORECASE):
            pattern = re.compile(old_txt, re.IGNORECASE)
            final_text = pattern.sub(new_txt, final_text)

    bold_text = f"<b>{final_text}</b>"

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
        print("Success: Caption Edited!")

        # 🚀 LOG CHANNEL FORWARDING LOGIC
        if LOG_CHANNEL_ID:
            try:
                # Yeh badle hue message ko aapke log channel mein copy kar dega
                await context.bot.copy_message(
                    chat_id=LOG_CHANNEL_ID,
                    from_chat_id=msg.chat_id,
                    message_id=msg.message_id
                )
                print("Success: Message logged to channel!")
            except Exception as log_error:
                print(f"Log Channel Error: {log_error}")

    except Exception as e:
        print(f"Main Error Log: {e}")

# 3. Dynamic Commands
async def add_rule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global REPLACEMENT_RULES
    raw_args = " ".join(context.args)
    if " -> " not in raw_args:
        await update.message.reply_text("❌ Sahi Tarika: /addrule purana -> naya", parse_mode="HTML")
        return
    try:
        old_part, new_part = raw_args.split(" -> ", 1)
        REPLACEMENT_RULES[old_part.strip()] = new_part.strip()
        await update.message.reply_text("✅ Rule Added!", parse_mode="HTML")
    except Exception:
        await update.message.reply_text("❌ Format galat hai.")

async def del_rule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global REPLACEMENT_RULES
    old_text = " ".join(context.args).strip()
    if old_text in REPLACEMENT_RULES:
        del REPLACEMENT_RULES[old_text]
        await update.message.reply_text("🗑️ Rule Deleted.", parse_mode="HTML")
    else:
        await update.message.reply_text("❌ Word nahi mila.")

async def clear_rules(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global REPLACEMENT_RULES
    REPLACEMENT_RULES.clear()
    await update.message.reply_text("🧹 Cleared!")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global REPLACEMENT_RULES
    if not REPLACEMENT_RULES:
        await update.message.reply_text("📊 No Active Rules.", parse_mode="HTML")
        return
    status_msg = "📊 <b>Active Rules:</b>\n\n"
    for old, new in REPLACEMENT_RULES.items():
        status_msg += f"🔍 <code>{old}</code> ➡️ <code>{new}</code>\n"
    await update.message.reply_text(status_msg, parse_mode="HTML")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_name = update.effective_user.first_name
    welcome_text = (
        f"👋 <b>Welcome, {user_name}!</b>\n\n"
        f"🤖 <b>Auto Caption Bot v2.0</b> mein aapka swagat hai.\n\n"
        f"<b>Commands List:</b>\n"
        f"• <code>/addrule</code> - Naya replacement rule jodne ke liye\n"
        f"• <code>/delrule</code> - Purana rule hatane ke liye\n"
        f"• <code>/clear</code> - Saare rules clear karne ke liye\n"
        f"• <code>/status</code> - Active rules dekhne ke liye\n\n"
        f"📖 <b>How to use?</b>\n"
        f"Bas channel mein video ya file dalo, baki ka kaam main khud kar dunga"
    )
    keyboard = [[InlineKeyboardButton("📢 Channel", url="https://t.me"),
                 InlineKeyboardButton("👥 Support", url="https://t.me")]]
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
    app.add_handler(CommandHandler("clear", clear_rules))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(MessageHandler(filters.ChatType.CHANNEL, edit_channel_caption))
    
    print("Bot is polling cleanly...")
    app.run_polling()

if __name__ == '__main__':
    main()
