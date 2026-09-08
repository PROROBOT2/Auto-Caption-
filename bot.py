import asyncio
import os
import http.server
import socketserver
import threading
import re
from telegram import Update
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

# ⚠️ MULTIPLE TEXT STORAGE (Aapki list)
REPLACEMENT_RULES = {
    "MovieHub": "DG_Contents",
    "JoinUs": "SubscribeNow"
}

# 2. Heavy Duty Channel Editor Logic
async def edit_channel_caption(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global REPLACEMENT_RULES
    
    msg = update.channel_post or update.edited_channel_post
    if not msg:
        return

    text_to_check = msg.text or msg.caption
    if not text_to_check:
        return

    final_text = text_to_check

    # Saare rules ko ek-ek karke replace karein
    for old_txt, new_txt in REPLACEMENT_RULES.items():
        if re.search(old_txt, final_text, re.IGNORECASE):
            pattern = re.compile(old_txt, re.IGNORECASE)
            final_text = pattern.sub(new_txt, final_text)

    # Pure message ko BOLD format me convert karein
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
        print("Successfully Replaced and Bolded!")
    except Exception as e:
        print(f"Edit log/status: {e}")

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
    await update.message.reply_text("🧹 Saare rules clear ho gaye!")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global REPLACEMENT_RULES
    if not REPLACEMENT_RULES:
        await update.message.reply_text("📊 Kuch bhi set nahi hai, bot sirf bold karega.")
        return
    status_msg = "📊 <b>Active Rules:</b>\n\n"
    for old, new in REPLACEMENT_RULES.items():
        status_msg += f"🔍 <code>{old}</code> ➡️ <code>{new}</code>\n"
    await update.message.reply_text(status_msg, parse_mode="HTML")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Hyy Bhai! Bot active hai.\n/addrule\n/delrule\n/clear\n/status", parse_mode="HTML")

def main():
    # Yeh purane original code ka setup hai jo Render par live chal raha tha
    threading.Thread(target=start_dummy_server, daemon=True).start()
    
    TOKEN = os.environ.get("BOT_TOKEN")
    app = ApplicationBuilder().token(TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("addrule", add_rule))
    app.add_handler(CommandHandler("delrule", del_rule))
    app.add_handler(CommandHandler("clear", clear_rules))
    app.add_handler(CommandHandler("status", status))
    
    app.add_handler(MessageHandler(filters.ChatType.CHANNEL, edit_channel_caption))
    
    print("Bot is polling successfully...")
    app.run_polling()

if __name__ == '__main__':
    main()
