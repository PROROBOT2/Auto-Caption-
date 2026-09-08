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

# ⚠️ DYNAMIC TEXT STORAGE (Default values)
# Jab tak aap Telegram par change nahi karoge, ye values kaam karengi
OLD_TEXT = "TvShowHub"
NEW_TEXT = "DG_Contents"

# 2. Heavy Duty Channel Editor Logic (With Auto-Bold)
async def edit_channel_caption(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global OLD_TEXT, NEW_TEXT
    
    msg = update.channel_post or update.edited_channel_post
    if not msg:
        return

    # Check karein ki text message hai ya koi media caption hai
    # Telegram markdown/entities ko hatakar plain text nikalne ke liye update.message ke text/caption formats use hote hain
    text_to_check = msg.text or msg.caption
    if not text_to_check:
        return

    # 1. Pehle text replace karein (Case-Insensitive)
    if re.search(OLD_TEXT, text_to_check, re.IGNORECASE):
        pattern = re.compile(OLD_TEXT, re.IGNORECASE)
        final_text = pattern.sub(NEW_TEXT, text_to_check)
    else:
        final_text = text_to_check

    # 2. pure message ko BOLD format me convert karein (HTML tag ke sath)
    bold_text = f"<b>{final_text}</b>"

    try:
        if msg.caption:
            # Agar photo/video ka caption hai
            await context.bot.edit_message_caption(
                chat_id=msg.chat_id,
                message_id=msg.message_id,
                caption=bold_text,
                parse_mode="HTML" # HTML parse mode bold karne ke liye zaroori hai
            )
        elif msg.text:
            # Agar sirf normal text message hai
            await context.bot.edit_message_text(
                chat_id=msg.chat_id,
                message_id=msg.message_id,
                text=bold_text,
                parse_mode="HTML"
            )
        print("Successfully Replaced and Bolded!")
    except Exception as e:
        print(f"Edit failed: {e}")

# 3. Dynamic Commands (Sirf Aapke use ke liye)
async def set_old(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global OLD_TEXT
    if not context.args:
        await update.message.reply_text("❌ Sahi tarika: /setold [purana_text]\nExample: /setold TvShowHub")
        return
    OLD_TEXT = " ".join(context.args)
    await update.message.reply_text(f"✅ Ab se bot channel me <b>{OLD_TEXT}</b> ko dhoondhega.", parse_mode="HTML")

async def set_new(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global NEW_TEXT
    if not context.args:
        await update.message.reply_text("❌ Sahi tarika: /setnew [naya_text]\nExample: /setnew DG_Contents")
        return
    NEW_TEXT = " ".join(context.args)
    await update.message.reply_text(f"✅ Ab se bot use badal kar <b>{NEW_TEXT}</b> kar dega.", parse_mode="HTML")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global OLD_TEXT, NEW_TEXT
    await update.message.reply_text(
        f"📊 <b>Current Settings:</b>\n\n🔍 Search for: <code>{OLD_TEXT}</code>\n✏️ Replace with: <code>{NEW_TEXT}</code>", 
        parse_mode="HTML"
    )

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Hyy Bhai! Bot active hai.\n\nCommands:\n/setold - Purana text set karein\n/setnew - Naya text set karein\n/status - Current settings check karein")

def main():
    threading.Thread(target=start_dummy_server, daemon=True).start()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    TOKEN = os.environ.get("BOT_TOKEN")
    app = ApplicationBuilder().token(TOKEN).build()
    
    # Handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("setold", set_old))
    app.add_handler(CommandHandler("setnew", set_new))
    app.add_handler(CommandHandler("status", status))
    
    # Channel message handler
    app.add_handler(MessageHandler(filters.ChatType.CHANNEL, edit_channel_caption))
    
    print("Bot is polling with Advanced features...")
    app.run_polling()

if __name__ == '__main__':
    main()
