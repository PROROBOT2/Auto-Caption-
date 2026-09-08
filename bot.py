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

# ⚠️ YAHAN APNA TEXT SET KAREIN
OLD_TEXT = "TvShowHub"
NEW_TEXT = "DG_Contents"

# 2. Heavy Duty Channel Editor Logic
async def edit_channel_caption(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Channel post ko capture karein
    msg = update.channel_post or update.edited_channel_post
    if not msg:
        return

    # Check karein ki text message hai ya koi media caption hai
    text_to_check = msg.text or msg.caption
    if not text_to_check:
        return

    # Case-Insensitive checking ke liye re.search use karenge
    # Isse agar tvshowhub, TvShowHub ya TVSHOWHUB kuch bhi hoga, toh pakda jayega
    if re.search(OLD_TEXT, text_to_check, re.IGNORECASE):
        # Text ko replace karein (case-insensitive tareeqe se)
        pattern = re.compile(OLD_TEXT, re.IGNORECASE)
        new_text = pattern.sub(NEW_TEXT, text_to_check)

        try:
            if msg.caption:
                # Agar photo/video ka caption hai
                await context.bot.edit_message_caption(
                    chat_id=msg.chat_id,
                    message_id=msg.message_id,
                    caption=new_text
                )
            elif msg.text:
                # Agar sirf normal text message hai
                await context.bot.edit_message_text(
                    chat_id=msg.chat_id,
                    message_id=msg.message_id,
                    text=new_text
                )
            print("Successfully Edited!")
        except Exception as e:
            print(f"Edit failed: {e}")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Hyy Bhai! Bot ekdum active hai.")

def main():
    threading.Thread(target=start_dummy_server, daemon=True).start()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    TOKEN = os.environ.get("BOT_TOKEN")
    app = ApplicationBuilder().token(TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    
    # Is baar hum saare channel updates track kar rahe hain bina kisi filter ke lafde ke
    app.add_handler(MessageHandler(filters.ChatType.CHANNEL, edit_channel_caption))
    
    print("Bot is polling...")
    app.run_polling()

if __name__ == '__main__':
    main()
