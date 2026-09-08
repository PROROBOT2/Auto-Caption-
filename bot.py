import asyncio
import os
import http.server
import socketserver
import threading
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters

# 1. Render ko Live rakhne ke liye Server
def start_dummy_server():
    PORT = int(os.environ.get("PORT", 10000))
    Handler = http.server.SimpleHTTPRequestHandler
    try:
        with socketserver.TCPServer(("", PORT), Handler) as httpd:
            httpd.serve_forever()
    except Exception:
        pass

# ⚠️ APNA TEXT YAHAN BADLO (Ye sirf ek example hai)
OLD_TEXT = "TvShowHub"       # Jo text ya link hatana hai
NEW_TEXT = "DG_Contents"       # Jo naya text ya link lagana hai

# 2. Channel Post ko Edit karne wala Logic
async def edit_channel_caption(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Channel ki post uthayein (Chahe video ho, photo ho ya document)
    channel_post = update.channel_post
    
    # Agar post me pehle se koi caption (text) likha hai
    if channel_post and channel_post.caption:
        current_caption = channel_post.caption
        
        # Agar purana text caption me maujood hai, toh use badlo
        if OLD_TEXT in current_caption:
            new_caption = current_caption.replace(OLD_TEXT, NEW_TEXT)
            
            try:
                # Channel me caption ko automatic edit kar do
                await context.bot.edit_message_caption(
                    chat_id=channel_post.chat_id,
                    message_id=channel_post.message_id,
                    caption=new_caption
                )
                print("Caption successfully updated in channel!")
            except Exception as e:
                print(f"Error editing caption: {e}")

# Bot ka Start Command
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Hyy Bhai! Auto Caption Editor Bot active hai. Mujhe channel me Admin banayein.")

def main():
    threading.Thread(target=start_dummy_server, daemon=True).start()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    TOKEN = os.environ.get("BOT_TOKEN")
    app = ApplicationBuilder().token(TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    
    # Yeh line channel ki posts ko track karegi (Sirf text/caption wali posts)
    app.add_handler(MessageHandler(filters.ChatType.CHANNEL & filters.CAPTION, edit_channel_caption))
    
    print("Bot is polling...")
    app.run_polling()

if __name__ == '__main__':
    main()
