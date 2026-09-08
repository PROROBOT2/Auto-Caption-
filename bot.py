import asyncio
import os
import http.server
import socketserver
import threading
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# 1. Render ko khush rakhne ke liye chota sa web server
def start_dummy_server():
    PORT = int(os.environ.get("PORT", 10000))
    Handler = http.server.SimpleHTTPRequestHandler
    try:
        with socketserver.TCPServer(("", PORT), Handler) as httpd:
            print(f"Dummy server running on port {PORT}")
            httpd.serve_forever()
    except Exception as e:
        print(f"Server error: {e}")

# 2. Bot ka Start Command (Aapka main bot logic)
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Hyy Bhai! Auto Caption Editor Bot active hai. Kaise madad karu?")

def main():
    # Background me server chalu karein
    threading.Thread(target=start_dummy_server, daemon=True).start()

    # Asyncio Loop error ko theek karne ke liye
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # Render se automatic token uthane ke liye
    TOKEN = os.environ.get("BOT_TOKEN")
    
    # Bot Application build karein
    app = ApplicationBuilder().token(TOKEN).build()
    
    # Start command handler jodna
    app.add_handler(CommandHandler("start", start))
    
    # Bot shuru karein
    print("Bot is polling...")
    app.run_polling()

if __name__ == '__main__':
    main()
