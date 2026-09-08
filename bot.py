import os
import asyncio
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes

TOKEN = os.environ.get("BOT_TOKEN")

async def replace_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message and update.message.text:
        original_text = update.message.text
        
        # Yahan aap apna badalne wala shabd badal sakte ho
        modified_text = original_text.replace("Apple", "Orange")
        
        if original_text != modified_text:
            await update.message.reply_text(f"Edited: {modified_text}")

def main():
    if not TOKEN:
        print("Error: BOT_TOKEN environment variable not set!")
        return
        
    # Python 3.14 ke event loop error ko theek karne ke liye:
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
    app = Application.builder().token(TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, replace_text))
    print("Bot is starting up...")
    app.run_polling()

if __name__ == '__main__':
    main()
