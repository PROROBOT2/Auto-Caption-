import os
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes

# The bot will safely read your token from Render's settings later
TOKEN = os.environ.get("BOT_TOKEN")

async def replace_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message and update.message.text:
        original_text = update.message.text
        
        # EDIT THIS PART: Put the words you want to find and replace here!
        # Syntax: original_text.replace("OLD WORD", "NEW WORD")
        modified_text = original_text.replace("Apple", "Orange")
        
        # If the text changed, the bot sends the new version back
        if original_text != modified_text:
            await update.message.reply_text(f"Edited: {modified_text}")

def main():
    if not TOKEN:
        print("Error: BOT_TOKEN environment variable not set!")
        return
        
    app = Application.builder().token(TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, replace_text))
    print("Bot is starting up...")
    app.run_polling()

if __name__ == '__main__':
    main()
