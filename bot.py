import asyncio
import os
import http.server
import socketserver
import threading
import re
import html
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters

# 1. Render Dummy Server (Port binding active rakhne ke liye)
def start_dummy_server():
    PORT = int(os.environ.get("PORT", 10000))
    Handler = http.server.SimpleHTTPRequestHandler
    try:
        with socketserver.TCPServer(("", PORT), Handler) as httpd:
            httpd.serve_forever()
    except Exception:
        pass

# ⚠️ STATIC IN-MEMORY STORAGE (Bina Database Ke Setup)
# Bot restart hone par ye wapas initial values par reset ho jayega
OLD_TEXTS_LIST = ["Old_Channel_Link"]
NEW_TEXT = "DG_Contents"

# 2. Channel Editor Logic (Multiple Text + Loop Protection)
async def edit_channel_caption(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global OLD_TEXTS_LIST, NEW_TEXT
    
    msg = update.channel_post or update.edited_channel_post
    if not msg:
        return

    text_to_check = msg.text or msg.caption
    if not text_to_check:
        return

    # 🛑 LOOP PROTECTION: Agar message pehle se hi bold format HTML tags me hai, toh skip karein
    if text_to_check.startswith("<b>") and text_to_check.endswith("</b>"):
        return

    has_match = False
    final_text = text_to_check

    # List ke har word ko check aur replace karein (Case-Insensitive)
    for old_txt in OLD_TEXTS_LIST:
        if re.search(re.escape(old_txt), final_text, re.IGNORECASE):
            pattern = re.compile(re.escape(old_txt), re.IGNORECASE)
            final_text = pattern.sub(NEW_TEXT, final_text)
            has_match = True

    # Agar text me koi badlav nahi hua aur message pehle se edited notification hai, toh skip karein
    if not has_match and update.edited_channel_post:
        return

    # HTML special characters escape karein taaki tags break na hon
    safe_text = html.escape(final_text)
    bold_text = f"<b>{safe_text}</b>"

    try:
        if msg.caption:
            await context.bot.edit_message_caption(
                chat_id=msg.chat_id, 
                message_id=msg.message_id, 
                caption=bold_text, 
                parse_mode="HTML"
            )
        elif msg.text:
            await context.bot.edit_message_text(
                chat_id=msg.chat_id, 
                message_id=msg.message_id, 
                text=bold_text, 
                parse_mode="HTML"
            )
        print("Successfully Replaced Multiple Words and Bolded!")
    except Exception as e:
        print(f"Edit failed (Check Admin Rights): {e}")

# 3. Dynamic Commands Setup
async def add_old(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global OLD_TEXTS_LIST
    if not context.args:
        await update.message.reply_text("❌ Sahi tarika: /addold [word]\nExample: /addold Old_Channel_Link")
        return
    new_word = " ".join(context.args)
    
    if new_word in OLD_TEXTS_LIST:
        await update.message.reply_text("ℹ️ Yeh word pehle se hi list me hai.")
        return
        
    OLD_TEXTS_LIST.append(new_word)
    await update.message.reply_text(f"✅ <b>{html.escape(new_word)}</b> ko list me jod diya gaya hai.", parse_mode="HTML")

async def del_old(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global OLD_TEXTS_LIST
    if not context.args:
        await update.message.reply_text("❌ Sahi tarika: /delold [word]\nExample: /delold Old_Channel_Link")
        return
    word_to_del = " ".join(context.args)
    
    if word_to_del not in OLD_TEXTS_LIST:
        await update.message.reply_text("❌ Yeh word list me nahi mila.")
        return
        
    OLD_TEXTS_LIST.remove(word_to_del)
    await update.message.reply_text(f"🗑️ <b>{html.escape(word_to_del)}</b> ko list se hata diya gaya hai.", parse_mode="HTML")

async def set_new(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global NEW_TEXT
    if not context.args:
        await update.message.reply_text("❌ Sahi tarika: /setnew [text]\nExample: /setnew YourBrandName")
        return
    NEW_TEXT = " ".join(context.args)
    await update.message.reply_text(f"✅ Ab se bot sabhi purane words ko badal kar <b>{html.escape(NEW_TEXT)}</b> kar dega.", parse_mode="HTML")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global OLD_TEXTS_LIST, NEW_TEXT
    
    words_str = "\n".join([f"- <code>{html.escape(w)}</code>" for w in OLD_TEXTS_LIST]) if OLD_TEXTS_LIST else "<i>List Khali Hai</i>"
    
    await update.message.reply_text(
        f"📊 <b>Current Settings:</b>\n\n"
        f"🔍 <b>Search for (Multiple Words):</b>\n{words_str}\n\n"
        f"✏️ <b>Replace with:</b> <code>{html.escape(NEW_TEXT)}</code>", 
        parse_mode="HTML"
    )

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    unique_welcome = (
        "⚡ <b>Auto Caption Editor Engine v2.0</b> ⚡\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "Hey Boss! Main active hu aur aapke channels ka branding control sambhalne ke liye bilkul taiyar hu.\n\n"
        "⚙️ <b>Control Panel Commands:</b>\n"
        "• /addold [word] ➔ Naya purana text/link list me jodein\n"
        "• /delold [word] ➔ List se koi word hatayein\n"
        "• /setnew [word] ➔ Apni nayi brand identity set karein\n"
        "• /status        ➔ Pure configurations check karein\n\n"
        "📢 <i>Note: Mujhe channel me admin banakar 'Edit Messages' ki permission dena mat bhoolna!</i>"
    )
    await update.message.reply_text(unique_welcome, parse_mode="HTML")

def main():
    threading.Thread(target=start_dummy_server, daemon=True).start()
    TOKEN = os.environ.get("BOT_TOKEN")
    app = ApplicationBuilder().token(TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("addold", add_old))
    app.add_handler(CommandHandler("delold", del_old))
    app.add_handler(CommandHandler("setnew", set_new))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(MessageHandler(filters.ChatType.CHANNEL, edit_channel_caption))
    
    print("Bot is running perfectly without database...")
    app.run_polling()

if __name__ == '__main__':
    main()
