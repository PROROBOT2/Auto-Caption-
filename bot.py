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

# ⚠️ DYNAMIC MULTIPLE TEXT STORAGE (Default values)
# Format: {"purana_word": "naya_word"}
REPLACEMENT_RULES = {
    "OldBrandName": "NewBrandName",
    "MovieHub": "DG_Contents",
    "JoinUs": "SubscribeNow"
}

# Default text jo tab use hoga agar text me koi matching word na mile par bold karna ho
DEFAULT_NEW_TEXT = "DG_Contents"

# 2. Heavy Duty Channel Editor Logic (With Auto-Bold & Multiple Word Replacement)
async def edit_channel_caption(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global REPLACEMENT_RULES
    
    msg = update.channel_post or update.edited_channel_post
    if not msg:
        return

    text_to_check = msg.text or msg.caption
    if not text_to_check:
        return

    # 🛑 Loop Protection: Agar message pehle se hi BOLD hai toh skip karein
    if text_to_check.startswith("<b>") and text_to_check.endswith("</b>"):
        return

    final_text = text_to_check
    is_replaced = False

    # 1. Loop chala kar saare rules check karein aur replace karein (Case-Insensitive)
    for old_txt, new_txt in REPLACEMENT_RULES.items():
        if re.search(old_txt, final_text, re.IGNORECASE):
            pattern = re.compile(old_txt, re.IGNORECASE)
            final_text = pattern.sub(new_txt, final_text)
            is_replaced = True

    # 2. Pure message ko BOLD format me convert karein
    bold_text = f"<b>{final_text}</b>"

    # Agar koi text change nahi hua aur na hi bold lagane ki zaroorat hai, toh edit mat karo
    if bold_text == text_to_check:
        return

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
        print(f"Edit failed: {e}")

# 3. Dynamic Commands for Multiple Texts
async def add_rule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global REPLACEMENT_RULES
    # Expected format: /addrule purana -> naya
    raw_args = " ".join(context.args)
    if "->" not in raw_args:
        await update.message.reply_text(
            "❌ <b>Sahi Tarika:</b>\n<code>/addrule [purana_text] -> [naya_text]</code>\n\n<b>Example:</b>\n<code>/addrule MovieHub -> DG_Contents</code>", 
            parse_mode="HTML"
        )
        return
    
    try:
        old_part, new_part = raw_args.split("->")
        old_text = old_part.strip()
        new_text = new_part.strip()
        
        if not old_text or not new_text:
            raise ValueError
            
        REPLACEMENT_RULES[old_text] = new_text
        await update.message.reply_text(f"✅ Rule Added!\n🔍 Search: <code>{old_text}</code>\n✏️ Replace: <code>{new_text}</code>", parse_mode="HTML")
    except Exception:
        await update.message.reply_text("❌ Kuch galat hua. Check karein ki '->' lagaya hai ya nahi.")

async def del_rule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global REPLACEMENT_RULES
    if not context.args:
        await update.message.reply_text("❌ <b>Sahi Tarika:</b> <code>/delrule [purana_text]</code>")
        return
        
    old_text = " ".join(context.args).strip()
    if old_text in REPLACEMENT_RULES:
        del REPLACEMENT_RULES[old_text]
        await update.message.reply_text(f"🗑️ Rule for <code>{old_text}</code> has been deleted.", parse_mode="HTML")
    else:
        await update.message.reply_text(f"❌ Ye word list me nahi mila.")

async def clear_rules(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global REPLACEMENT_RULES
    REPLACEMENT_RULES.clear()
    await update.message.reply_text("🧹 Saare replacement rules clear kar diye gaye hain!")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global REPLACEMENT_RULES
    if not REPLACEMENT_RULES:
        await update.message.reply_text("📊 Filhal koi active replacement rules nahi hain. Bot sirf texts ko Bold karega.")
        return
        
    status_msg = "📊 <b>Active Replacement Rules:</b>\n\n"
    for old, new in REPLACEMENT_RULES.items():
        status_msg += f"🔍 <code>{old}</code> ➡️ ✏️ <code>{new}</code>\n"
        
    await update.message.reply_text(status_msg, parse_mode="HTML")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    welcome_text = (
        "Hyy Bhai! Bot active hai aur multiple words replace kar sakta hai.\n\n"
        "<b>Commands:</b>\n"
        "/addrule [purana] -> [naya] - Naya replacement text add karein\n"
        "/delrule [purana] - Kisi purane text rule ko delete karein\n"
        "/clear - Saare rules delete karein\n"
        "/status - Active rules list check karein"
    )
    await update.message.reply_text(welcome_text, parse_mode="HTML")

def main():
    threading.Thread(target=start_dummy_server, daemon=True).start()
    
    TOKEN = os.environ.get("BOT_TOKEN")
    if not TOKEN:
        raise ValueError("BOT_TOKEN environmental variable missing!")

    app = ApplicationBuilder().token(TOKEN).build()
    
    # Handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("addrule", add_rule))
    app.add_handler(CommandHandler("delrule", del_rule))
    app.add_handler(CommandHandler("clear", clear_rules))
    app.add_handler(CommandHandler("status", status))
    
    # Channel message handler
    app.add_handler(MessageHandler(filters.ChatType.CHANNEL, edit_channel_caption))
    
    print("Bot is polling with Multiple Replacement features...")
    app.run_polling()

if __name__ == '__main__':
    main()
