from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from pymongo import MongoClient
from pyrogram.errors import FloodWait
import asyncio
import logging
import gc
from info import *

# Global control variables
cancel_process = False
skip_count = 0  # Default skip count
failed = 0
total = 0

def get_status_message(index, skip_count, failed, e_value=None):
    """Generates a status message for file sending progress."""
    global total
    total += 1
    status = f"Sleeping {e_value}" if e_value else "Sending Files"
    return f"""
╔════❰ ꜰɪʟᴇ ꜱᴇɴᴅᴇʀ - {total} ❱═❍⊱❁۪۪
║╭━━━━━━━━━━━━━━━➣
║┣⪼ <b>🕵 ғᴇᴄʜᴇᴅ Msɢ :</b> <code>{index}</code>
║┣⪼ <b>✅ Cᴏᴍᴩʟᴇᴛᴇᴅ:</b> <code>{(index-failed)-skip_count}</code>
║┣⪼ <b>🪆 Sᴋɪᴩᴩᴇᴅ Msɢ :</b> <code>{skip_count}</code>
║┣⪼ <b>⚠️ Fᴀɪʟᴇᴅ:</b> <code>{failed}</code>
║┣⪼ <b>📊 Cᴜʀʀᴇɴᴛ Sᴛᴀᴛᴜs:</b> <code>{status}</code>
║╰━━━━━━━━━━━━━━━➣ 
╚════❰ ꜰɪʟᴇ ꜱᴇɴᴅᴇʀ ❱══❍⊱❁۪۪
"""

@Client.on_message(filters.command("setskip"))
async def set_skip(client, message):
    """Allows admins to set a skip count for resuming file sending."""
    global skip_count
    try:
        skip_count = int(message.text.split(" ")[1])
        await message.reply_text(f"✅ Skip count set to {skip_count} files.")
    except (IndexError, ValueError):
        await message.reply_text("❌ Invalid format! Use `/setskip <number>` (e.g., `/setskip 5`).")

@Client.on_message(filters.command("send"))
async def send_files(client, message):
    """Sends all indexed files from MongoDB to a specified channel."""
    global cancel_process, skip_count, failed, total
    cancel_process = False
    failed = 0
    total = 0

    # MongoDB Setup
    DBUSER = message.from_user.id
    fs = await client.ask(chat_id=message.from_user.id, text="📌 Send the MongoDB URL")
    MONGO_URI = fs.text
    fs2 = await client.ask(chat_id=message.from_user.id, text="📌 Send the Database Name")
    DB_NAME = fs2.text
    fs3 = await client.ask(chat_id=message.from_user.id, text="📌 Send the Collection Name")
    COLLECTION_NAME = fs3.text
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    files_collection = db[COLLECTION_NAME]  # Collection where files are stored

    fsd = await client.ask(chat_id=message.from_user.id, text="📌 Send the Destination Channel ID or Username\n(Make sure the bot is an admin in the channel)")
    CHANNEL_ID = fsd.text

    # Notify user about process start
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_process")]])
    status_message = await client.send_message(
        message.chat.id,
        "🚀 Starting to send files...",
        reply_markup=keyboard
    )

    # **Retrieve all files stored in MongoDB using `_id` and `file_ref`**
    cursor = files_collection.find({}, {"_id": 1, "file_ref": 1}).skip(skip_count)  

    index = skip_count
    for file_data in cursor:  # Apply skip count
        if cancel_process:
            await status_message.edit_text("❌ Process canceled by the user.")
            return

        index += 1
        try:
            file_id = file_data["_id"]  # Telegram File ID
            file_ref = file_data["file_ref"]  # Reference to the file

            # **Send the file using the stored `file_ref`**
            await client.send_cached_media(chat_id=CHANNEL_ID, file_id=file_id)

        except FloodWait as e:
            logging.warning(f'⚠️ Flood wait of {e.value} seconds detected')
            await status_message.edit_text(get_status_message(index, skip_count, failed, e.value), reply_markup=keyboard)
            await asyncio.sleep(e.value)
            continue  # Skip current file and continue

        except Exception as e:
            logging.error(f'❌ Failed to send file: {e}')
            failed += 1
            await status_message.edit_text(get_status_message(index, skip_count, failed), reply_markup=keyboard)

        # Trigger garbage collection to free memory
        gc.collect()

        # Update status message
        await status_message.edit_text(get_status_message(index, skip_count, failed), reply_markup=keyboard)

    await status_message.edit_text("✅ All files have been sent successfully!")

@Client.on_callback_query()
async def handle_callbacks(client, callback_query):
    """Handles cancel button click event."""
    global cancel_process

    if callback_query.data == "cancel_process":
        cancel_process = True
        await callback_query.message.edit_text("❌ Process canceled by the user.")
        await callback_query.answer("Process canceled!")
