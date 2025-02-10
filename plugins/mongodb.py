from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from pymongo import MongoClient
from pyrogram.errors import FloodWait
import asyncio
import logging
import gc
from info import *
from umongo import Instance, Document, fields
import base64

# Global control variables
cancel_process = False
skip_count = 0  # Default skip count
batch_size = 10  # Reduce batch size to lower memory consumption
failed = 0
total = 0

def get_status_message(index, skip_count, failed, e_value=None):
    global total
    total += 1
    if e_value:
        return f"""
╔════❰ ꜰꜱʙᴏᴛᴢ - {total} ❱═❍⊱❁۪۪
║╭━━━━━━━━━━━━━━━➣
║┣⪼<b>🕵 ғᴇᴄʜᴇᴅ Msɢ :</b> <code>{index}</code>
║┃
║┣⪼<b>✅ Cᴏᴍᴩʟᴇᴛᴇᴅ:</b> <code>{(index-failed)-skip_count}</code>
║┃
║┣⪼<b>🪆 Sᴋɪᴩᴩᴇᴅ Msɢ :</b> <code>{skip_count}</code>
║┃
║┣⪼<b>⚠️ Fᴀɪʟᴇᴅ:</b> <code>{failed}</code>
║┃
║┣⪼<b>📊 Cᴜʀʀᴇɴᴛ Sᴛᴀᴛᴜs:</b> <code>Sleeping {e_value}</code>
║┃
║╰━━━━━━━━━━━━━━━➣ 
╚════❰ ꜰꜱʙᴏᴛᴢ ❱══❍⊱❁۪۪
"""
    else:
        return f"""
╔════❰ ꜰꜱʙᴏᴛᴢ - {total} ❱═❍⊱❁۪۪
║╭━━━━━━━━━━━━━━━➣
║┣⪼<b>🕵 ғᴇᴄʜᴇᴅ Msɢ :</b> <code>{index}</code>
║┃
║┣⪼<b>✅ Cᴏᴍᴩʟᴇᴛᴇᴅ:</b> <code>{(index-failed)-skip_count}</code>
║┃
║┣⪼<b>🪆 Sᴋɪᴩᴩᴇᴅ Msɢ :</b> <code>{skip_count}</code>
║┃
║┣⪼<b>⚠️ Fᴀɪʟᴇᴅ:</b> <code>{failed}</code>
║┃
║┣⪼<b>📊 Cᴜʀʀᴇɴᴛ Sᴛᴀᴛᴜs:</b> <code>Sending Files</code>
║┃
║╰━━━━━━━━━━━━━━━➣ 
╚════❰ ꜰꜱʙᴏᴛᴢ ❱══❍⊱❁۪۪
"""

@Client.on_message(filters.command("setskip"))
async def set_skip(client, message):
    global skip_count
    try:
        skip_count = int(message.text.split(" ")[1])
        await message.reply_text(f"✅ Skip count set to {skip_count} files.")
    except (IndexError, ValueError):
        await message.reply_text("❌ Invalid format! Use `/setskip <number>` (e.g., `/setskip 5`).")

@Client.on_message(filters.command("send"))
async def send_files(client, message):
    global cancel_process, skip_count, failed, total
    cancel_process = False  # Reset cancel flag
    failed = 0  # Reset failed count
    total = 0   # Reset total count

    # MongoDB Setup Start
    DBUSER = message.from_user.id
    fs = await client.ask(chat_id=message.from_user.id, text="Now Send Me The MongoDB URL")
    MONGO_URI = fs.text
    fs2 = await client.ask(chat_id=message.from_user.id, text="Now Send Me The DB Name")
    DB_NAME = fs2.text
    fs3 = await client.ask(chat_id=message.from_user.id, text="Now Send Me The Collection Name")
    COLLECTION_NAME = fs3.text
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    movies_collection = db[COLLECTION_NAME]
    # MongoDB Setup End
    instance = Instance.from_db(db)
    @instance.register
    class Media(Document):
        file_id = fields.StrField(attribute='_id')
        file_ref = fields.StrField(allow_none=True)
        file_name = fields.StrField(required=True)
        file_size = fields.IntField(required=True)
        file_type = fields.StrField(allow_none=True)
        mime_type = fields.StrField(allow_none=True)
        caption = fields.StrField(allow_none=True)
        class Meta:
            indexes = ('$file_name', )
            collection_name = COLLECTION_NAME
    async def get_file_details(query):
        filter = {'file_id': query}
        cursor = Media.find(filter)
        filedetails = await cursor.to_list(length=1)
        return filedetails
    fsd = await client.ask(chat_id=message.from_user.id, text="Now Send Me The Destination Channel ID Or Username\nMake Sure That Bot Is Admin In The Destination Channel")
    CHANNEL_ID = fsd.text

    # Notify user about the process start with cancel button
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_process")]])
    status_message = await client.send_message(
        message.chat.id,
        "Starting to send files to the channel...",
        reply_markup=keyboard
    )

    # Apply the skip count and stream files from MongoDB
    cursor = movies_collection.find().skip(skip_count)

    index = skip_count
    for file in cursor:
        if cancel_process:
            await status_message.edit_text("❌ Process canceled by the user.")
            return

        index += 1
        try:
            file_id = file.get("_id")
            if not file_id:
                raise ValueError("Invalid file ID")
            
            data = f"files_{file_id}"
            try:
                pre, file_id = data.split('_', 1)
            except:
                file_id = data
                pre = ""
            
            if data.startswith("files"):
                files_ = await get_file_details(file_id)
                if not files_:
                    pre, file_id = ((base64.urlsafe_b64decode(data + "=" * (-len(data) % 4))).decode("ascii")).split("_", 1)
                    try:
                        msg = await client.send_cached_media(
                            chat_id=CHANNEL_ID,
                            file_id=file_id,
                            protect_content=False
                        )
                        return
                    except:
                        pass
                    return
                files = files_[0]
                f_caption=files.file_name
                msg = await client.send_cached_media(
                    chat_id=CHANNEL_ID,
                    file_id=file_id,
                    caption=f_caption,
                    protect_content=False
                )
                return

        except FloodWait as e:
            logging.warning(f'Flood wait of {e.value} seconds detected')
            new_status_message = get_status_message(index, skip_count, failed, e.value)
            if new_status_message != status_message.text:
                await status_message.edit_text(new_status_message, reply_markup=keyboard)
            await asyncio.sleep(e.value)
            continue  # Skip the current file and continue with the next one
        except Exception as e:
            logging.error(f'Failed to send file: {e}')
            failed += 1
            new_status_message = get_status_message(index, skip_count, failed)
            if new_status_message != status_message.text:
                await status_message.edit_text(new_status_message, reply_markup=keyboard)

        # Trigger garbage collection to free memory
        gc.collect()

        # Update status in the user chat
        new_status_message = get_status_message(index, skip_count, failed)
        if new_status_message != status_message.text:
            await status_message.edit_text(new_status_message, reply_markup=keyboard)

    await status_message.edit_text("✅ All files have been sent successfully!")

@Client.on_callback_query()
async def handle_callbacks(client, callback_query):
    global cancel_process

    if callback_query.data == "cancel_process":
        cancel_process = True
        await callback_query.message.edit_text("❌ Process canceled by the user.")
        await callback_query.answer("Process canceled!")
