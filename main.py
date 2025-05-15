
from pymongo import MongoClient
from pyrogram import Client
from pyrogram.errors import FloodWait
import time
import datetime

# === Configuration ===
MONGO_URI = "mongodb+srv://username:password@cluster.mongodb.net/dbname"
API_ID = 123456                      # Replace with your API ID
API_HASH = "your_api_hash"          # Replace with your API Hash
BOT_TOKEN = "your_bot_token"        # Replace with your Bot Token
TARGET_CHANNEL = "@your_channel_username"  # Replace with your channel username or ID

# === MongoDB Setup ===
client = MongoClient(MONGO_URI)
db = client["your_db_name"]
collection = db["your_collection_name"]

# === Telegram Bot Setup ===
app = Client("forward_progress_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

with app:
    docs = list(collection.find())
    total = len(docs)
    print(f"Total messages to forward: {total}")

    start_time = time.time()

    for i, doc in enumerate(docs, start=1):
        chat_id = doc.get("chat_id")
        message_id = doc.get("message_id")

        if chat_id and message_id:
            try:
                app.forward_messages(
                    chat_id=TARGET_CHANNEL,
                    from_chat_id=chat_id,
                    message_ids=message_id
                )
                elapsed = time.time() - start_time
                avg_time = elapsed / i
                remaining = total - i
                eta = datetime.timedelta(seconds=int(avg_time * remaining))

                print(f"[{i}/{total}] Forwarded message {message_id} | Remaining: {remaining} | ETA: {eta}")
                time.sleep(1)

            except FloodWait as e:
                print(f"FloodWait: Sleeping for {e.value} seconds...")
                time.sleep(e.value)
            except Exception as e:
                print(f"Error forwarding message {message_id}: {e}")
