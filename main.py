from pymongo import MongoClient
from pyrogram import Client, filters
import logging

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Bot Configurations
API_ID = "12380656"
API_HASH = "d927c13beaaf5110f25c505b7c071273"
BOT_TOKEN = "8007837520:AAGIpK0CdS6U8gsx3a-m491ZFO8SurC4a7k"
AUTHORIZED_USERS = [7648939888, 987654321]  # Replace with authorized Telegram user IDs

# MongoDB Configurations
SOURCE_DB_URI = "mongodb+srv://source_user:password@source.mongodb.net/source_db"
DEST_DB_URI = "mongodb+srv://dest_user:password@dest.mongodb.net/dest_db"

source_client = MongoClient(SOURCE_DB_URI)
dest_client = MongoClient(DEST_DB_URI)

# Initialize Pyrogram Bot
app = Client("mongo_transfer_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

@app.on_message(filters.command("start") & filters.private)
async def start(client, message):
    if message.from_user.id not in AUTHORIZED_USERS:
        await message.reply("You are not authorized to use this bot.")
        return
    await message.reply("Welcome to MongoDB Transfer Bot! Use /transfer <source_collection> <dest_collection> to begin.")

@app.on_message(filters.command("transfer") & filters.private)
async def transfer_data(client, message):
    if message.from_user.id not in AUTHORIZED_USERS:
        await message.reply("You are not authorized to use this bot.")
        return

    try:
        # Parse command arguments
        args = message.text.split()
        if len(args) != 3:
            await message.reply("Usage: /transfer <source_collection> <dest_collection>")
            return

        source_collection_name = args[1]
        dest_collection_name = args[2]

        source_db = source_client.get_database()
        dest_db = dest_client.get_database()

        source_collection = source_db[source_collection_name]
        dest_collection = dest_db[dest_collection_name]

        # Transfer data securely
        documents = list(source_collection.find())
        if not documents:
            await message.reply(f"No data found in the source collection: {source_collection_name}")
            return

        # Insert documents into destination collection
        dest_collection.insert_many(documents)
        await message.reply(f"Data transferred from `{source_collection_name}` to `{dest_collection_name}` successfully.")

        # Log the transfer
        logging.info(f"User {message.from_user.id} transferred data from {source_collection_name} to {dest_collection_name}.")

    except Exception as e:
        logging.error(f"Error during transfer: {e}")
        await message.reply(f"An error occurred: {e}")

@app.on_message(filters.command("collections") & filters.private)
async def list_collections(client, message):
    if message.from_user.id not in AUTHORIZED_USERS:
        await message.reply("You are not authorized to use this bot.")
        return

    try:
        source_db = source_client.get_database()
        collections = source_db.list_collection_names()
        await message.reply(f"Available collections in source DB:\n{', '.join(collections)}")
    except Exception as e:
        logging.error(f"Error fetching collections: {e}")
        await message.reply(f"An error occurred: {e}")

if __name__ == "__main__":
    app.run()
