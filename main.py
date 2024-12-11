from pyrogram import Client, filters
from pymongo import MongoClient
import asyncio

# Telegram Bot Credentials
API_ID = "12380656"
API_HASH = "d927c13beaaf5110f25c505b7c071273"
BOT_TOKEN = "8007837520:AAGIpK0CdS6U8gsx3a-m491ZFO8SurC4a7k"

# Pyrogram Client
bot = Client("mongo_transfer_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# Temporary storage for URIs (per user)
user_data = {}

@bot.on_message(filters.command("start") & filters.private)
async def start(client, message):
    await message.reply_text(
        "👋 Welcome to the MongoDB Transfer Bot!\n\n"
        "Send `/set_old <old_mongo_uri>` to provide the URI for the old MongoDB instance."
    )

@bot.on_message(filters.command("setold") & filters.private)
async def set_old(client, message):
    try:
        if len(message.command) < 2:
            await message.reply_text("❌ Please provide the old MongoDB URI. Example:\n`/set_old mongodb://username:password@host:port/`")
            return

        old_uri = message.text.split(" ", 1)[1]
        user_data[message.from_user.id] = {"old_uri": old_uri}
        await message.reply_text(
            "✅ Old MongoDB URI saved! Now send `/set_new <new_mongo_uri>` to provide the URI for the new MongoDB instance."
        )
    except Exception as e:
        await message.reply_text(f"❌ An error occurred: {str(e)}")

@bot.on_message(filters.command("setnew") & filters.private)
async def set_new(client, message):
    try:
        if len(message.command) < 2:
            await message.reply_text("❌ Please provide the new MongoDB URI. Example:\n`/set_new mongodb://username:password@host:port/`")
            return

        new_uri = message.text.split(" ", 1)[1]
        if message.from_user.id not in user_data or "old_uri" not in user_data[message.from_user.id]:
            await message.reply_text("❌ You need to set the old MongoDB URI first using `/set_old`.")
            return

        user_data[message.from_user.id]["new_uri"] = new_uri
        await message.reply_text("✅ New MongoDB URI saved! Send `/transfer` to start the migration.")
    except Exception as e:
        await message.reply_text(f"❌ An error occurred: {str(e)}")

@bot.on_message(filters.command("transfer") & filters.private)
async def transfer_data(client, message):
    try:
        user_id = message.from_user.id
        if user_id not in user_data or "old_uri" not in user_data[user_id] or "new_uri" not in user_data[user_id]:
            await message.reply_text("❌ Please set both old and new MongoDB URIs using `/set_old` and `/set_new`.")
            return

        old_uri = user_data[user_id]["old_uri"]
        new_uri = user_data[user_id]["new_uri"]

        # Connect to MongoDB
        old_client = MongoClient(old_uri)
        new_client = MongoClient(new_uri)

        await message.reply_text("Starting transfer of all databases...")

        # Get list of databases
        old_db_list = old_client.list_database_names()
        success_count, failure_count = 0, 0

        for db_name in old_db_list:
            if db_name in ["admin", "config", "local"]:  # Skip system databases
                continue

            old_db = old_client[db_name]
            new_db = new_client[db_name]
            collections = old_db.list_collection_names()

            for collection_name in collections:
                old_collection = old_db[collection_name]
                new_collection = new_db[collection_name]

                # Transfer all documents
                documents = list(old_collection.find())
                if documents:
                    new_collection.insert_many(documents)
                success_count += len(documents)

        await message.reply_text(
            f"✅ Transfer completed!\n\n**Databases transferred**: {len(old_db_list)}\n**Documents transferred**: {success_count}"
        )
    except Exception as e:
        await message.reply_text(f"❌ An error occurred: {str(e)}")
    finally:
        # Close MongoDB connections
        old_client.close()
        new_client.close()

@bot.on_message(filters.command("status") & filters.private)
async def status(client, message):
    await message.reply_text("Bot is running and ready to transfer data.")

if __name__ == "__main__":
    print("Bot is starting...")
    bot.run()
