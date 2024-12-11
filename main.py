import os
import time
import json
import psutil
import asyncio
from bson import ObjectId
from datetime import datetime
from pyrogram.errors import FloodWait
from pyrogram import Client, filters, idle
from pyrogram.enums import ParseMode
from pymongo import MongoClient
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import OperationFailure
from config import *


loop = asyncio.get_event_loop()


# Telegram Bot Credentials
API_ID = "12380656"
API_HASH = "d927c13beaaf5110f25c505b7c071273"
BOT_TOKEN = "8007837520:AAGIpK0CdS6U8gsx3a-m491ZFO8SurC4a7k"


# Pyrogram Client
app = Client(
    "MongoDB", 
    api_id=API_ID, 
    api_hash=API_HASH, 
    bot_token=BOT_TOKEN
)

# Temporary storage for URIs (per user)
user_data = {}

# Start time for bot uptime tracking
bot_start_time = time.time()


async def main():
    cpu_usage, memory_usage = await get_system_info()
    await get_uptime()
    await app.start()
    await idle() 

# Helper function to format bot uptime
async def get_uptime():
    uptime_seconds = int(time.time() - bot_start_time)
    hours, remainder = divmod(uptime_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours}h {minutes}m {seconds}s"

# Helper function to get system info
async def get_system_info():
    # Get system CPU usage and memory info
    cpu_usage = psutil.cpu_percent(interval=1)
    memory = psutil.virtual_memory()
    return cpu_usage, memory.percent

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)  # Convert ObjectId to string
        if isinstance(obj, datetime):
            return obj.isoformat()  # Convert datetime to ISO 8601 format
        return super().default(obj)


async def export_database(db, db_name):
    """Export collections and documents of a MongoDB database to a JSON file."""
    data = {}
    collections = await db.list_collection_names()

    for collection_name in collections:
        collection = db[collection_name]
        documents = await collection.find().to_list(length=None)
        data[collection_name] = documents

    file_path = os.path.join("cache", f"{db_name}_backup.json")
    os.makedirs("cache", exist_ok=True)
    with open(file_path, "w") as backup_file:
        json.dump(data, backup_file, indent=4, cls=CustomJSONEncoder)

    return file_path


async def drop_database(client, db_name):
    """Drop a MongoDB database."""
    await client.drop_database(db_name)


async def edit_or_reply(message, text):
    """Edit or reply to a Pyrogram message."""
    try:
        return await message.edit_text(text, disable_web_page_preview=True)
    except FloodWait as e:
        await asyncio.sleep(e.value)
        return await message.edit_text(text, disable_web_page_preview=True)
    try:
        await message.delete()
    except Exception:
        pass
    return await app.send_message(message.chat.id, text, disable_web_page_preview=True)


@app.on_message(filters.command("export"))
async def export_all_databases(client, message):
    """Export all databases from a MongoDB instance."""
    if MONGO_DB_URI is None:
        return await message.reply_text(
            "Please set your `MONGO_DB_URI` to use this feature."
        )
    mystic = await message.reply_text("Exporting all databases...")
    mongo_client = AsyncIOMotorClient(MONGO_DB_URI)
    databases = await mongo_client.list_database_names()

    for db_name in databases:
        if db_name in ["local", "admin"]:
            continue

        db = mongo_client[db_name]
        mystic = await edit_or_reply(
            mystic, f"Processing `{db_name}` database... Exporting and deleting..."
        )
        file_path = await export_database(db, db_name)

        try:
            await app.send_document(
                message.chat.id,
                file_path,
                caption=f"Backup for `{db_name}` database."
            )
        except FloodWait as e:
            await asyncio.sleep(e.value)

        try:
            await drop_database(mongo_client, db_name)
        except OperationFailure:
            await edit_or_reply(
                mystic, f"Failed to delete `{db_name}` database due to permissions."
            )

        os.remove(file_path)

    await mystic.edit_text("All accessible databases exported and processed.")


@app.on_message(filters.command("import"))
async def import_database(client, message):
    """Import a database backup from a file."""
    if MONGO_DB_URI is None:
        return await message.reply_text(
            "Please set your `MONGO_DB_URI` to use this feature."
        )

    if not message.reply_to_message or not message.reply_to_message.document:
        return await message.reply_text("Reply to a backup file to import it.")

    mystic = await message.reply_text("Downloading backup file...")

    async def progress(current, total):
        try:
            await mystic.edit_text(f"Downloading... {current * 100 / total:.1f}%")
        except FloodWait as e:
            await asyncio.sleep(e.value)

    file_path = await message.reply_to_message.download(progress=progress)

    try:
        with open(file_path, "r") as backup_file:
            data = json.load(backup_file)
    except (json.JSONDecodeError, IOError):
        return await edit_or_reply(
            mystic, "Invalid backup file format. Please provide a valid JSON file."
        )

    mongo_client = AsyncIOMotorClient(MONGO_DB_URI)

    try:
        for db_name, collections in data.items():
            db = mongo_client[db_name]
            for collection_name, documents in collections.items():
                if documents:
                    collection = db[collection_name]
                    for document in documents:
                        await collection.replace_one(
                            {"_id": document["_id"]}, document, upsert=True
                        )
            await mystic.edit_text(f"Database `{db_name}` imported successfully.")
    except Exception as e:
        await mystic.edit_text(f"Error during import: {e}. Rolling back changes.")
    os.remove(file_path)
    


@app.on_message(filters.command("start"))
async def start(client, message):
    await message.reply_text(
        "**👋 Welcome to the MongoDB Transfer Bot!**\n\n"
        "**Commands:**\n"
        "/setold `<old_mongo_uri>` - Set old MongoDB URI\n"
        "/setnew `<new_mongo_uri>` - Set new MongoDB URI\n"
        "/transfer `<transfer_data>` - Start transferring data\n"
        "/listalldb `<see_data>` - List all databases in the old MongoDB instance\n"
        "/status `<status_process>` - Check bot status\n"
        "/ping `<uptime_bot>` - Get system info and bot uptime\n",
        "/clean `<delete_data>` - This cmd can delete all your entire data which is stored in your mongo db database\n", 
        parse_mode=ParseMode.MARKDOWN
    )

@app.on_message(filters.command("setold"))
async def set_old(client, message):
    try:
        if len(message.command) < 2:
            await message.reply_text(
                "**❌ Please provide the old MongoDB URI.**\nExample:\n`/setold mongodb://username:password@host:port/`",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        old_uri = message.text.split(" ", 1)[1]
        user_data[message.from_user.id] = {"old_uri": old_uri}
        await message.reply_text(
            "**✅ Old MongoDB URI saved!**\nNow send `/setnew <new_mongo_uri>` to provide the URI for the new MongoDB instance.",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        await message.reply_text(f"**❌ An error occurred:** `{str(e)}`", parse_mode=ParseMode.MARKDOWN)

@app.on_message(filters.command("setnew"))
async def set_new(client, message):
    try:
        if len(message.command) < 2:
            await message.reply_text(
                "**❌ Please provide the new MongoDB URI.**\nExample:\n`/setnew mongodb://username:password@host:port/`",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        new_uri = message.text.split(" ", 1)[1]
        if message.from_user.id not in user_data or "old_uri" not in user_data[message.from_user.id]:
            await message.reply_text("**❌ You need to set the old MongoDB URI first using `/setold`.**", parse_mode=ParseMode.MARKDOWN)
            return

        user_data[message.from_user.id]["new_uri"] = new_uri
        await message.reply_text(
            "**✅ New MongoDB URI saved!**\nSend `/transfer` to start the migration.",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        await message.reply_text(f"**❌ An error occurred:** `{str(e)}`", parse_mode=ParseMode.MARKDOWN)

@app.on_message(filters.command("listalldb"))
async def list_databases(client, message):
    

    if len(message.command) < 2:
        return await message.reply_text("Please provide a MongoDB URL as an argument.")

    mongo_url = message.command[1].strip()
    mystic = await message.reply_text("Fetching database list...")

    try:
        client = AsyncIOMotorClient(mongo_url)
        databases = await client.list_database_names()
        databases = [db for db in databases if db not in ["local", "admin"]]

        if not databases:
            await mystic.edit_text("No user-defined databases found.")
        else:
            db_list = "\n".join(f"- {db}" for db in databases)
            await mystic.edit_text(f"Databases in the MongoDB URL:\n\n{db_list}")
    except Exception as e:
        await mystic.edit_text(f"Error while connecting to MongoDB: {e}")
        
from motor.motor_asyncio import AsyncIOMotorClient
from pyrogram import filters



@app.on_message(filters.command("clean"))
async def delete_all_databases(client, message):
    

    if len(message.command) < 2:
        return await message.reply_text("Please provide a MongoDB URL as an argument.")

    mongo_url = message.command[1].strip()
    mystic = await message.reply_text("Connecting to MongoDB...")

    try:
        client = AsyncIOMotorClient(mongo_url)
        databases = await client.list_database_names()
        databases = [db for db in databases if db not in ["local", "admin"]]

        if not databases:
            return await mystic.edit_text("No user-defined databases to delete.")

        for db_name in databases:
            await client.drop_database(db_name)

        await mystic.edit_text("All user-defined databases have been deleted successfully.")
    except Exception as e:
        await mystic.edit_text(f"Error: {e}")  # Ensure this is only called if old_client was created


@app.on_message(filters.command("transfer"))
async def transfer_data(client, message):
    try:
        user_id = message.from_user.id
        if user_id not in user_data or "old_uri" not in user_data[user_id] or "new_uri" not in user_data[user_id]:
            await message.reply_text("**❌ Please set both old and new MongoDB URIs using `/setold` and `/setnew`.**", parse_mode=ParseMode.MARKDOWN)
            return

        old_uri = user_data[user_id]["old_uri"]
        new_uri = user_data[user_id]["new_uri"]

        # Connect to MongoDB
        old_client = MongoClient(old_uri)
        new_client = MongoClient(new_uri)

        await message.reply_text("**Starting transfer of all databases...**", parse_mode=ParseMode.MARKDOWN)

        # Get list of databases
        old_db_list = old_client.list_database_names()
        success_count = 0

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
            f"**✅ Transfer completed!**\n\n**Databases transferred:** `{len(old_db_list)}`\n**Documents transferred:** `{success_count}`",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        await message.reply_text(f"**❌ An error occurred:** `{str(e)}`", parse_mode=ParseMode.MARKDOWN)
    finally:
        old_client.close()
        new_client.close()

@app.on_message(filters.command("status"))
async def status(client, message):
    await message.reply_text("**Bot is running and ready to transfer data.**", parse_mode=ParseMode.MARKDOWN)


@app.on_message(filters.command("clean"))
async def delete_all_databases(client, message):
    

    if len(message.command) < 2:
        return await message.reply_text("Please provide a MongoDB URL as an argument.")

    mongo_url = message.command[1].strip()
    mystic = await message.reply_text("Connecting to MongoDB...")

    try:
        client = AsyncIOMotorClient(mongo_url)
        databases = await client.list_database_names()
        databases = [db for db in databases if db not in ["local", "admin"]]

        if not databases:
            return await mystic.edit_text("No user-defined databases to delete.")

        for db_name in databases:
            await client.drop_database(db_name)

        await mystic.edit_text("All user-defined databases have been deleted successfully.")
    except Exception as e:
        await mystic.edit_text(f"Error: {e}")        
         

@app.on_message(filters.command("ping"))
async def check_sping(client, message):
    start = datetime.now()
    end = datetime.now()
    ms = (end - start).microseconds / 1000
    m = await message.reply_text("**🤖 Ping...!!**")
    await m.edit(f"**🤖 Pinged...!!\nLatency:** `{ms}` ms")


if __name__ == "__main__":
    print("Bot is starting...")
    loop.run_until_complete(main())
