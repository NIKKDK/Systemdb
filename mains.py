import asyncio
import json
import os
from datetime import datetime

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import OperationFailure
from pyrogram import filters
from pyrogram.errors import FloodWait


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
    
    
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from pyrogram import filters
from pyrogram.errors import FloodWait

from config import BANNED_USERS, OWNER_ID

@app.on_message(filters.command("listdb"))
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



@app.on_message(filters.command("deletedball"))
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
