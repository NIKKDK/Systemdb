import os
import time
import json
import psutil
import asyncio
import logging
from bson import ObjectId
from datetime import datetime
from pyrogram.errors import FloodWait
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pymongo import MongoClient
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import OperationFailure
from config import *
import config
import pymongo
from config import *
import motor.motor_asyncio
from pyrogram.types import Message
from YukkiMusic import LOGGER, app


# Temporary storage for URIs (per user)
user_data = {}

# Start time for bot uptime tracking
bot_start_time = time.time()


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




# MongoDB connection details
async def backup_all_databases(mongo_url: str):
    # Connect to MongoDB using the provided URL
    client = motor.motor_asyncio.AsyncIOMotorClient(mongo_url)

    # Get the list of all databases in the MongoDB server
    db_names = await client.list_database_names()

    if not db_names:
        return "No databases found on the server."

    all_data = {}

    for db_name in db_names:
        try:
            # Connect to the database
            db = client[db_name]

            # Get all collection names from the database
            collection_names = await db.list_collection_names()

            # Skip this database if no collections are found
            if not collection_names:
                continue

            db_data = {}

            for collection in collection_names:
                # Fetch all documents from the collection
                collection_data = await db[collection].find().to_list(length=None)

                # Add collection data to the database's data
                if collection_data:
                    db_data[collection] = collection_data

            if db_data:
                all_data[db_name] = db_data

        except Exception as e:
            print(f"Error backing up database {db_name}: {str(e)}")

    # If no data was collected, return an error message
    if not all_data:
        return "Error: No data was backed up."

    # Write all data to the JSON file
    temp_filename = f"backup_all_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    try:
        with open(temp_filename, 'w') as f:
            json.dump(all_data, f, default=str, indent=4)
        return temp_filename
    except Exception as e:
        return f"Error creating the backup file: {str(e)}"


@app.on_message(filters.command("backup"))
async def backup_handler(client: Client, message: Message):
    if len(message.command) < 2:
        await message.reply("Please provide the MongoDB URL after /backup command.")
        return
    
    mongo_url = message.command[1]

    # Send a text message indicating that the backup process has started
    await message.reply("Backup process has started... This may take some time depending on the size of your databases.")

    # Call the backup function for all databases
    backup_file = await backup_all_databases(mongo_url)

    if isinstance(backup_file, str) and backup_file.endswith('.json'):
        # Send the backup file to the user
        try:
            # Ensure the file exists before sending
            if os.path.exists(backup_file) and os.path.getsize(backup_file) > 0:
                await message.reply_document(document=backup_file)
                # Clean up the temporary backup file after sending it
                os.remove(backup_file)
                await message.reply("Backup completed successfully.")
            else:
                await message.reply("Error: The backup file is empty or could not be found.")
        except Exception as e:
            await message.reply(f"Error sending backup: {str(e)}")
    else:
        await message.reply(backup_file)  # In case of error message


# Store MongoDB URI dynamically (user input)
user_mongo_uri = None

@app.on_message(filters.command("restore"))
async def restore_command(client: Client, message: Message):
    global user_mongo_uri
    
    # Ask the user for the MongoDB URI (if not set)
    if not user_mongo_uri:
        await message.reply("Please provide your MongoDB URI to start restoring data. Format: `mongodb+srv://username:password@host:port/database_name`")
    else:
        # Notify the user to send the file to restore data
        await message.reply("Please send me the file to restore the data.")

@app.on_message(filters.text)
async def set_mongo_uri(client: Client, message: Message):
    global user_mongo_uri
    
    # Store the MongoDB URI from user input
    if not user_mongo_uri and message.text.startswith("mongodb+srv://"):
        user_mongo_uri = message.text
        await message.reply(f"MongoDB URI set to {user_mongo_uri}. Now send the file to restore data.")
    else:
        # Handle other commands or unexpected inputs
        await message.reply("Invalid MongoDB URI. Please provide a valid MongoDB URI starting with `mongodb+srv://`.")

@app.on_message(filters.document)
async def handle_file(client: Client, message: Message):
    global user_mongo_uri
    
    if message.document:
        try:
            if not user_mongo_uri:
                await message.reply("Please provide the MongoDB URI first using the /restore command.")
                return
            
            client_mongo = pymongo.MongoClient(user_mongo_uri)

            file_id = message.document.file_id
            downloaded_file = await message.download()

            with open(downloaded_file, "r") as file:
                data = json.load(file)

            if isinstance(data, dict):
                for db_name, collections_data in data.items():
                    if db_name in ['local', 'admin', 'config']:
                        LOGGER.info(f"Skipping system database: {db_name}")
                        continue

                    db = client_mongo[db_name]

                    for collection_name, collection_data in collections_data.items():
                        collection = db[collection_name]
                        if isinstance(collection_data, list):
                            try:
                                collection.insert_many(collection_data, ordered=False)  # Skip duplicates
                                LOGGER.info(f"Inserted documents into {db_name}.{collection_name}")
                            except pymongo.errors.BulkWriteError as bwe:
                                LOGGER.warning(f"Duplicate documents skipped for {db_name}.{collection_name}: {bwe.details}")
                        else:
                            LOGGER.warning(f"Skipping invalid data format for {db_name}.{collection_name}")
                await message.reply("Data restored successfully with duplicates skipped.")
            else:
                await message.reply("The file format is incorrect. Please provide a valid JSON file.")

        except Exception as e:
            LOGGER.error(f"Error during file processing: {e}")
            await message.reply(f"An error occurred while processing the file: {e}")
