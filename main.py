import time
import psutil
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pymongo import MongoClient

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

# Helper function to format bot uptime
def get_uptime():
    uptime_seconds = int(time.time() - bot_start_time)
    hours, remainder = divmod(uptime_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours}h {minutes}m {seconds}s"

# Helper function to get system info
def get_system_info():
    # Get system CPU usage and memory info
    cpu_usage = psutil.cpu_percent(interval=1)
    memory = psutil.virtual_memory()
    return cpu_usage, memory.percent

@app.on_message(filters.command("start"))
async def start(client, message):
    await message.reply_text(
        "**👋 Welcome to the MongoDB Transfer Bot!**\n\n"
        "**Commands:**\n"
        "/setold `<old_mongo_uri>` - Set old MongoDB URI\n"
        "/setnew `<new_mongo_uri>` - Set new MongoDB URI\n"
        "/setold `<old_mongo_uri>` - To check how many databases this mongo has\n"
        "/transfer - Start transferring data\n"
        "/listalldb - List all databases in the old MongoDB instance\n"
        "/status - Check bot status\n"
        "/ping - Get system info and bot uptime",
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

@app.on_message(filters.command("setnew") & filters.private)
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
async def list_all_dbs(client, message):
    old_client = None  # Initialize old_client here
    try:
        user_id = message.from_user.id
        if user_id not in user_data or "old_uri" not in user_data[user_id]:
            await message.reply_text("**❌ Please set the old MongoDB URI first using `/set_old`.**", parse_mode=ParseMode.MARKDOWN)
            return

        old_uri = user_data[user_id]["old_uri"]

        # Connect to MongoDB
        old_client = MongoClient(old_uri)
        db_list = old_client.list_database_names()

        # Filter out system databases
        db_list = [db for db in db_list if db not in ["admin", "config", "local"]]

        if db_list:
            await message.reply_text(
                "**✅ Databases in the old MongoDB instance:**\n\n" + "\n".join(f"- `{db}`" for db in db_list),
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await message.reply_text("**❌ No databases found in the old MongoDB instance.**", parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        await message.reply_text(f"**❌ An error occurred:** `{str(e)}`", parse_mode=ParseMode.MARKDOWN)
    finally:
        if old_client:
            old_client.close()  # Ensure this is only called if old_client was created


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

        await message.reply_text("**Starting transfer of all databases...**", parse_mode="markdown")

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

@app.on_message(filters.command("ping"))
async def ping(client, message):
    try:
        # Get bot uptime
        uptime = get_uptime()

        # Get system info (CPU and memory usage)
        cpu_usage, memory_usage = get_system_info()

        response = (
            f"**📊 System Info and Bot Uptime**\n\n"
            f"**Bot Uptime:** `{uptime}`\n"
            f"**CPU Usage:** `{cpu_usage}%`\n"
            f"**Memory Usage:** `{memory_usage}%`"
        )

        await message.reply_text(response, parse_mode=ParseMode.MARKDOWN)

    except Exception as e:
        await message.reply_text(f"**❌ An error occurred:** `{str(e)}`", parse_mode=ParseMode.MARKDOWN)

if __name__ == "__main__":
    print("Bot is starting...")
    app.run()
