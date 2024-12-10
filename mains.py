from pyrogram import Client, filters
from pymongo import MongoClient
import os

# Bot Configuration
API_ID = "12380656"  # Replace with your API ID
API_HASH = "d927c13beaaf5110f25c505b7c071273"  # Replace with your API Hash
BOT_TOKEN = "8007837520:AAGIpK0CdS6U8gsx3a-m491ZFO8SurC4a7k"  # Replace with your Bot Token

# MongoDB Configuration
MONGO_URI = "mongodb+srv://public:abishnoimf@cluster0.rqk6ihd.mongodb.net/?retryWrites=true&w=majority"  # Replace with your MongoDB URI
DATABASE_NAME = "public"  # Replace with your database name

# Initialize the Telegram bot
app = Client("mongoDbCheckerBot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# Initialize MongoDB client
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DATABASE_NAME]

@app.on_message(filters.command("start"))
async def start_command(client, message):
    """Handles the /start command."""
    await message.reply(
        "Hello! I am a bot to query data from MongoDB.\n\n"
        "**Commands:**\n"
        "/collections - List all collections\n"
        "/query <collection> <field>=<value> - Query data from a collection\n"
        "/help - Show help menu"
    )

@app.on_message(filters.command("collections"))
async def list_collections(client, message):
    """Lists all collections in the database."""
    try:
        collections = db.list_collection_names()
        if not collections:
            await message.reply("No collections found in the database.")
        else:
            response = "Collections in the database:\n\n" + "\n".join(collections)
            await message.reply(response)
    except Exception as e:
        await message.reply(f"An error occurred while listing collections: {str(e)}")

@app.on_message(filters.command("query"))
async def query_command(client, message):
    """Handles the /query command."""
    try:
        if len(message.command) < 3:
            await message.reply("Usage: /query <collection> <field>=<value>")
            return

        collection_name = message.command[1]
        query_text = message.command[2]
        field, value = query_text.split("=")

        # Access the collection
        collection = db[collection_name]

        # Prepare the query
        query = {field.strip(): value.strip()}
        
        # Fetch data
        results = collection.find(query)

        # Prepare the response
        response = f"Query Results from `{collection_name}`:\n\n"
        found = False
        for doc in results:
            found = True
            response += f"{doc}\n\n"

        if not found:
            response = f"No matching documents found in `{collection_name}`."

        # Send the response
        await message.reply(response)

    except Exception as e:
        await message.reply(f"An error occurred: {str(e)}")

@app.on_message(filters.command("help"))
async def help_command(client, message):
    """Handles the /help command."""
    await message.reply(
        "**Commands:**\n"
        "/start - Welcome message\n"
        "/collections - List all collections in the database\n"
        "/query <collection> <field>=<value> - Query data from a specific collection\n"
        "/help - Show this help message"
    )

if __name__ == "__main__":
    print("Bot is running...")
    app.run()
