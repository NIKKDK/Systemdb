from pyrogram import Client, filters
from pymongo import MongoClient

# MongoDB setup
client = MongoClient('mongodb+srv://bikash:bikash@bikash.3jkvhp7.mongodb.net/?retryWrites=true&w=majority')  # MongoDB URI
db = client['OwnerBgt']  # Replace with your database name
users_collection = db['users']  # Replace with your collection name

# Bot Configuration
API_ID = "12380656"  # Replace with your API ID
API_HASH = "d927c13beaaf5110f25c505b7c071273"  # Replace with your API Hash
BOT_TOKEN = "8007837520:AAGIpK0CdS6U8gsx3a-m491ZFO8SurC4a7k"  # Replace with your Bot Token

# Initialize the Telegram bot
app = Client("mongoDbCheckerBot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

@app.on_message(filters.command("usercount"))
async def user_count(client, message):
    # Get the count of users from MongoDB
    user_count = users_collection.count_documents({})
    
    # Send the result to the chat
    await message.reply(f"There are {user_count} users in the database.")

# Run the bot
app.run()
