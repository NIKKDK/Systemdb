import os
import re
import sys
from os import getenv
from dotenv import load_dotenv
from pyrogram import filters, Client

# Load environment variables from .env file
load_dotenv()

# Get it from my.telegram.org
API_ID = int(getenv("API_ID"))
API_HASH = getenv("API_HASH")

## Get it from @Botfather in Telegram.
BOT_TOKEN = getenv("BOT_TOKEN")

# Database to save your chats and stats... Get MongoDB:-  https://telegra.ph/How-To-get-Mongodb-URI-04-06
MONGO_DB_URI = getenv("MONGO_DB_URI", "mongodb+srv://vikkyydv:vikky34@vikkyydv.ovbza.mongodb.net/?retryWrites=true&w=majority&appName=vikkyydv")

# Get it from Termux and generate your latest pyrogram string session. 
STRING_SESSION = getenv("STRING_SESSION", None)

# You can add more configuration variables as needed

# Your User ID.
OWNER_ID = list(
    map(int, getenv("OWNER_ID", "").split())
)  # Input type must be interger

BANNED_USERS = filters.user()
