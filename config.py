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

# Get it from Termux and generate your latest pyrogram string session. 
STRING_SESSION = getenv("STRING_SESSION", None)

# You can add more configuration variables as needed

# Your User ID.
OWNER_ID = list(
    map(int, getenv("OWNER_ID", "").split())
)  # Input type must be interger

BANNED_USERS = filters.user()
