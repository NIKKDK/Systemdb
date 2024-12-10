from pymongo import MongoClient, errors
from tqdm import tqdm
import logging


# Bot Configurations
API_ID = "12380656"
API_HASH = "d927c13beaaf5110f25c505b7c071273"
BOT_TOKEN = "8007837520:AAGIpK0CdS6U8gsx3a-m491ZFO8SurC4a7k"
AUTHORIZED_USERS = [7648939888, 987654321]  # Replace with authorized Telegram user IDs


#logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("data_transfer.log"),
        logging.StreamHandler()
    ]
)

# MongoDB configurations
OLD_DB_URI = "mongodb+srv://Bikash:Bikash@bikash.yl2nhcy.mongodb.net/?retryWrites=true&w=majority"
NEW_DB_URI = "mongodb+srv://public:abishnoimf@cluster0.rqk6ihd.mongodb.net/?retryWrites=true&w=majority"

# List specific collections to transfer (leave empty to transfer all collections)
COLLECTIONS_TO_TRANSFER = []

# MongoDB Clients
old_client = MongoClient(OLD_DB_URI)
new_client = MongoClient(NEW_DB_URI)

def transfer_collection(old_db, new_db, collection_name):
    """
    Transfer a single collection from the old database to the new database.

    Args:
        old_db: The source MongoDB database object.
        new_db: The destination MongoDB database object.
        collection_name: The name of the collection to transfer.
    """
    try:
        old_collection = old_db[collection_name]
        new_collection = new_db[collection_name]

        # Fetch all documents from the old collection
        documents = list(old_collection.find())

        if not documents:
            logging.info(f"No data found in collection: {collection_name}")
            return

        # Insert documents into the new collection
        logging.info(f"Transferring {len(documents)} documents from '{collection_name}'...")
        new_collection.insert_many(documents)
        logging.info(f"Successfully transferred collection: {collection_name}")

    except errors.BulkWriteError as bwe:
        logging.error(f"Bulk write error while transferring {collection_name}: {bwe.details}")
    except Exception as e:
        logging.error(f"Error transferring {collection_name}: {e}")

def transfer_database(old_db_name, new_db_name):
    """
    Transfer data from the old database to the new database.

    Args:
        old_db_name: The name of the source database.
        new_db_name: The name of the destination database.
    """
    old_db = old_client[old_db_name]
    new_db = new_client[new_db_name]

    # Get collections to transfer
    if COLLECTIONS_TO_TRANSFER:
        collections = COLLECTIONS_TO_TRANSFER
    else:
        collections = old_db.list_collection_names()

    logging.info(f"Starting data transfer from '{old_db_name}' to '{new_db_name}'...")
    for collection_name in tqdm(collections, desc="Transferring collections"):
        transfer_collection(old_db, new_db, collection_name)

    logging.info(f"Data transfer from '{old_db_name}' to '{new_db_name}' completed.")

if __name__ == "__main__":
    # Specify the old and new database names
    OLD_DB_NAME = "Bikash"
    NEW_DB_NAME = "public"

    # Execute the transfer
    try:
        transfer_database(OLD_DB_NAME, NEW_DB_NAME)
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        # Close MongoDB clients
        old_client.close()
        new_client.close()
