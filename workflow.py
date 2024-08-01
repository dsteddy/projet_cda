from prefect import flow, task
import asyncio
import subprocess
import json
from cleaner import Cleaner
from pymongo import MongoClient, errors
import os
import pickle
from dotenv import load_dotenv

dotenv_path = './mongodb/.env'
load_dotenv(dotenv_path=dotenv_path)

# Load MongoDB credentials from environment
def get_db_connection():
    username = os.getenv('MONGO_INITDB_ROOT_USERNAME')
    password = os.getenv('MONGO_INITDB_ROOT_PASSWORD')
    host = os.getenv('MONGO_HOST')
    port = os.getenv('MONGO_PORT')

    if not username or not password:
        raise ValueError("MongoDB credentials are not set in environment variables.")

    try:
        client = MongoClient(f'mongodb://{username}:{password}@{host}:{port}/?authSource=admin')
        return client.offers.offers
    except errors.ConnectionFailure as e:
        print(f"Could not connect to MongoDB: {e}")

# Scrap offers from wttj
@task
def run_scraper(job_title: str = "data", max_pages: int = 40) -> list[str]:
    """Execute the js script to scrap job offers from wttj and return api links."""
    command = ["node", "js_scripts/scrap.js", job_title, str(max_pages)]

    try:
        result = subprocess.run(command, capture_output=True, text=True, encoding="utf-8")
        print(result.stdout[:10])
        output = json.loads(result.stdout)
        return output
    except subprocess.CalledProcessError as e:
        print(f"Error running the script: {e.stderr}")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")

# Get info from scraped offers
@task
def api_request(link: str) -> dict[str, str | float]:
    """Get job offer info from API."""
    command = ["node", "js_scripts/api_request.js", link]

    try:
        result = subprocess.run(command, capture_output=True, text=True, encoding="utf-8")
        output = json.loads(result.stdout)
        return output
    except subprocess.CalledProcessError as e:
        print(f"Error running the script: {e.stderr}")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")

# Clean info from offers
@task
def clean_data(job: dict[str, str | float]) -> dict[str, str | float]:
    """Clean the job offer."""
    cleaner = Cleaner(job)
    return cleaner.clean_full()

# Insert Data in MongoDB
@task
def insert_data(data: list[dict[str, str | float]], collection):
    try:
        for offer in data:
            existing_id = collection.find_one({'id': offer['id']})
            if existing_id:
                print(f'ID : "{offer["id"]}" already in db.')
                modif_date_db = existing_id['date_modif']
                modif_date_scrap = offer['date_modif']
                if modif_date_db < modif_date_scrap:
                    collection.replace_one({'id': offer['id']}, offer)
                    print(f'Offer "{offer["id"]}" updated.')
                else:
                    print(f'No modifications have been made to offer "{offer["id"]}".')
            else:
                collection.insert_one(offer)
                print(f'Offer "{offer["id"]}" added to db.')
    except errors.PyMongoError as e:
        print(f"An error occured: {e}")

def save_cache(data, filename):
    with open(filename, 'wb') as file:
        pickle.dump(data, file)

def load_cache(filename):
    if os.path.exists(filename):
        with open(filename, 'rb') as file:
            return pickle.load(file)

# Retrieve all IDs from MongoDB
@task
def get_all_db_ids(collection) -> set[str]:
    cache_filename = 'db_ids_cache.pkl'
    cached_ids = load_cache(cache_filename)
    if cached_ids:
        return cached_ids

    ids = collection.find({}, {'id': 1, '_id': 0})
    id_set = set([doc['id'] for doc in ids])
    save_cache(id_set, cache_filename)
    return id_set

# Remove offers that are not valid anymore
@task
def remove_old_offers(ids_to_del: set[str], collection):
    try:
        result = collection.delete_many({'id': {'$in': list(ids_to_del)}})
        print(f'Deleted {result.deleted_count} offers.')
    except errors.PyMongoError as e:
        print(f'An error occurred: {e}')

@flow(
        name="Scrape Job Offers",
        log_prints=True,
        # schedule=IntervalSchedule(interval=timedelta(minutes=30))
        )
def scrap_job_offers(job_title: str = "data", max_pages: int = 40) -> None:
    api_links = run_scraper(job_title, max_pages).result()
    result = []

    if api_links:
        for link in api_links:
            offer_info = api_request(link).result()
            if offer_info:
                cleaned_offer_info = clean_data(offer_info).result()
                result.append(cleaned_offer_info)
            else:
                print(f"Failed to fetch offer details for link: {link}")
    api_ids = {offer['id'] for offer in result}
    if db_ids:
        db_ids += api_ids
    else:
        db_ids = get_all_db_ids(collection).result()
        db_ids += api_ids

    collection = get_db_connection()
    if collection:
        insert_data(result, collection)
        api_ids = {offer['id'] for offer in result}
        ids_to_del = db_ids - api_ids
        remove_old_offers(ids_to_del, collection)

if __name__ == "__main__":
    scrap_job_offers(max_pages=1)