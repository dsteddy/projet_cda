from prefect import flow, task
import subprocess
import json
from cleaner import Cleaner
from pymongo import MongoClient, errors
import os
import pickle
from dotenv import load_dotenv
import asyncio
from tqdm.asyncio import tqdm
import timeit

dotenv_path = './mongodb/.env'
load_dotenv(dotenv_path=dotenv_path)

# Load MongoDB credentials from environment
def get_db_connection():
    """Retrieve login credentials from .env file and create connection to offers table from MongoDB."""
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
    """Execute js script to scrap job offers from wttj and return a list api links."""
    command = ["node", "js_scripts/scrap.js", job_title, str(max_pages)]

    try:
        result = subprocess.run(command, capture_output=True, text=True, encoding="utf-8")
        output = json.loads(result.stdout)
        print(f"Scraped {len(output)} offers.")
        return output
    except subprocess.CalledProcessError as e:
        print(f"Error running the script: {e.stderr}")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")

# Get info from scraped offers
@task
def api_requests(api_links: list[str]):
    """Start api request for all api links and return job offers data"""
    result = asyncio.run(fetch_all(api_links))
    return result

async def fetch(command: list[str]) -> dict[str, str | float]:
    """Individual task to start api request from js script for one job offer"""
    while True:
        try:
            result = subprocess.run(command, capture_output=True, text=True, encoding="utf-8")
            output = json.loads(result.stdout)
            return output
        except subprocess.CalledProcessError as e:
            print(f"Error running the script: {e.stderr}")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")
            asyncio.sleep(30)

async def fetch_all(api_links: list[str]) -> list[dict[str, str | float]]:
    """List the tasks and gather the results to request api data from all the links in api_links list"""
    tasks = []
    for link in api_links:
        command = ["node", "js_scripts/api_request.js", link]
        task = asyncio.create_task(fetch(command))
        tasks.append(task)
        await asyncio.sleep(0.07)
    responses = []
    with tqdm(total=len(tasks), desc="Fetching API data") as pbar:
        for task in asyncio.as_completed(tasks):
            result = await task
            responses.append(result)
            pbar.update(1)

    return responses

# Clean info from offers
@task
def clean(jobs):
    cleaned_result = asyncio.run(clean_all(jobs))
    return cleaned_result

async def clean_data(job: dict[str, str | float]) -> dict[str, str | float]:
    """Clean the job offer."""
    cleaner = Cleaner(job)
    return cleaner.clean_full()

async def clean_all(job_offers: list[dict[str, str | float]]) -> list[dict[str, str | float]]:
    """List the tasks and gather the results to clean all job offers from the job_offers list"""
    tasks = []
    for offer in job_offers:
        task = asyncio.create_task(clean_data(offer))
        tasks.append(task)
    cleaned_offers = await asyncio.gather(*tasks)
    return cleaned_offers

# Insert Data in MongoDB
@task
def insert_data(data: list[dict[str, str | float]], collection):
    """Insert/update data in MongoDB table"""
    try:
        for offer in data:
            existing_id = collection.find_one({'id': offer['id']})
            if existing_id:
                print(f'Offer "{offer["id"]}" already in db.')
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
    """save list of id from MongoDB table in pkl file"""
    with open(filename, 'wb') as file:
        pickle.dump(data, file)

def load_cache(filename):
    """load list of id from MongoDB table in pkl file"""
    if os.path.exists(filename):
        with open(filename, 'rb') as file:
            return pickle.load(file)

# Retrieve all IDs from MongoDB
@task
def get_all_db_ids(collection) -> set[str]:
    """Retrieve all id from pkl file or directly from MongoDB table ir pkl file doesn't exist"""
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
    """Remove offers that are not available anymore"""
    try:
        result = collection.delete_many({'id': {'$in': list(ids_to_del)}})
        print(f'Deleted {result.deleted_count} offers.')
    except errors.PyMongoError as e:
        print(f'An error occurred: {e}')

@flow(name="Scrap Job Offers", log_prints=True)
def scrap_job_offers(job_title: str = "data", max_pages: int = 40) -> None:
    start_time = timeit.default_timer()

    api_links = run_scraper(job_title, max_pages)

    if api_links:
        result = api_requests(api_links)
        cleaned_result = clean(result)
        api_ids = set([offer["id"] for offer in cleaned_result])

    collection = get_db_connection()
    if collection is not None:
        try:
            db_ids
        except UnboundLocalError:
            db_ids = get_all_db_ids(collection)
        db_ids.update(api_ids)
        insert_data(cleaned_result, collection)
        ids_to_del = db_ids - api_ids
        remove_old_offers(ids_to_del, collection)

    end_time = timeit.default_timer()
    elapsed_time = end_time - start_time
    time_unit = "seconds" if elapsed_time < 60 else "minutes"
    print(f'Time: {elapsed_time:.2f} {time_unit}')

if __name__ == "__main__":
    scrap_job_offers(max_pages=1)