from prefect import flow, task
import subprocess
import json
from cleaner import Cleaner
from pymongo import MongoClient
import os
from dotenv import load_dotenv

dotenv_path = '/mongodb/.env'
load_dotenv(dotenv_path=dotenv_path)

@task
def run_scraper(
    job_title: str = "data",
    max_pages: int = 5
    ) -> list[str]:
    """Execute the js script to scrap job offers from wttj and return api links."""
    command = ["node", "js_scripts/scrap.js", job_title, str(max_pages)]

    try:
        result = subprocess.run(command, capture_output=True, text=True, encoding="utf-8")
        output = json.loads(result.stdout)
        return output
    except subprocess.CalledProcessError as e:
        print(f"Error running the script: {e.stderr}")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")

@task
def api_request(
    link: str
    ) -> dict[str, str | float]:
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

@task
def clean_data(job: dict[str, str | float]) -> dict[str, str | float]:
    """Clean the job offer."""
    cleaner = Cleaner(job)
    cleaned_job = cleaner.clean_full()
    return cleaned_job

@task
def insert_data(data: list[dict[str, str | float]]) -> None:
    username = os.getenv('MONGO_INITDB_ROOT_USERNAME')
    password = os.getenv('MONGO_INITDB_ROOT_PASSWORD')
    host = 'localhost'
    port = 27017

    if not username or not password:
        raise ValueError("MongoDB credentials are not set in environment variables.")

    try:
        client = MongoClient(f'mongodb://{username}:{password}@{host}:{port}/?authSource=admin')

        db = client.offers
        collection = db.offers

        result = collection.insert_many(data)
        # print(f"Data inserted with id: {result.inserted_id}")

    except Exception as e:
        print(f"An error occurred: {e}")

@flow(name="Scrape Job Offers", log_prints=True)
def scrap_job_offers(
    job_title: str = "data",
    max_pages: int = 5,
    ) -> None:
    api_links = run_scraper(job_title, max_pages)

    result = []

    if api_links:
        for link in api_links[:2]:
            offer_info = api_request(link)
            if offer_info:
                cleaned_offer_info = clean_data(offer_info)
                result.append(cleaned_offer_info)
            else:
                print(f"Failed to fetch offer details for link: {link}")

    with open("offers.json", "w", encoding="utf-8") as file:
        json.dump(result, file, ensure_ascii=False, indent=2)

    insert_data(result)


if __name__ == "__main__":
    scrap_job_offers(max_pages=1)