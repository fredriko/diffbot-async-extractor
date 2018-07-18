import asyncio

import pymongo
import csv
import hashlib
import time
from typing import Dict, List

import requests
from w3lib.url import canonicalize_url, url_query_cleaner
from pymongo.collection import Collection
from src.third_party.diffbot import DiffbotClient
from src.third_party import asyncioplus


def set_up_db(db: str, collection: str) -> Collection:
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    return client[db][collection]


def diffbot_extract(url: str, access_token: str) -> Dict[str, object]:
    diffbot = DiffbotClient()
    try:
        response = diffbot.request(url, access_token, "analyze")
    except requests.exceptions.HTTPError as error:
        print("Got error when calling Diffbot for url: {} - {}".format(error, url))
        response = None
    return response


def read_urls_from_csv(csv_file: str, url_field: str) -> List[str]:
    result = []
    with open(csv_file, newline="") as input_file:
        reader = csv.DictReader(input_file)
        for row in reader:
            result.append(row[url_field])
    return result


def compute_id(url: str) -> str:
    normalized_url = canonicalize_url(url_query_cleaner(url, [], remove=False))
    return hashlib.md5(normalized_url.encode('utf-8')).hexdigest()


async def extract_async(diffbot_api_token: str, url: str, collection: Collection) -> str:
    id = compute_id(url)
    document = collection.find_one({"_id": id})
    if document is None:
        start_time = time.time()
        response = diffbot_extract(url, diffbot_api_token)
        if response is not None:
            if "errorCode" in response:
                print("Error in retrieving data from Diffbot. Error code {}: {}".format(response["errorCode"],
                                                                                        response["error"]))
                result = "Could not extract data: {}".format(response["error"])
            else:
                response["_id"] = id
                collection.insert_one(response)
                result = "Extracted text from url {} in {} seconds.".format(url, (time.time() - start_time))
        else:
            result = "Nothing extracted."
    else:
        result = "Document already in the database: {}".format(url)
    return result


async def execute_tasks(tasks, num_tasks: int):
    num_tasks_completed = 0
    for result in asyncioplus.limited_as_completed(tasks, 50):
        num_tasks_completed += 1
        print("{}/{} Done: {}".format(num_tasks_completed, num_tasks, await result), flush=True)


if __name__ == "__main__":
    ### CONFIGURE
    diffbot_api_token = "<your diffbot api token>"
    input_file = "/path/to/csv_file_with_urls.csv"
    name_of_url_field = "url"
    db_name = "texts"
    db_collection_name = "diffbot"
    ### END CONFIGURE

    collection = set_up_db(db_name, db_collection_name)
    urls = read_urls_from_csv(input_file, name_of_url_field)

    tasks = (extract_async(diffbot_api_token, url, collection) for url in urls)
    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(execute_tasks(tasks, len(urls)))
    event_loop.close()
