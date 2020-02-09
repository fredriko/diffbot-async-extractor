import asyncio
import logging
from datetime import datetime

import pymongo
import csv
import hashlib
import time
from typing import Dict, List
import random

import requests
from w3lib.url import canonicalize_url, url_query_cleaner
from pymongo.collection import Collection
from pymongo.errors import DocumentTooLarge

from src.text_extractor import TextExtractor
from src.third_party.diffbot import DiffbotClient
from src.third_party import asyncioplus


# TODO re-write to make sure tasks do not die. See rise-edu.
def set_up_db(db: str, collection: str) -> Collection:
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    return client[db][collection]


logging.basicConfig(level=logging.DEBUG)

HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
    "Accept-Language": "en-US;q=0.8,en;q=0.7"}


def diffbot_extract(url: str, access_token: str) -> Dict[str, object]:
    diffbot = DiffbotClient()
    try:
        response = diffbot.request(url, access_token, "analyze")
    except requests.exceptions.HTTPError as error:
        print("Got error when calling Diffbot for url: {} - {}".format(error, url))
        response = None
    return response


def read_urls_from_csv(csv_file: str, url_field: str, collection: Collection) -> List[str]:
    result = []
    num_docs_in_db = 0
    print("Reading urls from file: {}".format(csv_file))
    with open(csv_file, newline="") as input_file:
        reader = csv.DictReader(input_file)
        for row in reader:
            url = row[url_field]
            id = compute_id(url)
            document = collection.find_one({"_id": id})
            if document is None:
                result.append(row[url_field])
            else:
                num_docs_in_db += 1
    print("Got {} new urls and skipped {} existing urls.".format(len(result), num_docs_in_db, csv_file))
    random.shuffle(result)
    return result


def compute_id(url: str) -> str:
    normalized_url = canonicalize_url(url_query_cleaner(url, [], remove=False))
    return hashlib.md5(normalized_url.encode('utf-8')).hexdigest()


@asyncio.coroutine
async def extract_async_text(url: str, collection: Collection) -> str:
    id = compute_id(url)
    document = collection.find_one({"_id": id})
    if document is None:
        start_time = time.time()
        try:
            response = requests.get(url, allow_redirects=True, headers=HTTP_HEADERS)
        except requests.exceptions.SSLError as ssl_error:
            result = f"Could not retrieve url {url}. Failed after {(time.time()) - start_time} seconds - got error: {ssl_error}"
            collection.insert_one(
                {"_id": id,
                 "url": url,
                 "text_extracted_at": datetime.utcnow(),
                 "extraction_status_ok": False,
                 "extraction_fail_reason": "SSL error"
                 }
            )
            return result
        except requests.exceptions.ConnectionError as connection_error:
            result = f"Could not retrieve url {url}. Failed after {(time.time()) - start_time} seconds - got error: {connection_error}"
            collection.insert_one(
                {"_id": id,
                 "url": url,
                 "text_extracted_at": datetime.utcnow(),
                 "extraction_status_ok": False,
                 "extraction_fail_reason": "Connection error"
                 }
            )
            return result
        except requests.exceptions.ContentDecodingError as decoding_error:
            result = f"Could not retrieve url {url}. Failed after {(time.time()) - start_time} seconds - gor error: {decoding_error}"
            collection.insert_one(
                {"_id": id,
                 "url": url,
                 "text_extracted_at": datetime.utcnow(),
                 "extraction_status_ok": False,
                 "extraction_fail_reason": "Decoding error"
                 }
            )
            return result
        if response.ok:
            title, text = TextExtractor.extract_text(response.text, url=url)
            try:
                collection.insert_one(
                    {"_id": id,
                     "url": url,
                     "title": title,
                     "text": text,
                     "text_extracted_at": datetime.utcnow(),
                     "extraction_status_ok": True
                     })
                result = f"Extracted text from url {url} in {(time.time() - start_time)} seconds"
            except DocumentTooLarge as error:
                print(f"Got error: {error} for document with title: '{title}' and url: {url}")
                result = f"Could not extract document from {url} - too large document. Time {time.time() - start_time} seconds."
        else:
            result = f"Response status: {response.status_code} - Could not extract data from url {url}. Failed after {(time.time()) - start_time}"
            collection.insert_one(
                {"_id": id,
                 "url": url,
                 "text_extracted_at": datetime.utcnow(),
                 "extraction_status_ok": False,
                 "extraction_fail_reason": f"Extraction error - HTTP status: {response.status_code}"
                 }
            )
    else:
        result = f"Document already in database: {url}"
    return result


@asyncio.coroutine
async def extract_async_diffbot(diffbot_api_token: str, url: str, collection: Collection) -> str:
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
                response["text_extracted_at"] = datetime.utcnow()
                collection.insert_one(response)
                result = "Extracted text from url {} in {} seconds.".format(url, (time.time() - start_time))
        else:
            result = "Nothing extracted."
    else:
        result = "Document already in the database: {}".format(url)
    return result


async def execute_tasks(tasks, num_tasks: int):
    num_tasks_completed = 0
    for result in asyncioplus.limited_as_completed(tasks, 300):
        num_tasks_completed += 1
        print("{}/{} Done: {}".format(num_tasks_completed, num_tasks, await result), flush=True)


if __name__ == "__main__":
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    ### CONFIGURE
    diffbot_api_token = None
    input_file = "/Users/fredriko/Dropbox/data/metacurate-urls/urls.csv"
    name_of_url_field = "url"
    db_name = "texts"
    db_collection_name = "plain_text_w_title"
    ### END CONFIGURE

    collection = set_up_db(db_name, db_collection_name)
    urls = read_urls_from_csv(input_file, name_of_url_field, collection)

    if diffbot_api_token is not None:
        tasks = (extract_async_diffbot(diffbot_api_token, url, collection) for url in urls)
    else:
        tasks = (extract_async_text(url, collection) for url in urls)

    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(execute_tasks(tasks, len(urls)))
    event_loop.close()
