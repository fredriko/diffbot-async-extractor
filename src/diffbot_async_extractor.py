import asyncio
import logging

import pymongo
import csv
import hashlib
import time
from typing import Dict, List
import random

import requests
from w3lib.url import canonicalize_url, url_query_cleaner
from pymongo.collection import Collection
from src.third_party.diffbot import DiffbotClient
from src.third_party import asyncioplus
from bs4 import BeautifulSoup


def set_up_db(db: str, collection: str) -> Collection:
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    return client[db][collection]


logging.basicConfig(level=logging.DEBUG)

USER_AGENT_HEADER = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36"}


def diffbot_extract(url: str, access_token: str) -> Dict[str, object]:
    diffbot = DiffbotClient()
    try:
        response = diffbot.request(url, access_token, "analyze")
    except requests.exceptions.HTTPError as error:
        print("Got error when calling Diffbot for url: {} - {}".format(error, url))
        response = None
    return response


def extract_text(html, url=None) -> str:
    """ Method for extracting the (relevant) plain text content from a HTML document. """
    try:
        text = ""
        content = BeautifulSoup(html, "lxml")
        if url is not None and "https://twitter.com" in url:
            if content.find("title") is not None:
                text = content.find("title").text.strip()
        else:
            for script in content(["script", "style", "pre", "code", "aside"]):
                script.extract()
            select = [e.text.strip() for e in content.select("p")]
            if content.find("title") is not None:
                title = content.find("title").text.strip()
                select.insert(0, title)
            text = "\n".join(select)
        # Heuristics adopted from elsewhere: when the "raw" text of the HTML document
        # is much larger than that of the text extracted from paragraphs, use the raw
        # text. Useful if, e.g., the HTML does not contain paragraph mark-up
        # if 0.1 * len(str(content.get_text())) >= len(text):
        #    text = str(content.get_text)
    except TypeError:
        print("Could not parse payload: {}".format(html))
        text = ""
    return text


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
            response = requests.get(url, allow_redirects=True, headers=USER_AGENT_HEADER)
        except requests.exceptions.SSLError as ssl_error:
            result = f"Could not retrieve url {url} - got error: {ssl_error}"
            return result
        except requests.exceptions.ConnectionError as connection_error:
            result = f"Could not retrieve url {url} - got error: {connection_error}"
            return result
        if response.ok:
            text = extract_text(response.text, url=url)
            collection.insert_one({"_id": id, "url": url, "text": text})
            result = f"Extracted text from url {url} in {(time.time() - start_time)} seconds"
        else:
            result = f"Response status: {response.status_code} - Could not extract data from url {url}"
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
                collection.insert_one(response)
                result = "Extracted text from url {} in {} seconds.".format(url, (time.time() - start_time))
        else:
            result = "Nothing extracted."
    else:
        result = "Document already in the database: {}".format(url)
    return result


async def execute_tasks(tasks, num_tasks: int):
    num_tasks_completed = 0
    for result in asyncioplus.limited_as_completed(tasks, 150):
        num_tasks_completed += 1
        print("{}/{} Done: {}".format(num_tasks_completed, num_tasks, await result), flush=True)


if __name__ == "__main__":
    ### CONFIGURE
    diffbot_api_token = None
    input_file = "/path/to/urls.csv"
    name_of_url_field = "url"
    db_name = "texts"
    db_collection_name = "plain_text"
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
