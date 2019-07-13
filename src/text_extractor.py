from bs4 import BeautifulSoup
from typing import Tuple
import newspaper
import requests
import json


class TextExtractor(object):
    HTTP_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
        "Accept-Language": "en-US;q=0.8,en;q=0.7"}

    def __init__(self):
        pass

    @staticmethod
    def get_content(url: str) -> BeautifulSoup:
        response = requests.get(url, allow_redirects=True, headers=TextExtractor.HTTP_HEADERS)
        response.raise_for_status()
        content = BeautifulSoup(response.text, "lxml")
        return content

    @staticmethod
    def _extract_text_default(html: str) -> Tuple[str, str]:
        """ Method for extracting the (relevant) plain text content from a HTML document. """
        try:
            title = ""
            text = ""
            content = BeautifulSoup(html, "lxml")
            for script in content(["script", "style", "pre", "code", "aside"]):
                script.extract()
            select = [e.text.strip() for e in content.select("p")]
            if content.find("title") is not None:
                title = content.find("title").text.strip()
            text = "\n".join(select)
        except TypeError:
            print("Could not parse payload: {}".format(html))
            title = ""
            text = ""
        return title, text

    @staticmethod
    def _extract_text_fancy(html: str, content: BeautifulSoup) -> Tuple[str, str]:
        title = text = ""
        text = newspaper.fulltext(html)
        if content.find("title") is not None:
            title = content.find("title").text.strip()
        return title, text

    @staticmethod
    def _extract_text_arxiv(content: BeautifulSoup) -> Tuple[str, str]:
        title = text = ""
        if content.find("title") is not None:
            title = content.find("title").text.strip()
        if content.find("blockquote", {"class": "abstract mathjax"}) is not None:
            text = content.find("blockquote", {"class": "abstract mathjax"}).text.strip()
        return title, text

    @staticmethod
    def _extract_text_twitter(content: BeautifulSoup) -> Tuple[str, str]:
        title = ""
        text = ""
        if content.find("title") is not None:
            text = content.find("title").text.strip()
            title = text[:50] + "..."
        return title, text

    @staticmethod
    def _extract_text_bloomberg(content: BeautifulSoup) -> Tuple[str, str]:
        # Bloomberg requires javascript enabled, i.e., in-memory rendering of pages. Skip for now.
        return "", ""

    @staticmethod
    def _extract_text_instagram(content: BeautifulSoup) -> Tuple[str, str]:
        title = text = ""
        if content.find("script") is not None:
            try:
                data = json.loads(content.find('script', type='application/ld+json').text)
                text = data.get("caption", "")
                title = data.get("name")
                if title is not None:
                    title = title.split(":")[0]
            except Exception as err:
                print(f"Gott error: {err}")
        return title, text

    @staticmethod
    def extract_text(html, url) -> Tuple[str, str]:
        try:
            title = text = ""
            content = BeautifulSoup(html, "lxml")
            if "twitter.com" in url:
                title, text = TextExtractor._extract_text_twitter(content)
            elif "arxiv.org" in url:
                title, text = TextExtractor._extract_text_arxiv(content)
            elif "bloomberg.com" in url:
                title, text = TextExtractor._extract_text_bloomberg(content)
            elif "instagram.com" in url:
                title, text = TextExtractor._extract_text_instagram(content)
            else:
                try:
                    title, text = TextExtractor._extract_text_fancy(html, content)
                except AttributeError as a_err:
                    print("Could not extract title and text using Newspaper3k, resorting to basic tech")
                    title, text = TextExtractor._extract_text_default(html)
        except TypeError as t_err:
            print("Could not parse payload, got error '{}' for payload '{}'".format(t_err, html))
            title = ""
            text = ""
        return title, text
