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
    def _extract_text_quanta_magazine(content: BeautifulSoup) -> Tuple[str, str]:
        # Example url with single content div:
        # https://www.quantamagazine.org/puzzle-with-infinite-regress-is-it-turtles-all-the-way-down-20200206/

        # Example url with multiple content divs:
        # https://www.quantamagazine.org/artificial-intelligence-will-do-what-we-ask-thats-a-problem-20200130/

        # The Quantamagazine site is littered with <script ...> tags. Remove them to make it easier to extract
        # the textual contents.
        script_tags = content.find_all("script")
        _ = [s.extract() for s in script_tags]
        title = content.select("#postBody > div:nth-of-type(1) > section > section > div > "
                               "div.post__title.pv1.scale1.mha > div > h1")[0].get_text().strip()

        text_containers = content.find_all("div", {"class": "post__content__section"})
        text = "\n".join([t.get_text().strip() for t in text_containers])
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
            elif "quantamagazine.org" in url:
                title, text = TextExtractor._extract_text_quanta_magazine(content)
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


if __name__ == "__main__":
    from src.main import HTTP_HEADERS

    url = "https://www.quantamagazine.org/puzzle-with-infinite-regress-is-it-turtles-all-the-way-down-20200206/"
    #url = "https://www.quantamagazine.org/artificial-intelligence-will-do-what-we-ask-thats-a-problem-20200130/"
    response = requests.get(url, allow_redirects=True, headers=HTTP_HEADERS)
    if response.ok:
        title, text = TextExtractor.extract_text(response.text, url=url)
        print(f"title='{title}'")
        print(f"text={text}")
