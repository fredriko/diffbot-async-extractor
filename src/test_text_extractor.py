from unittest import TestCase
from bs4 import BeautifulSoup
from src.text_extractor import TextExtractor
import requests


class TestExtractText(TestCase):

    def test_extract_text(self):
        url = "https://medium.com/suspended"
        response = requests.get(url, allow_redirects=True, headers=TextExtractor.USER_AGENT)
        title, text = TextExtractor.extract_text(response.text, url)
        print(f"title: {title}")
        print(f"text:  {text}")

    def test_extract_text_newspaper(self):
        url = "https://arxiv.org/abs/1807.08518"
        response = requests.get(url, allow_redirects=True, headers=TextExtractor.USER_AGENT)
        content = BeautifulSoup(response.text, "lxml")
        title, text = TextExtractor._extract_text_fancy(response.text, content)
        print(f"title: {title}")
        print(f"text:  {text}")

    def test_extract_text_arxiv(self):
        url = "https://arxiv.org/abs/1807.08518"
        content = TextExtractor.get_content(url)
        title, text = TextExtractor._extract_text_arxiv(content)
        print(f"title: {title}")
        print(f"text:  {text}")

    def test_extract_text_instagram(self):
        url = "https://www.instagram.com/p/BfWL3G_BFTM/?hl=en&taken-by=ptr_yeung"
        response = requests.get(url, allow_redirects=True, headers=TextExtractor.USER_AGENT)
        title, text = TextExtractor.extract_text(response.text, url)
        print(f"title: {title}")
        print(f"text:  {text}")

