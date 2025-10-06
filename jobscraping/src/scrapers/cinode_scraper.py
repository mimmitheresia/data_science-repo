from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from src.scrapers.abstract_scraper import AbstractScraper


class CinodeScraper(AbstractScraper):
    site = "Cinode"

    def __init__(self):
        self.site = "Cinode"

    def _request_status(self):
        url = "https://cinode.market/requests"
        response = requests.get(url)
        print(f"{self.__class__.site} > Response:", response.status_code)
        return response

    def _extract_job_payloads(self, response):
        scraped_html = BeautifulSoup(response.text, "html.parser")
        job_payloads = scraped_html.find_all("app-list-row")

        print(f"{self.site} > Nmr of scraped adds:", len(job_payloads))
        return job_payloads

    def extract_id(self, payload):
        try:
            site_id = self.extract_site_id(payload)
            return f"{self.site}-{site_id}"
        except:
            return None

    def extract_site_id(self, payload):
        try:
            return self.extract_link(payload)
        except:
            return None

    def extract_job_title(self, payload):
        try:
            tag_link = payload.find("a", class_="list__heading", href=True)
            job_title = (tag_link.get_text(strip=True)).encode("latin1").decode("utf-8")
            return job_title
        except:
            return None

    def extract_link(self, payload):
        try:
            tag_link = payload.find("a", class_="list__heading", href=True)
            link = f"https://cinode.market{tag_link['href']}"
            return link
        except:
            None
