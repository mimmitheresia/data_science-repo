from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from src.scrapers.abstract_scraper import AbstractScraper
from src.utils import return_regex_string_match, slugify_title_for_link


class NikitaScraper(AbstractScraper):
    site = "Nikita"

    def __init__(self):
        super().__init__()
        self.site = "Nikita"

    def _request_status(self):
        url = "https://www.nikita.se/lediga-uppdrag/"
        headers = {}

        response = requests.get(url, headers=headers)
        print(f"{self.__class__.site} > Response:", response.status_code)
        return response

    def _extract_job_payloads(self, response):
        tag_job_div = "li.open-position-item.opened"
        scraped_html = BeautifulSoup(response.text, "html.parser")
        job_payloads = scraped_html.select(tag_job_div)

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
            tag_title = payload.select_one("span.open-position-title")
            job_title = tag_title.get_text(strip=True) if tag_title else ""
            return job_title
        except:
            return None

    def extract_link(self, payload):
        try:
            tag_link = payload.select_one("a.open-position-list-link")
            link = tag_link.get("href") if tag_link else ""
            return link
        except:
            None
