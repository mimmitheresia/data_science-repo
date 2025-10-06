import re
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from src.scrapers.abstract_scraper import AbstractScraper


class ProfinderScraper(AbstractScraper):
    site = "Profinder"

    def __init__(self):
        self.site = "Profinder"

    def _request_status(self):
        url = "https://www.profinder.se/lediga-uppdrag"
        response = requests.get(url)
        print(f"{self.__class__.site} > Response:", response.status_code)
        return response

    def _extract_job_payloads(self, response):
        scraped_html = BeautifulSoup(response.text, "html.parser")
        job_payloads = scraped_html.select("div.item-link-wrapper")

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
            tag_job_title = payload.find("div", class_="item-action")
            job_title_id = tag_job_title.get(
                "aria-label"
            )  # extracts the content of aria-label
            job_title = re.sub(r"\s*ID:\d+", "", job_title_id)
            return job_title
        except:
            return None

    def extract_link(self, payload):
        try:
            tag_link = payload.find("a", href=True)
            link = tag_link["href"]
            return link
        except:
            None
