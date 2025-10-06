import ast
from datetime import datetime

import pandas as pd
import requests
from src.scrapers.abstract_scraper import AbstractScraper


class SigmaScraper(AbstractScraper):
    site = "Sigma"

    def __init__(self):
        self.site = "Sigma"

    def _request_status(self):
        url = "https://www.sigma.se/service/jobs.json?limit=53&type=assignment&language=sv&nocache=202509231400"
        response = requests.get(url)
        print(f"{self.__class__.site} > Response:", response.status_code)
        return response

    def _extract_job_payloads(self, response):
        scraped_data = response.json()  # parse till Python-dict
        job_payloads = scraped_data["items"]

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
            return payload["headline"]
        except:
            return None

    def extract_area(self, payload):
        try:
            return payload["tags"]
        except:
            return None

    def extract_due_date(self, payload):
        try:
            return payload["expire"]
        except:
            return None

    def extract_work_location(self, payload):
        try:
            return payload["location"]
        except:
            return None

    def extract_link(self, payload):
        try:
            return f"https://www.sigma.se/{payload['url']}"
        except:
            None
