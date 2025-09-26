import ast
from datetime import datetime

import pandas as pd
import requests
from src.scrapers.abstract_scraper import AbstractScraper


class TingentScraper(AbstractScraper):
    site = "Tingent"

    def __init__(self):
        self.site = "Tingent"

    def request_status(self):
        url = "https://tingent.se/api/jobs"

        headers = {
            "accept": "application/json, text/plain, */*",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
            "referer": "https://tingent.se/sv/jobs",
        }

        response = requests.get(url, headers=headers)
        return response

    def _extract_job_payloads(self, response):
        scraped_data = response.json()  # parse till Python-dict
        job_payloads = scraped_data["data"]

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
            return payload["abstract_id"]
        except:
            return None

    def extract_job_title(self, payload):
        try:
            return payload["requisition_name"]
        except:
            return None

    def extract_area(self, payload):
        try:
            return payload["requisition_servicecategoryid"]
        except:
            return None

    def extract_due_date(self, payload):
        try:
            return payload["requisition_offerduedate"]
        except:
            return None

    def extract_work_location(self, payload):
        try:
            return payload["requisition_locationid"]
        except:
            return None

    def extract_link(self, payload):
        try:
            return "https://tingent.se/sv/jobs"
        except:
            None
