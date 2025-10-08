from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from src.scrapers.abstract_scraper import AbstractScraper
from src.utils import slugify_title_for_link


class ITCNetworkScraper(AbstractScraper):
    site = "ITC Network"

    def __init__(self):
        super().__init__()
        self.site = "ITC Network"

    def _request_status(self):
        url = "https://itcnetwork.se/uppdrag/"
        headers = {
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
        }

        response = requests.get(url, headers=headers)

        print(f"{self.__class__.site} > Response:", response.status_code)
        return response

    def _extract_job_payloads(self, response):
        scraped_html = BeautifulSoup(response.text, "html.parser")
        tag_job_div = "div.maf_feed_single.maf_wid100"
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
            tag_job_title = payload.select_one("h3.maf_feed_title b")
            site_id = tag_job_title.get_text(strip=True) if tag_job_title else None
            return site_id
        except:
            return None

    def extract_job_title(self, payload):
        try:
            return self.extract_site_id(payload)
        except:
            return None

    def extract_link(self, payload):
        try:
            return f"https://itcnetwork.se/uppdrag/"
        except:
            None
