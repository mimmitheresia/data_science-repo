import re
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from src.scrapers.abstract_scraper import AbstractScraper


class UpgradedScraper(AbstractScraper):
    site = "Upgraded"

    def __init__(self):
        super().__init__()
        self.site = "Upgraded"


    def _request_status(self):
        url = "https://upgraded.se/wp-admin/admin-ajax.php"

        payload = {
            "action": "do_filter_posts",
            "nonce": "39f36792ad",  # OBS: kan ändras per session
            "params[ort-term]": "alla-orter",
            "params[roll-term]": "alla-roller",
            "params[kund-term]": "alla-kunder",
            "params[ansokdate-term]": "sortering",
            "params[search-term]": ""
        }

        headers = {
            "User-Agent": "Mozilla/5.0",
            "Content-Type": "application/x-www-form-urlencoded"
        }

        response = requests.post(url, data=payload, headers=headers)
   
        print(f"{self.__class__.site} > Response:", response.status_code)
        return response

    def _extract_job_payloads(self, response):
        scraped_html = BeautifulSoup(response.json()["content"], "html.parser")
        tag_job_div = "tr.konsultuppdrag__table-row"
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
            tag_job_title = payload.find("h5", class_="entry-title")
            job_title = tag_job_title.get_text(strip=True) if tag_job_title else ""
            return job_title
        except:
            return None

    def extract_area(self, payload):
        try:
            tags_all_span = payload.select("span")
            area = tags_all_span[6].get_text(strip=True)
            return area
        except:
            None

    def extract_due_date(self, payload):
        try:
            due_date = payload.select_one(".konsultuppdrag-column-3").get_text(strip=True)
            return due_date
        except:
            None

    def extract_work_location(self, payload):
        try:
            tags_all_span = payload.select("span")
            work_location = tags_all_span[2].get_text(strip=True)
            return work_location
        except:
            None

    def extract_work_type(self, payload):
        try:
            tags_all_span = payload.select("span")
            work_type = tags_all_span[4].get_text(strip=True)
            return work_type
        except:
            None

    def extract_link(self, payload):
        try:
            tag_link = payload.find("a", href=True)
            return tag_link["href"]
        except:
            None
