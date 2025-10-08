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
        self.base_url = "https://upgraded.se/lediga-uppdrag/"
        self.ajax_url = "https://upgraded.se/wp-admin/admin-ajax.php"
        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}
        )

    def get_nonce(self):
        r = self.session.get(self.base_url)

        # find "nonce" inside page script
        match = re.search(r'"nonce":"([a-f0-9]+)"', r.text)
        if not match:
            raise ValueError("Nonce not found")
        return match.group(1)

    def _request_status(self):
        nonce = self.get_nonce()
        data = {
            "action": "do_filter_posts",
            "nonce": nonce,
            "params[ort-term]": "alla-orter",
            "params[roll-term]": "alla-roller",
            "params[kund-term]": "alla-kunder",
            "params[ansokdate-term]": "sortering",
            "params[search-term]": "",
        }
        response = self.session.post(self.ajax_url, data=data)
        print(f"{self.__class__.site} > Response:", response.status_code)
        return response

    def _extract_job_payloads(self, response):
        scraped_html = BeautifulSoup(response.json()["content"], "html.parser")
        tag_job_div = "td.konsultuppdrag-column-1"
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
            tags_all_span = payload.select("span")
            due_date = tags_all_span[-1].get_text(strip=True)
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
