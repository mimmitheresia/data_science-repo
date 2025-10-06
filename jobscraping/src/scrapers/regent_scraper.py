from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from src.scrapers.abstract_scraper import AbstractScraper
from src.utils import return_regex_string_match, slugify_title_for_link


class RegentScraper(AbstractScraper):
    site = "Regent"

    def __init__(self):
        self.site = "Regent"

    def _request_status(self):
        url = "https://regent.se/uppdrag/"
        headers = {}

        response = requests.get(url, headers=headers)
        print(f"{self.__class__.site} > Response:", response.status_code)
        return response

    def _extract_job_payloads(self, response):
        scraped_html = BeautifulSoup(response.text, "html.parser")
        tag_job_div = "div.assignment-item"
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
            tag_site_id = payload.select_one("a.btn.btn-warning.visa-desktop")
            site_id = tag_site_id.get("href") if tag_site_id else ""
            return site_id
        except:
            return None

    def extract_job_title(self, payload):
        try:
            tag_title = payload.select_one("a.blue > strong")
            job_title = tag_title.get_text(strip=True) if tag_title else ""
            return job_title
        except:
            return None

    def extract_area(self, payload):
        try:
            tag_area = payload.select_one("div.summary")
            area = tag_area.get_text(strip=True) if tag_area else ""
            return area
        except:
            None

    def extract_work_location(self, payload):
        try:
            div_location = payload.find("strong", string="Ort:").find_next_sibling(
                "div"
            )
            work_location = div_location.get_text(strip=True) if div_location else ""
            return work_location
        except:
            None

    def extract_link(self, payload):
        try:
            site_id = self.extract_site_id(payload)
            link = "https://regent.se" + site_id
            return link
        except:
            None
