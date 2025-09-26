from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from src.scrapers.abstract_scraper import AbstractScraper
from src.utils import return_regex_string_match, slugify_title_for_link


class CombitechScraper(AbstractScraper):
    site = "Combitech"

    def __init__(self):
        self.site = "Combitech"

    def request_status(self):
        url = "https://www.combitech.se/karriar/lediga-jobb/"
        headers = {}

        response = requests.get(url, headers=headers)
        print(f"{self.__class__.site} > Response:", response.status_code)
        return response

    def _extract_job_payloads(self, response):
        scraped_html = BeautifulSoup(response.text, "html.parser")
        tag_job_div = "div.block.w-full.mb-4.md\\:pb-0.md\\:mb-0.lg\\:pb-4"
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
            tag_site_id = payload.select_one("a.cursor-pointer")
            site_id = (
                tag_site_id.get("onclick", "")
                .replace("location.href=", "")
                .replace("'", "")
            )
            return site_id
        except:
            return None

    def extract_job_title(self, payload):
        try:
            tag_title = payload.select_one("#job-title")
            job_title = tag_title.get_text(strip=True) if tag_title else ""
            return job_title
        except:
            return None

    def extract_area(self, payload):
        try:
            tag_area = payload.select_one("#job-type")
            area = tag_area.get("data-value", "").strip() if tag_area else ""
            return area
        except:
            None

    def extract_due_date(self, payload):
        try:
            tag_due_date = payload.select_one("h5.font-normal")
            if tag_due_date:
                txt = tag_due_date.get_text(strip=True)
                if "Sista ansökningsdag:" in txt:
                    due_date = txt.replace("Sista ansökningsdag:", "").strip()
                    return due_date
        except:
            None

    def extract_work_location(self, payload):
        try:
            tag_location = payload.select_one("#job-locations")
            work_location = tag_location.get_text(strip=True) if tag_location else ""
            return work_location
        except:
            None

    def extract_link(self, payload):
        try:
            site_id = self.extract_site_id(payload)
            link = "https://www.combitech.se" + site_id
            return link
        except:
            None
