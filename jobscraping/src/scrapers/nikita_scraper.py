from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from src.scrapers.abstract_scraper import AbstractScraper
from src.utils import return_regex_string_match, slugify_title_for_link


class NikitaScraper(AbstractScraper):
    site = "Nikita"

    def __init__(self):
        self.site = "Nikita"

    def request_status(self):
        url = "https://www.nikita.se/lediga-uppdrag/"
        headers = {}

        response = requests.get(url, headers=headers)
        print(f"{self.__class__.site} > Response:", response.status_code)
        return response

    def extract_job_payloads(self, response):
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

    def scrape_jobs_payloads_dict(self, response):
        tag_job_div = "li.open-position-item.opened"
        scraped_html = BeautifulSoup(response.text, "html.parser")
        job_posts = scraped_html.select(tag_job_div)
        print(f"{self.__class__.site} > Nmr of scraped adds:", len(job_posts))

        job_payloads = {}
        for job in job_posts:
            tag_site_id = job.select_one("a.open-position-list-link")

            site = NikitaScraper.site
            site_id = tag_site_id.get("href") if tag_site_id else ""
            id = f"{site}-{site_id}"
            job_payloads[id] = str(job)

        return job_payloads

    def parse_bronze_data(self, new_payloads):
        bronze_data = pd.DataFrame(columns=AbstractScraper.bronze_columns)

        for id, payload in new_payloads.items():

            site = NikitaScraper.site

            payload = BeautifulSoup(payload, "html.parser")
            tag_site_id = payload.select_one("a.open-position-list-link")
            tag_title = payload.select_one("span.open-position-title")

            site_id = tag_site_id.get("href") if tag_site_id else ""
            job_title = tag_title.get_text(strip=True) if tag_title else ""
            area = None
            due_date = None

            work_location = None
            work_type = None
            link = site_id
            ingestion_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            is_new = True

            bronze_data.loc[len(bronze_data)] = [
                id,
                site,
                site_id,
                job_title,
                area,
                due_date,
                work_location,
                work_type,
                link,
                ingestion_ts,
                is_new,
            ]

        print(f"{self.__class__.site} > Parsing bronze data:", len(bronze_data))
        return bronze_data
