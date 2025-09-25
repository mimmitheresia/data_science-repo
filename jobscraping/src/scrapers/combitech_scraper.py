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

    def extract_job_payloads(self, response):
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

    def scrape_jobs_payloads_dict(self, response):
        tag_job_div = "div.block.w-full.mb-4.md\\:pb-0.md\\:mb-0.lg\\:pb-4"
        scraped_html = BeautifulSoup(response.text, "html.parser")
        job_posts = scraped_html.select(tag_job_div)
        print(f"{self.__class__.site} > Nmr of scraped adds:", len(job_posts))

        job_payloads = {}
        for job in job_posts:
            tag_site_id = job.select_one("a.cursor-pointer")
            site = CombitechScraper.site

            site_id = (
                tag_site_id.get("onclick", "")
                .replace("location.href=", "")
                .replace("'", "")
            )
            id = f"{site}-{site_id}"
            job_payloads[id] = str(job)

        return job_payloads

    def parse_bronze_data(self, new_payloads):
        bronze_data = pd.DataFrame(columns=AbstractScraper.bronze_columns)

        for id, payload in new_payloads.items():

            site = CombitechScraper.site

            payload = BeautifulSoup(payload, "html.parser")
            tag_site_id = payload.select_one("a.cursor-pointer")
            tag_title = payload.select_one("#job-title")
            tag_area = payload.select_one("#job-type")
            tag_due_date = payload.select_one("h5.font-normal")
            tag_location = payload.select_one("#job-locations")

            site_id = (
                tag_site_id.get("onclick", "")
                .replace("location.href=", "")
                .replace("'", "")
            )
            job_title = tag_title.get_text(strip=True) if tag_title else ""
            area = tag_area.get("data-value", "").strip() if tag_area else ""
            due_date = None

            work_location = tag_location.get_text(strip=True) if tag_location else ""
            work_type = None
            link = "https://www.combitech.se" + site_id
            ingestion_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            is_new = True

            if tag_due_date:
                txt = tag_due_date.get_text(strip=True)
                if "Sista ansökningsdag:" in txt:
                    due_date = txt.replace("Sista ansökningsdag:", "").strip()

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
