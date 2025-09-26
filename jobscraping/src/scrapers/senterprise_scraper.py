from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from src.scrapers.abstract_scraper import AbstractScraper


class SenterpriseScraper(AbstractScraper):
    site = "Senterprise"

    def __init__(self):
        self.site = "Senterprise"

    def request_status(self):
        url = "https://jobb.senterprise.se/jobs?department_id=6559"
        response = requests.get(url)
        print(f"{self.__class__.site} > Response:", response.status_code)
        return response

    def _extract_job_payloads(self, response):
        scraped_html = BeautifulSoup(response.text, "html.parser")
        jobs_container = scraped_html.find("ul", id="jobs_list_container")
        job_payloads = (
            jobs_container.find_all("li", class_="w-full") if jobs_container else []
        )

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
            tag_title_span = payload.find("a", href=True)
            job_title = (
                (tag_title_span.get_text(strip=True))
                .replace("Konsultuppdrag | ", "")
                .strip("")
            )
            return job_title
        except:
            return None

    def extract_work_location(self, payload):
        try:
            tags_all_span = payload.select("div.mt-1.text-md span")
            return tags_all_span[2].get_text(strip=True)
        except:
            None

    def extract_work_type(self, payload):
        try:
            tags_all_span = payload.select("div.mt-1.text-md span")
            return tags_all_span[4].get_text(strip=True)
        except:
            None

    def extract_link(self, payload):
        try:
            tag_link = payload.find("a", href=True)
            return tag_link["href"]
        except:
            None
