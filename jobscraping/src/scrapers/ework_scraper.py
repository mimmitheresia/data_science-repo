import ast
from datetime import datetime

import pandas as pd
import requests
from src.scrapers.abstract_scraper import AbstractScraper


class EworkScraper(AbstractScraper):
    site = "Ework"

    def __init__(self):
        self.site = "Ework"

    def _request_status(self):
        url = "https://app.verama.com/api/public/job-requests"

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://app.verama.com/sv/job-requests",
            "x-frontend-version": "a4c15af0",
            "x-session": "a9b80196-b28a-42a3-a83f-c1059ef946af",
        }

        # Parametrar för att hämta alla jobb (storlek 100)
        params = {
            "page": 0,
            "size": 1000,  # hämta upp till 100 jobb
            "sort": "firstDayOfApplications,DESC",
            "location.country": "Sweden",
            "location.countryCode": "SWE",
            "location.suggestedPhoneCode": "SE",
            "location.locationId": "here:cm:namedplace:20298368",
            "location.id": "NaN",
            "location.signature": "",
            "dedicated": "false",
            "favouritesOnly": "false",
            "recommendedOnly": "false",
            "query": "",
        }

        response = requests.get(url, headers=headers, params=params)
        print(f"{self.__class__.site} > Response:", response.status_code)
        return response

    def _extract_job_payloads(self, response):
        scraped_data = response.json()  # parse till Python-dict
        job_payloads = scraped_data["content"]

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
            return payload["id"]
        except:
            return None

    def extract_job_title(self, payload):
        try:
            return payload["title"]
        except:
            return None

    def extract_area(self, payload):
        try:
            area = ""
            for skills_dict in payload["skills"]:
                skill = skills_dict["skill"]["name"]
                area += f"{skill}, "
            return area.strip(", ")

        except:
            return None

    def extract_due_date(self, payload):
        try:
            return payload["lastDayOfApplications"]
        except:
            return None

    def extract_work_location(self, payload):
        work_location = ""
        try:
            for location_dict in payload["locations"]:
                location = location_dict["city"]
                work_location += f"{location}, "
            return work_location.strip(", ")
        except:
            return None

    def extract_work_type(self, payload):
        work_type = ""
        try:
            remoteness = payload["remoteness"]
            if remoteness == 0:
                work_type = "På plats"
            elif remoteness == 100:
                work_type = "Remote"
            else:
                work_type = "Hybrid"
            return work_type
        except:
            return None

    def extract_link(self, payload):
        try:
            site_id = self.extract_site_id(payload)
            link = f"https://app.verama.com/sv/job-requests/{site_id}"
            return link
        except:
            None
