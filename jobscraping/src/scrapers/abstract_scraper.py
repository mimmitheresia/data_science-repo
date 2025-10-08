from abc import ABC, abstractmethod
from datetime import datetime

import pandas as pd
import pytz


class AbstractScraper(ABC):
    bronze_columns = [
        "id",
        "site",
        "site_id",
        "job_title",
        "area",
        "due_date",
        "work_location",
        "work_type",
        "link",
        "ingestion_ts",
        "is_new",
    ]
    payload_columns = ["id", "raw_payload"]

    def __init__(self):
        self.is_failed = False
        self.failed_message = ""

    def request_status(self):
        """Template method: wraps subclass extraction with error handling."""
        try:
            return self._request_status()
        except Exception as e:
            print(f"{self.site} > Request failed: {e}")
            return None

    @abstractmethod
    def _request_status(self):
        pass

    def extract_job_payloads(self, response):
        """Template method: wraps subclass extraction with error handling."""
        try:
            return self._extract_job_payloads(response)
        except Exception as e:
            job_payloads = []
            self.is_failed = True
            self.failed_message = "Failed to extract any jobs from website."
            print(f"{self.site} > {self.failed_message}: {e}")

            return job_payloads

    @abstractmethod
    def _extract_job_payloads(self, response):
        pass

    def scrape_all_jobs(self, job_payloads):
        scraped_data = pd.DataFrame(
            columns=AbstractScraper.bronze_columns + ["raw_payload"]
        )
        if job_payloads == []:
            return scraped_data

        for payload in job_payloads:
            job_row = [
                self.extract_id(payload),
                self.site,
                self.extract_site_id(payload),
                self.extract_job_title(payload),
                self.extract_area(payload),
                self.extract_due_date(payload),
                self.extract_work_location(payload),
                self.extract_work_type(payload),
                self.extract_link(payload),
                AbstractScraper.extract_ingestion_ts(),
                False,
                str(payload),
            ]
            scraped_data.loc[len(scraped_data)] = job_row

        valid_scraped_data = self.return_valid_scraped_data(scraped_data)
        return valid_scraped_data

    def extract_id(self, payload):
        return None

    def extract_site_id(self, payload):
        return None

    def extract_job_title(self, payload):
        return None

    def extract_area(self, payload):
        return None

    def extract_due_date(self, payload):
        return None

    def extract_work_location(self, payload):
        return None

    def extract_work_type(self, payload):
        return None

    def extract_link(self, payload):
        return None

    def extract_ingestion_ts():
        tz = pytz.timezone("Europe/Stockholm")
        return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

    def return_valid_scraped_data(self, scraped_data):

        valid_scraped_data = scraped_data[scraped_data["site_id"].notna()].reset_index(
            drop=True
        )
        if len(valid_scraped_data) == 0:
            self.is_failed = True
            self.failed_message = "Failed to parse 'site_id' from jobs."
            return valid_scraped_data

        valid_scraped_data = valid_scraped_data[
            scraped_data["job_title"].notna()
        ].reset_index(drop=True)
        if len(valid_scraped_data) == 0:
            self.is_failed = True
            self.failed_message = "Failed to parse 'job_title' from jobs."

        return valid_scraped_data

    def update_failed_scrapers_data(self, failed_scrapers_data):
        if self.is_failed:
            # Failed scraper: 0 jobs was scraped from website. Adding failed scraper do table if not already exists
            if self.site not in failed_scrapers_data["site"].values:
                failed_scrapers_data.loc[len(failed_scrapers_data)] = [
                    self.site,
                    self.failed_message,
                    AbstractScraper.extract_ingestion_ts(),
                ]

        else:
            # Succeed scraper: jobs was scraped from website. Removing scraper from failed table if exists
            failed_scrapers_data = failed_scrapers_data[
                failed_scrapers_data["site"] != self.site
            ].reset_index(drop=True)
        return failed_scrapers_data

    def return_new_rows(self, new_data, old_data, key_column="id"):

        if len(old_data) == 0:
            old_data = pd.DataFrame(columns=new_data.columns)

        new_raw_data = new_data[
            ~new_data[key_column].astype(str).isin(old_data[key_column].astype(str))
        ].copy()
        new_raw_data.loc[:, "is_new"] = True

        print(f"{self.site} > Nmr of new adds:", len(new_raw_data))
        return new_raw_data

    def set_dtypes(data):
        for column in data.columns:
            if column == "ingestion_ts":
                data["ingestion_ts"] = pd.to_datetime(
                    data["ingestion_ts"], errors="coerce"
                )
            elif column == "is_new":
                data["is_new"] = data["is_new"].astype(bool)
            else:
                # Remove semicolons in string
                data[column] = data[column].astype(str)
                data.loc[:, column] = data.loc[:, column].apply(
                    lambda x: x.replace(";", "") if isinstance(x, str) else x
                )
        return data
