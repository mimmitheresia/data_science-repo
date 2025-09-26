from abc import ABC, abstractmethod
from datetime import datetime

import pandas as pd


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

    @abstractmethod
    def request_status(self):
        pass

    @abstractmethod
    def _extract_job_payloads(self, response):
        pass

    def extract_job_payloads(self, response):
        """Template method: wraps subclass extraction with error handling."""
        try:
            return self._extract_job_payloads(response)
        except Exception as e:
            print(f"{self.site} > Failed to extract job payloads: {e}")
            job_payloads = []
            return job_payloads

    def scrape_all_jobs(self, job_payloads):
        scraped_data = pd.DataFrame(
            columns=AbstractScraper.bronze_columns + ["raw_payload"]
        )

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
                self.extract_ingestion_ts(),
                False,
                str(payload),
            ]

            if self.is_valid_scraped_row(job_row, scraped_data):
                scraped_data.loc[len(scraped_data)] = job_row
        return scraped_data

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

    def extract_ingestion_ts(self):
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def is_valid_scraped_row(self, row, scraped_data):
        index_id = AbstractScraper.bronze_columns.index("id")
        index_site_id = AbstractScraper.bronze_columns.index("site_id")
        index_title = AbstractScraper.bronze_columns.index("job_title")

        id = row[index_id]
        site_id = row[index_site_id]
        job_title = row[index_title]

        if None in [site_id, job_title]:
            print(
                f"{self.site} > Failed to add job into data due to invalid parsing of 'site_id' or 'job_title' from payload. Resulting [site_id, job_title] = {[site_id,job_title]}"
            )
            return False

        if id in scraped_data["id"].values:
            print(
                f"{self.site} > Failed to add job into data due to non-unique 'id' from payload. Resulting 'id' = {id}"
            )
            return False

        return True

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
