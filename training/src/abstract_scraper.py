from abc import ABC, abstractmethod
from pandas import pd
import os

class AbstractScraper(ABC):
    @abstractmethod 
    def request_status(self):
        pass

    @abstractmethod
    def return_raw_job_posts_data(self, response):
        pass

    @abstractmethod
    def parse_raw_data(self, job_posts): 
        pass 


    @abstractmethod
    def parse_bronze_data(self, raw_data):
        pass

    def unload_data(self, file_path, data):
        if os.path.exists(file_path):
            existing_data = pd.read_csv(file_path)
            dublicated_data = pd.concat([existing_data, data], ignore_index=True)
        else:
            dublicated_data = data.copy()

        updated_data = dublicated_data.drop_duplicates(subset=["site", "site_id", "job_title"], keep="first")
        updated_data.to_csv(file_path, index=False)    # 4. Save back to CSV
        return

