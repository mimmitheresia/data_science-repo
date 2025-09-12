from abc import ABC, abstractmethod
import pandas as pd
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

    def unload_data(self, file_path, new_data):
 
       
        if os.path.exists(file_path):
            existing_data = pd.read_csv(file_path)    
        else:
            existing_data = pd.DataFrame(columns=new_data.columns)

        dublicated_data = pd.concat([existing_data, new_data], ignore_index=True)
        updated_data = dublicated_data.drop_duplicates(subset=["site", "site_id", "job_title"], keep="first")
        updated_data.to_csv(file_path, index=False)    # 4. Save back to CSV
        
        print(f'{self.__class__.site} > Unloading data to {file_path}. Nmr of new added jobs:{len(updated_data)-len(existing_data)}')
        return
    

    def load_last_added_raw_data(self): 
        raw_file_path = "../data/raw/jobs.csv"
        bronze_file_path = "../data/bronze/jobs.csv"

        raw_data_full = pd.read_csv(raw_file_path)
        raw_data = raw_data_full.loc[raw_data_full['site']==self.__class__.site].copy() 
       

        if not os.path.exists(bronze_file_path):
            last_data = raw_data.copy()
     
        else: 
            bronze_data = pd.read_csv(bronze_file_path)

            if self.__class__.site not in bronze_data['site']: 
                last_data = raw_data.copy()
            
            else: 
                if "ingestion_ts" not in raw_data.columns:
                    raise ValueError("raw_data must contain column 'ingestion_ts'")
                if "ingestion_ts" not in bronze_data.columns:
                    raise ValueError("bronze_data must contain column 'ingestion_ts'")

                # Ensure timestamps are datetime
                raw_data["ingestion_ts"] = pd.to_datetime(raw_data["ingestion_ts"])
                bronze_data["ingestion_ts"] = pd.to_datetime(bronze_data["ingestion_ts"])
                last_data = raw_data[raw_data["ingestion_ts"] > bronze_data["ingestion_ts"].max()].copy()

        print(f'{self.__class__.site} > Loading last scraped jobs, nr: {len(last_data)}')
        return last_data

