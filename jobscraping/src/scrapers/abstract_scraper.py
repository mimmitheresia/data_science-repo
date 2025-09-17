from abc import ABC, abstractmethod
import pandas as pd
import os

class AbstractScraper(ABC):
    bronze_columns=['site', 'site_id','job_title', 'area', 'due_date', 'work_location', 'work_type', 'link', 'raw_payload', 'ingestion_ts']

    @abstractmethod 
    def request_status(self):
        pass

    @abstractmethod
    def return_raw_job_posts_data(self, response):
        pass

    @abstractmethod
    def parse_bronze_data(self, raw_data):
        pass


    def return_new_rows(self, new_data, old_data, key_column='site_id'): 
   
        if len(old_data)==0: 
            old_data = pd.DataFrame(columns=new_data.columns)

        new_raw_data = new_data[~new_data[key_column].astype(str).isin(old_data[key_column].astype(str))]
        return new_raw_data
    
    
    def concat_new_rows(self, new_data, old_data): 
        if len(old_data)==0: 
            old_data = pd.DataFrame(columns=new_data.columns)
        
        updated_data = pd.concat([old_data, new_data], ignore_index=True)

        for column in updated_data.columns: 
            updated_data[column] = updated_data[column].astype(pd.StringDtype())
        return updated_data



    
