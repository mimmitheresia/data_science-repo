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
    

    def set_dtypes(data): 
        for column in data.columns:
            if column == 'ingestion_ts': 
                data['ingestion_ts'] = pd.to_datetime(
                    data['ingestion_ts'],
                    errors='coerce'
                )
            else: 
                data[column] = data[column].apply(
                    lambda x: str(x) if x is not None else ''
                )
        return data
    
    
    def concat_new_rows(self, new_data, old_data): 
        if len(old_data)==0: 
            old_data = pd.DataFrame(columns=new_data.columns)
        
        new_data = AbstractScraper.set_dtypes(new_data)
        old_data = AbstractScraper.set_dtypes(old_data)
        
        updated_data = pd.concat([old_data, new_data], ignore_index=True)

        # If ingestion_ts should be a timestamp, convert it explicitly
            
        return updated_data



    
