from abc import ABC, abstractmethod
import pandas as pd
import os

class AbstractScraper(ABC):
    bronze_columns=['site', 'site_id','job_title', 'area', 'created', 'start_date', 'end_date', 'duration', 'due_date', 'work_location', 'work_type', 'link', 'raw_payload', 'ingestion_ts']

    @abstractmethod 
    def request_status(self):
        pass

    @abstractmethod
    def return_raw_job_posts_data(self, response):
        pass

    @abstractmethod
    def parse_bronze_data(self, raw_data):
        pass



    def return_new_rows(self, new_data, old_data, key_columns=['site', 'site_id']): 
   
        if len(old_data)==0: 
            old_data = pd.DataFrame(columns=new_data.columns)

        for key_column in key_columns:
            new_data[key_column] = new_data[key_column].astype(str)
            old_data[key_column] = old_data[key_column].astype(str)
            
        diff = pd.merge(new_data, old_data[key_columns], on=key_columns, how="left", indicator=True)
        new_rows = diff.loc[diff["_merge"] == "left_only", new_data.columns]
        return new_rows
    

    def load_bronze_data(self, file_path='../data/bronze/jobs.csv'):
        

        if os.path.exists(file_path): 
            stored_data = pd.read_csv(file_path, index_col=0)    
        else:
            stored_data = pd.DataFrame(columns=AbstractScraper.bronze_columns)
        return stored_data


    def unload_bronze_data(self, file_path, new_data):
        
        if os.path.exists(file_path):
            stored_data = pd.read_csv(file_path)    
        else:
            stored_data = pd.DataFrame(columns=AbstractScraper.bronze_columns)

        updated_data = pd.concat([stored_data, new_data], ignore_index=True)
        updated_data.to_csv(file_path, index=False)    # 4. Save back to CSV
        
        print(f'{self.__class__.site} > Unloading {len(new_data)} new ads to {file_path}...')

        return 
    
