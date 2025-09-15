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

    def read_stored_raw_data(self, file_path='../data/raw/jobs.csv'):
        raw_columns = ['site','site_id','job_title','raw_payload','ingestion_ts']

        if os.path.exists(file_path):
            stored_data = pd.read_csv(file_path)    
        else:
            stored_data = pd.DataFrame(columns=raw_columns)
        return stored_data


    def read_stored_bronze_data(self, file_path='../data/bronze/jobs.csv'):
        bronze_columns=['site', 'site_id','job_title', 'area', 'created', 'start_date', 'end_date', 'duration', 'due_date', 'work_location', 'work_type', 'link', 'ingestion_ts']

        if os.path.exists(file_path): 
            stored_data = pd.read_csv(file_path)    
        else:
            stored_data = pd.DataFrame(columns=bronze_columns)
        return stored_data
    


    def return_new_rows(self, new_data, old_data, key_columns=['site', 'site_id']): 
   
        if len(old_data)==0: 
            old_data = pd.DataFrame(columns=new_data.columns)

        for key_column in key_columns:
            new_data[key_column] = new_data[key_column].astype(str)
            old_data[key_column] = old_data[key_column].astype(str)
            
        diff = pd.merge(new_data, old_data[key_columns], on=key_columns, how="left", indicator=True)
        new_rows = diff.loc[diff["_merge"] == "left_only", new_data.columns]
        return new_rows

    def unload_data(self, file_path, new_data):
        
        if os.path.exists(file_path):
            stored_data = pd.read_csv(file_path)    
        else:
            stored_data = pd.DataFrame(columns=new_data.columns)

        updated_data = pd.concat([stored_data, new_data], ignore_index=True)
        updated_data.to_csv(file_path, index=False)    # 4. Save back to CSV
        
        print(f'{self.__class__.site} > Unloading data to {file_path}. Nmr of new added jobs:{len(new_data)}')

        return 
    

    def load_last_added_raw_data(self):
        raw_file_path = "../data/raw/jobs.csv"
        bronze_file_path = "../data/bronze/jobs.csv"

        raw_data_full = self.read_stored_raw_data()
        new_data = raw_data_full.loc[raw_data_full['site']==self.__class__.site].copy()
     
        old_data = self.read_stored_bronze_data()
        new_rows = self.return_new_rows(new_data=new_data, old_data=old_data)
        
        print(f'{self.__class__.site} > Loading last scraped jobs, nr: {len(new_rows)}')
        return new_rows
