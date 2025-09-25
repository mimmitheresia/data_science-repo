from abc import ABC, abstractmethod
import pandas as pd
import os
from datetime import datetime

class AbstractScraper(ABC):
    bronze_columns=['id', 'site', 'site_id','job_title', 'area', 'due_date', 'work_location', 'work_type', 'link', 'ingestion_ts', 'is_new']
    payload_columns = ['id', 'raw_payload']

    @abstractmethod 
    def request_status(self):
        pass

    @abstractmethod
    def scrape_jobs_payloads_dict(self, response):
        pass

    @abstractmethod
    def parse_bronze_data(self, raw_data):
        pass


    def return_new_rows(self, new_data, old_data, key_column='id'): 
   
        if len(old_data)==0: 
            old_data = pd.DataFrame(columns=new_data.columns)

        new_raw_data = new_data[~new_data[key_column].astype(str).isin(old_data[key_column].astype(str))].copy()
        new_raw_data.loc[:,'is_new'] = True

        print(f'{self.site} > Nmr of new adds:', len(new_raw_data))
        return new_raw_data
    
    
    def return_new_payloads(self, new_dict, old_dict, key_column='id'): 
   
        new_ads = {id: payload for id, payload in new_dict.items() if id not in old_dict}
        return new_ads
    

    def set_dtypes(data):
        for column in data.columns:
            if column == 'ingestion_ts':
                data['ingestion_ts'] = pd.to_datetime(data['ingestion_ts'], errors='coerce')
            elif column == 'is_new':
                data['is_new'] = data['is_new'].astype(bool)
            else:
                # Remove semicolons in string
                data[column] = data[column].astype(str)
                data.loc[:, column] = data.loc[:, column].apply(
                    lambda x: x.replace(";", "") if isinstance(x, str) else x
                )
        return data

    
    def concat_new_rows(self, new_data, old_data): 
        if len(old_data)==0: 
            old_data = pd.DataFrame(columns=new_data.columns)
        
        # Update is_new adds column 
        new_data['is_new'] = True
        old_data.loc[old_data['site'] == self.__class__.site, 'is_new'] = False
        
        new_data = AbstractScraper.set_dtypes(new_data)
        old_data = AbstractScraper.set_dtypes(old_data)
        
        updated_data = pd.concat([old_data, new_data], ignore_index=True)
        # If ingestion_ts should be a timestamp, convert it explicitly   
        return updated_data
    

    def concat_dicts(self, new_dict, old_dict):
        updated_dict = old_dict.copy()
        updated_dict.update(new_dict) 
        return updated_dict
     

    def scrape_all_jobs(self, job_payloads):
        scraped_data = pd.DataFrame(columns=AbstractScraper.bronze_columns + ['raw_payload'])

        for payload in job_payloads:
            id = self.extract_id(payload)
            site = self.site
            site_id = self.extract_site_id(payload)
            job_title = self.extract_job_title(payload)
            area = self.extract_area(payload)
            due_date = self.extract_due_date(payload)
            work_location = self.extract_work_location(payload)
            work_type = self.extract_work_type(payload)
            link = self.extract_link(payload)
            ingestion_ts = self.extract_ingestion_ts()
            is_new = False
            raw_payload = str(payload)

            if None in [site_id, job_title]:
                print(f'{self.site} > Failed to parse payload. [site_id, job_title] {[site_id,job_title]}')
            else: 
                scraped_data.loc[len(scraped_data)] = [id, site, site_id, job_title, area, due_date, work_location, work_type, link, ingestion_ts, is_new, raw_payload]
        
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



    
