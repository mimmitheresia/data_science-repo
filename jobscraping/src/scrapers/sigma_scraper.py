from src.scrapers.abstract_scraper import AbstractScraper
import requests
import pandas as pd
from datetime import datetime
import os
import json
import ast
    


class SigmaScraper(AbstractScraper):
    site = 'Sigma'
    def __init__(self):
        self.site = 'Sigma'

    def request_status(self):
        url = 'https://www.sigma.se/service/jobs.json?limit=53&type=assignment&language=sv&nocache=202509231400'
        response = requests.get(url)
        print(f'{self.__class__.site} > Response:', response.status_code)
        return response
        
    def extract_job_payloads(self, response):
        scraped_data = response.json()   # parse till Python-dict
        job_payloads = scraped_data["items"] 
        
        print(f'{self.site} > Nmr of scraped adds:', len(job_payloads))   
        return job_payloads 


    def extract_id(self, payload):
        try:
            site_id = self.extract_site_id(payload) 
            return f'{self.site}-{site_id}'
        except: return None


    def extract_site_id(self, payload):
        try: return self.extract_link(payload)
        except: return None


    def extract_job_title(self, payload):
        try: return payload['headline']
        except: return None
                

    def extract_area(self, payload):
        try: return payload["tags"]
        except: return None
      

    def extract_due_date(self, payload):
        try: return payload['expire']
        except: return None
    

    def extract_work_location(self, payload):
        try: return payload["location"]
        except: return None


    def extract_link(self, payload):
        try: return f'https://www.sigma.se/{payload['url']}' 
        except: None
        
        
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
            scraped_data.loc[len(scraped_data)] = [id, site, site_id, job_title, area, due_date, work_location, work_type, link, ingestion_ts, is_new, raw_payload]
        
        return scraped_data
    

    def scrape_jobs_payloads_dict(self, response):
        scraped_data = response.json()   # parse till Python-dict
        job_posts = scraped_data["items"]
        print(f'{self.__class__.site} > Nmr of scraped adds:', len(job_posts))

        job_payloads = {}
        for job in job_posts:
            site = SigmaScraper.site
            site_id = f'https://www.sigma.se/{job['url']}'
            id = f'{site}-{site_id}'
            job_payloads[id] = str(job)
           
        return job_payloads
    

    def parse_bronze_data(self, new_payloads):
        bronze_data = pd.DataFrame(columns=AbstractScraper.bronze_columns)
        
        for id, payload in new_payloads.items():
          
            site = SigmaScraper.site
            payload = ast.literal_eval(payload)
            
            site_id = id.replace(f"{SigmaScraper.site}-", "")
            job_title = payload['headline']
            area = payload['tags']
            due_date = payload['expire']
            work_location = payload['location']
            work_type = None
            link = site_id
            ingestion_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # <--- timestamp här
            is_new = True
    
           
            bronze_data.loc[len(bronze_data)] = [id, site, site_id, job_title, area, due_date, work_location, work_type, link, ingestion_ts, is_new]
        
        print(f'{self.__class__.site} > Parsing bronze data:', len(bronze_data))
        return bronze_data
    

        

        