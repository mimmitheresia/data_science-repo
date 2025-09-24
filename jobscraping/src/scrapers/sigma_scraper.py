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
    

        

        