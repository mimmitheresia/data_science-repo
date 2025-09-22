from src.scrapers.abstract_scraper import AbstractScraper
import requests
import pandas as pd
from datetime import datetime
import os
import json
import ast
    

class TingentScraper(AbstractScraper):
    site = 'Tingent'

    def request_status(self):
        url = "https://tingent.se/api/jobs"

        headers = {
            "accept": "application/json, text/plain, */*",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
            "referer": "https://tingent.se/sv/jobs",
        }

        response = requests.get(url, headers=headers)
        return response
        
    
    def scrape_jobs_payloads_dict(self, response):
        scraped_data = response.json()   # parse till Python-dict
        job_posts = scraped_data["data"]
        print(f'{self.__class__.site} > Nmr of scraped adds:', len(job_posts))

        job_payloads = {}
        for job in job_posts:
            site = TingentScraper.site
            id = f'{site}-{job['abstract_id']}'
            job_payloads[id] = str(job)
        
        return job_payloads
    

    def parse_bronze_data(self, new_payloads):
        bronze_data = pd.DataFrame(columns=AbstractScraper.bronze_columns)
        
        for id, payload in new_payloads.items():
          
            site = TingentScraper.site
            payload = ast.literal_eval(payload)
            
            site_id = payload['abstract_id']
            job_title = payload['requisition_name']
            area = payload['requisition_servicecategoryid']
           
            due_date = payload['requisition_offerduedate']
            work_location = payload['requisition_locationid']
            work_type = None
            link = 'https://tingent.se/sv/jobs'
            ingestion_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # <--- timestamp här
            is_new = True
           
            bronze_data.loc[len(bronze_data)] = [id, site, site_id, job_title, area, due_date, work_location, work_type, link, ingestion_ts, is_new]
        
        print(f'{self.__class__.site} > Parsing bronze data:', len(bronze_data))
        return bronze_data
    

        

        