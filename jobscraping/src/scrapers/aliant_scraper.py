from src.scrapers.abstract_scraper import AbstractScraper
import requests
import pandas as pd
from datetime import datetime
import os
import json
import ast
    

class AliantScraper(AbstractScraper):
    site = 'Aliant'

    def __init__(self): 
        self.site = 'Aliant'

    def request_status(self):
        url = "https://aliant.recman.page/api/jobs?sort=newest"


        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://aliant.recman.page/jobs?sort=newest",
            "X-CSRF-TOKEN": "694-1334-250910123734-95bda24a27252a6ddffd471f9e57c22b8e41f4600506a059f483cbde2f2a1328b748",   # if you see this in Network, copy it too
            "Cookie": "recman=drd5t8o7u0h47bo4sotqccb6br0951sk; organisation=aliant.recman.page%3B694%3B1334%3B1334; axe_xs=694-1334-250910123734-95bda24a27252a6ddffd471f9e57c22b8e41f4600506a059f483cbde2f2a1328b748; cookie_accept=0%3B1" #sometimes Recman requires session cookies
        }

        response = requests.get(url, headers=headers)
        print(f'{self.__class__.site} > Response:', response.status_code)
        return response
    

    def extract_job_payloads(self, response):
        scraped_data = response.json()   # parse till Python-dict
        job_payloads = scraped_data["data"]["job_posts"] 
        
        print(f'{self.site} > Nmr of scraped adds:', len(job_payloads))   
        return job_payloads 


    def extract_id(self, payload):
        try:
            site_id = self.extract_site_id(payload) 
            return f'{self.site}-{site_id}'
        except: return None


    def extract_site_id(self, payload):
        try: return payload['AdID']
        except: return None


    def extract_job_title(self, payload):
        try: return payload['Name']
        except: return None
                

    def extract_due_date(self, payload):
        try: return payload['Expire']
        except: return None
      

    def extract_work_location(self, payload):
        try: return payload['Place']
        except: return None
    
    
    def extract_work_type(self, payload):
        try: return payload['WorkType']
        except: return None


    def extract_link(self, payload):
        try:
            site_id = self.extract_site_id(payload)
            link = f'https://aliant.recman.page/job/{site_id}'    
            return link
        except: None
        
    

    def scrape_jobs_payloads_dict(self, response):
        scraped_data = response.json()   # parse till Python-dict
        job_posts = scraped_data["data"]["job_posts"] 
        print(f'{self.__class__.site} > Nmr of scraped adds:', len(job_posts))
        
        job_payloads = {}
        for job in job_posts:
            site = AliantScraper.site
            site_id = str(job['AdID'])
            id = f'{site}-{site_id}'
            job_payloads[id] = str(job)
        
        return job_payloads
    

    def parse_bronze_data(self, new_payloads):
        bronze_data = pd.DataFrame(columns=AbstractScraper.bronze_columns)        
        
        for id, payload in new_payloads.items():

            site = AliantScraper.site
            payload = ast.literal_eval(payload)
            site_id = payload['AdID']
            job_title = payload['Name']
            area = None 
            due_date = payload['Expire']

            work_location = payload['Place']
            work_type = payload['WorkType']
            link = f'https://aliant.recman.page/job/{site_id}'
            ingestion_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # <--- timestamp här
            is_new = True

            bronze_data.loc[len(bronze_data)] = [id, site, site_id, job_title, area, due_date, work_location, work_type, link, ingestion_ts, is_new]
        print(f'{self.__class__.site} > Parsing bronze data:', len(bronze_data))
        return bronze_data

        

        
        