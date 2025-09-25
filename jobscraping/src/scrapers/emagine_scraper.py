from src.scrapers.abstract_scraper import AbstractScraper
from src.utils import slugify_title_for_link
import requests
import pandas as pd
from datetime import datetime
import os
import json
import ast
    

class EmagineScraper(AbstractScraper):
    site = 'Emagine'

    def __init__(self):
        self.site = 'Emagine'

    def request_status(self):
        url = "https://portal-api.emagine.org/api/JobAds/Search"

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Referer": "https://portal.emagine.org/"
        }

        payload = {
            "skipCount": 0,
            "maxResultCount": 1000,
            "sorting": "CreationTime desc",
            "filter": {
                "isPartTime": None,
                "textFilters": [],
                "workLocationTypes": [],
                "workLocations": [{"countryId": "SE", "city": "", "region": ""}],
                "professionalRolesIds": [],
                "consultantSeniorities": [],
                "languageProficiencies": [],
                "industriesIds": [],
                "recordIdsToExclude": []
            },
            "supportedLanguageId": "EN"
        }

        response = requests.post(url, headers=headers, json=payload)
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
        try: return payload['id']
        except: return None


    def extract_job_title(self, payload):
        try: return payload['title']
        except: return None
                

    def extract_area(self, payload):
        try: return payload["area"]["name"]
        except: return None
      

    def extract_due_date(self, payload):
        try: return payload['applicationDate']
        except: return None
    
    def extract_work_location(self, payload):
        try: return payload["jobAdWorkLocation"]["city"]
        except: return None
    
    def extract_work_type(self, payload):
        try: return payload["jobAdWorkLocation"]["workLocationType"]
        except: return None


    def extract_link(self, payload):
        try:
            site_id = self.extract_site_id(payload)
            job_title = self.extract_job_title(payload)
            link = f'https://portal.emagine.org/jobs/{site_id}/{slugify_title_for_link(job_title)}'    
            return link
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
        data = response.json()   # parse till Python-dict
        # alla annonser
        job_posts = data["items"]
        print(f'{self.__class__.site} > Nmr of scraped adds:', len(job_posts))

        job_payloads = {}
        for job in job_posts:
            site = EmagineScraper.site
            site_id = str(job['id'])
            id = f'{site}-{site_id}'
            job_payloads[id] = str(job)
        return job_payloads

    

    def parse_bronze_data(self, new_payloads):
        bronze_data = pd.DataFrame(columns=AbstractScraper.bronze_columns)

        
        for id, payload in new_payloads.items():
            
            site = EmagineScraper.site
            
            payload = ast.literal_eval(payload)
            site_id = str(payload['id'])
            job_title = payload['title']
            try: 
                area = payload["area"]["name"]
            except: 
                area = None

            due_date = payload['applicationDate']

            work_location = payload["jobAdWorkLocation"]["city"]
            work_type = payload["jobAdWorkLocation"]["workLocationType"]
            link = f'https://portal.emagine.org/jobs/{site_id}/{slugify_title_for_link(job_title)}' 
            ingestion_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # <--- timestamp här
            is_new = True

            bronze_data.loc[len(bronze_data)] = [id, site, site_id, job_title, area, due_date, work_location, work_type, link, ingestion_ts, is_new]
        print(f'{self.__class__.site} > Parsing bronze data:', len(bronze_data))
        return bronze_data

        

        
        