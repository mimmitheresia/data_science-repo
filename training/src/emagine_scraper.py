from src.abstract_scraper import AbstractScraper
from src.utils import slugify_title_for_link
import requests
import pandas as pd
from datetime import datetime
import os
import json
import ast
    

class EmagineScraper(AbstractScraper):
    site = 'Emagine'

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
    

    def return_raw_job_posts_data(self, response):
        data = response.json()   # parse till Python-dict
        # alla annonser
        job_posts = data["items"]
        print(f'{self.__class__.site} > Nmr of scraped adds:', len(job_posts))
        return job_posts


    def parse_raw_data(self, job_posts):
        raw_data = pd.DataFrame(columns=['site', 'site_id','job_title', 'raw_payload', 'ingestion_ts'])
        
        for job in job_posts:
            site = EmagineScraper.site
            site_id = job['id']
            job_title = job['title']
            ingestion_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # <--- timestamp här
            raw_data.loc[len(raw_data)] = [site, site_id, job_title, job, ingestion_ts]
         
        return raw_data
    

    def parse_bronze_data(self, last_raw_data):
        bronze_data = pd.DataFrame(columns=['site', 'site_id','job_title', 'area', 'created', 'start_date', 'end_date', 'duration', 'due_date', 'work_location', 'work_type', 'link', 'ingestion_ts'])
        
        for idx, row in last_raw_data.iterrows():
            site = row['site']
            site_id = row['site_id']
            job_title = row['job_title']
        
            payload = ast.literal_eval(row['raw_payload'])
            try: 
                area = payload["area"]["name"]
            except: 
                area = None

            created = None 
            start_date = payload["startDate"]
            end_date = None 
            duration = payload["duration"]
            due_date = payload['applicationDate']

            work_location = payload["jobAdWorkLocation"]["city"]
            work_type = payload["jobAdWorkLocation"]["workLocationType"]
            link = f'https://portal.emagine.org/jobs/{site_id}/{slugify_title_for_link(job_title)}' 
            ingestion_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # <--- timestamp här

            bronze_data.loc[len(bronze_data)] = [site, site_id, job_title, area, created, start_date, end_date, duration, due_date, work_location, work_type, link, ingestion_ts]
        print(f'{self.__class__.site} > Parsing bronze data:', len(bronze_data))
        return bronze_data

        

        
        