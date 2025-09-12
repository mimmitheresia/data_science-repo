from src.abstract_scraper import AbstractScraper
import requests
import pandas as pd
from datetime import datetime
import os
import json
import ast
    

class AliantScraper(AbstractScraper):
    site = 'Aliant'

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
        print('Aliant response:', response.status_code)
        return response
    

    def return_raw_job_posts_data(self, response):
        data = response.json()   # parse till Python-dict
        # alla annonser
        job_posts = data["data"]["job_posts"] 
        return job_posts


    def parse_raw_data(self, job_posts):
        raw_data = pd.DataFrame(columns=['site', 'site_id','job_title', 'raw_payload', 'ingestion_ts'])
        
        for job in job_posts:
            site = AliantScraper.site
            site_id = job['AdID']
            job_title = job['Name']
            ingestion_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # <--- timestamp här
            raw_data.loc[len(raw_data)] = [site, site_id, job_title, job, ingestion_ts]
         
        return raw_data
    
    def read_last_raw_data(self): 
        raw_file_path = "../data/raw/jobs.csv"
        bronze_file_path = "../data/bronze/jobs.csv"

        raw_data = pd.read_csv(raw_file_path)

        if not os.path.exists(bronze_file_path):
            last_data = raw_data.copy()
        
        else: 
            bronze_data = pd.read_csv(bronze_file_path)

            if "ingestion_ts" not in raw_data.columns:
                raise ValueError("raw_data must contain column 'ingestion_ts'")
            if "ingestion_ts" not in bronze_data.columns:
                raise ValueError("bronze_data must contain column 'ingestion_ts'")

            # Ensure timestamps are datetime
            raw_data["ingestion_ts"] = pd.to_datetime(raw_data["ingestion_ts"])
            bronze_data["ingestion_ts"] = pd.to_datetime(bronze_data["ingestion_ts"])

            # Keep only rows newer than max_ts
            last_data = raw_data[raw_data["ingestion_ts"] > bronze_data["ingestion_ts"].max()].copy()

        return last_data



    def parse_bronze_data(self, last_raw_data):
        bronze_data = pd.DataFrame(columns=['site', 'site_id','job_title', 'area', 'created', 'start_date', 'end_date', 'duration', 'due_date', 'work_location', 'work_type', 'link', 'ingestion_ts'])
        
        for idx, row in last_raw_data.iterrows():
            site = row['site']
            site_id = row['site_id']
            job_title = row['job_title']
        
            payload = ast.literal_eval(row['raw_payload'])
            area = None 
            created = payload['Created']
            start_date = None 
            end_date = None 
            duration = None 
            due_date = payload['Expire']

            work_location = payload['Place']
            work_type = payload['WorkType']
            link = f'https://aliant.recman.page/job/{site_id}'
            ingestion_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # <--- timestamp här

            bronze_data.loc[len(bronze_data)] = [site, site_id, job_title, area, created, start_date, end_date, duration, due_date, work_location, work_type, link, ingestion_ts]
       
        return bronze_data

        

        
        