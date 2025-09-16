from src.abstract_scraper import AbstractScraper
from src.utils import return_regex_string_match, slugify_title_for_link
import requests
import pandas as pd
from datetime import datetime
import os
import json, re
import ast
import pandas as pd
from bs4 import BeautifulSoup
    

class CombitechScraper(AbstractScraper):
    site = 'Combitech'

    
    def request_status(self):
        url = "https://www.combitech.se/karriar/lediga-jobb/"
        headers = {}

        response = requests.get(url, headers=headers)
        print(f'{self.__class__.site} > Response:', response.status_code)
        return response


    def return_raw_job_posts_data(self, response):
        scraped_html = BeautifulSoup(response.text, "html.parser")
        tag_job_div = "div.block.w-full.mb-4.md\\:pb-0.md\\:mb-0.lg\\:pb-4"
        
        job_posts = scraped_html.select(tag_job_div)
        print(f'{self.__class__.site} > Nmr of scraped adds:', len(job_posts))

        raw_data = pd.DataFrame(columns=['site', 'site_id', 'raw_payload'])
        for job in job_posts: 
            tag_site_id = job.select_one("a.cursor-pointer")
            site = CombitechScraper.site
            site_id = tag_site_id.get("onclick","").replace("location.href=", "").replace("'", "")
            raw_data.loc[len(raw_data)] = [site, site_id, job]
        
        return raw_data
        
        

    def parse_raw_data(self, job_posts):
        raw_data = pd.DataFrame(columns=['site', 'site_id','job_title', 'raw_payload', 'ingestion_ts'])
        
        for job in job_posts:
            tag_title = job.select_one("#job-title")
            tag_site_id = job.select_one("a.cursor-pointer")

            site = CombitechScraper.site
            site_id = tag_site_id.get("onclick","").replace("location.href=", "").replace("'", "")
            job_title = tag_title.get_text(strip=True) if tag_title else ""
            ingestion_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 

            raw_data.loc[len(raw_data)] = [site, site_id, job_title, str(job), ingestion_ts]  
        return raw_data
    

    def parse_bronze_data(self, last_raw_data):
        bronze_data = pd.DataFrame(columns=['site', 'site_id','job_title', 'area', 'created', 'start_date', 'end_date', 'duration', 'due_date', 'work_location', 'work_type', 'link', 'raw_payload', 'ingestion_ts'])
        
        for idx, row in last_raw_data.iterrows():
            site = row['site']
            site_id = row['site_id']
        
            payload = row['raw_payload']
            payload = BeautifulSoup(payload, "html.parser")
            
            tag_title = payload.select_one("#job-title")
            tag_area = payload.select_one("#job-type")
            tag_due_date = payload.select_one("h5.font-normal")
            tag_location = payload.select_one("#job-locations")

            job_title = tag_title.get_text(strip=True) if tag_title else ""
            area = tag_area.get("data-value", "").strip() if tag_area else ""

            created = None 
            start_date = None
            end_date = None
            duration = None 
            due_date = None

            work_location = tag_location.get_text(strip=True) if tag_location else ""
            work_type = None
            link = 'https://www.combitech.se' + site_id
            ingestion_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 

            if tag_due_date:
                txt = tag_due_date.get_text(strip=True)
                if "Sista ansökningsdag:" in txt:
                    due_date = txt.replace("Sista ansökningsdag:", "").strip()



            bronze_data.loc[len(bronze_data)] = [site, site_id, job_title, area, created, start_date, end_date, duration, due_date, work_location, work_type, link, payload, ingestion_ts]
        
        print(f'{self.__class__.site} > Parsing bronze data:', len(bronze_data))
        return bronze_data
    



        

        
        