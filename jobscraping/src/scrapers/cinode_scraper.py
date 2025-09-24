from src.scrapers.abstract_scraper import AbstractScraper
from src.utils import return_regex_string_match, slugify_title_for_link
import requests
import pandas as pd
from datetime import datetime
import os
import json, re
import ast
import pandas as pd
from bs4 import BeautifulSoup
    

class CinodeScraper(AbstractScraper):
    site = 'Cinode'

    def __init__(self):
        self.site = 'Cinode'


    def request_status(self):
        url = 'https://cinode.market/requests'
        response = requests.get(url)
        print(f'{self.__class__.site} > Response:', response.status_code)
        return response
    

    def extract_job_payloads(self, response):
        scraped_html = BeautifulSoup(response.text, "html.parser")
        job_payloads = scraped_html.find_all('app-list-row')
    
        print(f'{self.site} > Nmr of scraped adds:', len(job_payloads))   
        return job_payloads 


    def extract_id(self, payload):
        try:
            site_id = self.extract_site_id(payload) 
            return f'{self.site}-{site_id}'
        except: return None


    def extract_site_id(self, payload):   
        try: 
            return self.extract_link(payload)
        except: return None


    def extract_job_title(self, payload):
        try: 
            tag_link = payload.find("a", class_="list__heading", href=True) 
            job_title = (tag_link.get_text(strip=True)).encode('latin1').decode('utf-8')
            return job_title
        except: return None


    def extract_link(self, payload):
        try:
            tag_link = payload.find("a", class_="list__heading", href=True)
            link = f'https://cinode.market{tag_link['href']}'   
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
        scraped_html = BeautifulSoup(response.text, "html.parser")
        job_posts = scraped_html.find_all('app-list-row')
        print(f'{self.__class__.site} > Nmr of scraped adds:', len(job_posts))

        job_payloads = {}
        for job in job_posts: 
            tag_link = job.find("a", class_="list__heading", href=True)
            site = CinodeScraper.site
            site_id = f'https://cinode.market{tag_link['href']}'
            id = f'{site}-{site_id}'
            job_payloads[id] = str(job)
        
        return job_payloads
            

    def parse_bronze_data(self, new_payloads):
        bronze_data = pd.DataFrame(columns=AbstractScraper.bronze_columns)
        
        for id, payload in new_payloads.items():
            payload = BeautifulSoup(payload, "html.parser")
            tag_link = payload.find("a", class_="list__heading", href=True)   
            
            site = CinodeScraper.site
            site_id = id.replace(f"{CinodeScraper.site}-", "")
            job_title = None
            area = None 
            due_date = None 
            work_location = None 
            work_type = None 
            link = site_id
            ingestion_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
            is_new = True

            job_title = (tag_link.get_text(strip=True)).encode('latin1').decode('utf-8')
        
    
          
            bronze_data.loc[len(bronze_data)] = [id, site, site_id, job_title, area, due_date, work_location, work_type, link, ingestion_ts, is_new]
        
        print(f'{self.__class__.site} > Parsing bronze data:', len(bronze_data))
        return bronze_data
    



        

        
        