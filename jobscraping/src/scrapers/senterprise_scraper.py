from src.scrapers.abstract_scraper import AbstractScraper


import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime


class SenterpriseScraper(AbstractScraper):
    site = "Senterprise"
    def __init__(self):
        self.site = 'Senterprise'

    def request_status(self):
        url = "https://jobb.senterprise.se/jobs?department_id=6559"
        response = requests.get(url)
        print(f'{self.__class__.site} > Response:', response.status_code)
        return response
    
    def extract_job_payloads(self, response):
        scraped_html = BeautifulSoup(response.text, "html.parser")
        jobs_container = scraped_html.find("ul", id="jobs_list_container")
        job_payloads = jobs_container.find_all("li", class_="w-full") if jobs_container else []
    
        print(f'{self.site} > Nmr of scraped adds:', len(job_payloads))   
        return job_payloads 


    def extract_id(self, payload):
        try:
            site_id = self.extract_site_id(payload) 
            return f'{self.site}-{site_id}'
        except: return None


    def extract_site_id(self, payload):   
        try:    
            return self.extract_site_id(payload)
        except: return None


    def extract_job_title(self, payload):
        try: 
            tag_title_span = payload.find("span", title=True)
            job_title = tag_title_span["title"].replace('Konsultuppdrag | ', '').strip('')
            return job_title
        except: return None
   

    def extract_work_location(self, payload):
        try:
            tags_all_span = payload.select("div.mt-1.text-md span")
            return tags_all_span[2].get_text(strip=True)
        except: None  


    def extract_work_type(self, payload):
        try:
            tags_all_span = payload.select("div.mt-1.text-md span")
            return tags_all_span[4].get_text(strip=True)
        except: None  
    

    def extract_link(self, payload):
        try:
            tag_link = payload.select("a",  href=True)
            return tag_link['href']
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
        jobs_container = scraped_html.find("ul", id="jobs_list_container")

        # Find all job <li> inside this container
        job_posts = jobs_container.find_all("li", class_="w-full") if jobs_container else []

        print(f'{self.__class__.site} > Nmr of scraped adds:', len(job_posts))
       
        job_payloads = {}
        for job in job_posts:
            tag_link = job.find("a",  href=True)       
            site = SenterpriseScraper.site
            site_id = tag_link['href']
            id = f"{site}-{site_id}"

            job_payloads[id] = str(job)

        return job_payloads


    def parse_bronze_data(self, new_payloads):
        bronze_data = pd.DataFrame(columns=AbstractScraper.bronze_columns)

        for id, payload in new_payloads.items():
            payload = BeautifulSoup(payload, "html.parser")
      
            tag_title_span = payload.find("span", title=True)
            tags_all_span = payload.select("div.mt-1.text-md span")

            site = SenterpriseScraper.site
            site_id = id.replace(f"{SenterpriseScraper.site}-", "")
            job_title = None 
            area = None
            due_date = None
            work_location = None 
            work_type = None 
            link = id.replace(f"{SenterpriseScraper.site}-", "")
            ingestion_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            is_new = True

            if tag_title_span:
                tag_title_span = payload.find("span", title=True)
                job_title = tag_title_span["title"].replace('Konsultuppdrag | ', '').strip('')
       
        
            if len(tags_all_span) >= 3: 
                work_location = tags_all_span[2].get_text(strip=True)
            
            if len(tags_all_span) >= 5:
                work_type = tags_all_span[4].get_text(strip=True)
            
            bronze_data.loc[len(bronze_data)] = [
                id, site, site_id, job_title, area, due_date,
                work_location, work_type, link, ingestion_ts, is_new
            ]
   
        print(f'{self.__class__.site} > Parsing bronze data:', len(bronze_data))
        return bronze_data
        

        
        