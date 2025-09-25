from src.scrapers.abstract_scraper import AbstractScraper
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
import re
    

class ProfinderScraper(AbstractScraper):
    site = "Profinder"

    def __init__(self):
        self.site = 'Profinder'

    def request_status(self):
        url = "https://www.profinder.se/lediga-uppdrag"
        response = requests.get(url)
        print(f'{self.__class__.site} > Response:', response.status_code)
        return response
    

    def extract_job_payloads(self, response):
        scraped_html = BeautifulSoup(response.text, "html.parser")
        job_payloads = scraped_html.select('div.item-link-wrapper')
   
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
            tag_job_title = payload.find("div", class_="item-action")
            job_title_id = tag_job_title.get("aria-label")  # extracts the content of aria-label
            job_title = re.sub(r'\s*ID:\d+', '', job_title_id)
            return job_title
        except: return None
        

    def extract_link(self, payload):
        try:
            tag_link = payload.find("a",  href=True) 
            link = tag_link['href']
            return link
        except: None
        


    def scrape_jobs_payloads_dict(self, response):
        scraped_html = BeautifulSoup(response.text, "html.parser")
        job_posts = scraped_html.select('div.item-link-wrapper')
        print(f'{self.__class__.site} > Nmr of scraped adds:', len(job_posts))
        
        tag_link = scraped_html.select("a",  href=True)
     
        job_payloads = {}
        for job in job_posts:
            tag_link = job.find("a",  href=True)       
            site = ProfinderScraper.site
            site_id = tag_link['href']
            id = f"{site}-{site_id}"

            job_payloads[id] = str(job)

        return job_payloads


    def parse_bronze_data(self, new_payloads):
        bronze_data = pd.DataFrame(columns=AbstractScraper.bronze_columns)

        for id, payload in new_payloads.items():
            payload = BeautifulSoup(payload, "html.parser")
      
            tag_job_title = payload.find("div", class_="item-action")
            tag_info = payload.find('div', class_='BOlnTh')

            site = ProfinderScraper.site
            site_id = id.replace(f"{ProfinderScraper.site}-", "")
            job_title = None 
            area = None
            due_date = None
            work_location = None 
            work_type = None 
            link = id.replace(f"{ProfinderScraper.site}-", "")
            ingestion_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            is_new = True
            tag_job_title = payload.find("div", class_="item-action")
            job_title_id = tag_job_title.get("aria-label")  # extracts the content of aria-label
            job_title = re.sub(r'\s*ID:\d+', '', job_title_id)


            bronze_data.loc[len(bronze_data)] = [
                id, site, site_id, job_title, area, due_date,
                work_location, work_type, link, ingestion_ts, is_new
            ]
  

        print(f'{self.__class__.site} > Parsing bronze data:', len(bronze_data))
        return bronze_data
    



        

        
        