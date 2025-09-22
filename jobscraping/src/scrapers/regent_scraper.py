from src.scrapers.abstract_scraper import AbstractScraper
from src.utils import return_regex_string_match, slugify_title_for_link
import requests
import pandas as pd
from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup

    

class RegentScraper(AbstractScraper):
    site = 'Regent'

    
    def request_status(self):
        url = "https://regent.se/uppdrag/"
        headers = {}

        response = requests.get(url, headers=headers)
        print(f'{self.__class__.site} > Response:', response.status_code)
        return response
    
    def return_raw_job_posts_data(self, response):
        scraped_html = BeautifulSoup(response.text, "html.parser")
        tag_job_div = "div.assignment-item"
        
        job_posts = scraped_html.select(tag_job_div)
        print(f'{self.__class__.site} > Nmr of scraped adds:', len(job_posts))

        job_payloads = {}
        for job in job_posts: 
            tag_site_id = job.select_one("a.btn.btn-warning.visa-desktop")
            site = RegentScraper.site
            site_id = tag_site_id.get("href") if tag_site_id else ""
            id = f'{site}-{site_id}'
            job_payloads[id] = str(job)
        
        return job_payloads
    

    def parse_bronze_data(self, new_payloads):
        bronze_data = pd.DataFrame(columns=AbstractScraper.bronze_columns)
        
        for id, payload in new_payloads.items():
            
            site = RegentScraper.site
        
            payload = BeautifulSoup(payload, "html.parser")
            
            tag_site_id = payload.select_one("a.btn.btn-warning.visa-desktop")
            tag_title = payload.select_one("a.blue > strong")
            tag_area = payload.select_one("div.summary")
            div_location = payload.find("strong", string="Ort:").find_next_sibling("div")
            
            site_id = tag_site_id.get("href") if tag_site_id else ""
            job_title = tag_title.get_text(strip=True) if tag_title else ""
            area = tag_area.get_text(strip=True) if tag_area else ""
            due_date = None 

            work_location = div_location.get_text(strip=True) if div_location else ""
            work_type = None 
           
    
            link = 'https://regent.se' + site_id
            payload = str(payload)
            ingestion_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            is_new = True

            bronze_data.loc[len(bronze_data)] = [id, site, site_id, job_title, area, due_date, work_location, work_type, link, ingestion_ts, is_new]
        
        print(f'{self.__class__.site} > Parsing bronze data:', len(bronze_data))
        return bronze_data
    



        

        
        