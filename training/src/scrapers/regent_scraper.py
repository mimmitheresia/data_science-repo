from src.abstract_scraper import AbstractScraper
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

        raw_data = pd.DataFrame(columns=['site', 'site_id', 'raw_payload'])
        for job in job_posts: 
            tag_site_id = job.select_one("a.btn.btn-warning.visa-desktop")
            site = RegentScraper.site
            site_id = tag_site_id.get("href") if tag_site_id else ""
            site_id = f'{site}-{site_id}'
            raw_data.loc[len(raw_data)] = [site, str(site_id), str(job)]
        
        return raw_data
    

    def parse_bronze_data(self, last_raw_data):
        bronze_data = pd.DataFrame(columns=['site', 'site_id','job_title', 'area', 'due_date', 'work_location', 'work_type', 'link', 'raw_payload', 'ingestion_ts'])
        
        for idx, row in last_raw_data.iterrows():
            site = row['site']
            site_id = row['site_id']
        
            payload = BeautifulSoup(row['raw_payload'], "html.parser")
            
            tag_title = payload.select_one("a.blue > strong")
            tag_area = payload.select_one("div.summary")

            div_location = payload.find("strong", string="Ort:").find_next_sibling("div")
            
            job_title = tag_title.get_text(strip=True) if tag_title else ""
            area = tag_area.get_text(strip=True) if tag_area else ""
            due_date = None 

            work_location = div_location.get_text(strip=True) if div_location else ""
            work_type = None 
           
    
            link = 'https://regent.se' + site_id
            ingestion_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            bronze_data.loc[len(bronze_data)] = [site, site_id, job_title, area, due_date, work_location, work_type, link, payload, ingestion_ts]
        
        print(f'{self.__class__.site} > Parsing bronze data:', len(bronze_data))
        return bronze_data
    



        

        
        