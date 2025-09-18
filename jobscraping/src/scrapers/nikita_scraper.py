from src.scrapers.abstract_scraper import AbstractScraper
from src.utils import return_regex_string_match, slugify_title_for_link
import requests
import pandas as pd
from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup


class NikitaScraper(AbstractScraper):
    site = 'Nikita'

    def request_status(self):
        url = "https://www.nikita.se/lediga-uppdrag/"
        headers = {}

        response = requests.get(url, headers=headers)
        print(f'{self.__class__.site} > Response:', response.status_code)
        return response
    
    def return_raw_job_posts_data(self, response):
        tag_job_div = "li.open-position-item.opened"
        scraped_html = BeautifulSoup(response.text, "html.parser")    
        job_posts = scraped_html.select(tag_job_div)
        print(f'{self.__class__.site} > Nmr of scraped adds:', len(job_posts))

        job_payloads = {}
        for job in job_posts: 
            tag_site_id = job.select_one("a.open-position-list-link")
            
            site = NikitaScraper.site
            site_id = tag_site_id.get("href") if tag_site_id else ""
            id = f'{site}-{site_id}'
            job_payloads[id] = str(job)
        
        return job_payloads


    def parse_bronze_data(self, new_payloads):
        bronze_data = pd.DataFrame(columns=AbstractScraper.bronze_columns)
        
        for id, payload in new_payloads.items():
            
            site = NikitaScraper.site
     
            payload = BeautifulSoup(payload, "html.parser")
            tag_site_id = payload.select_one("a.open-position-list-link")
            tag_title = payload.select_one("span.open-position-title") 
            
            site_id = tag_site_id.get("href") if tag_site_id else ""
            job_title = tag_title.get_text(strip=True) if tag_title else ""
            area = None 
            due_date = None 

            work_location = None
            work_type = None 
            link = site_id
            ingestion_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            bronze_data.loc[len(bronze_data)] = [id, site, site_id, job_title, area, due_date, work_location, work_type, link, ingestion_ts]
        
        print(f'{self.__class__.site} > Parsing bronze data:', len(bronze_data))
        return bronze_data
    



        

        
        