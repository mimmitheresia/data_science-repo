from src.scrapers.abstract_scraper import AbstractScraper
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
import re
    

class ProfinderScraper(AbstractScraper):
    site = "Profinder"

    def request_status(self):
        url = "https://www.profinder.se/lediga-uppdrag"
        response = requests.get(url)
        print(f'{self.__class__.site} > Response:', response.status_code)
        return response

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

            job_title_id = tag_job_title.get("aria-label")  # extracts the content of aria-label
            job_title = re.sub(r'\s*ID:\d+', '', job_title_id)

            info_str = tag_info.get_text(strip=True) if tag_info else ""
            info_list = re.split(r'(?=\b\w+:)', info_str)
            for info in info_list:
                key_value = info.split(':')
                if len(key_value) >=2:
                    key = key_value[0].strip()
                    value = key_value[1].strip()

                    if key in ['Placering', 'Location']:
                        work_location = value

            bronze_data.loc[len(bronze_data)] = [
                id, site, site_id, job_title, area, due_date,
                work_location, work_type, link, ingestion_ts, is_new
            ]
  

        print(f'{self.__class__.site} > Parsing bronze data:', len(bronze_data))
        return bronze_data
    



        

        
        