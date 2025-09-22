from src.scrapers.abstract_scraper import AbstractScraper
from src.utils import slugify_title_for_link
import requests
import pandas as pd
from datetime import datetime
import os
import json
import ast
from bs4 import BeautifulSoup
    

class ITCNetworkScraper(AbstractScraper):
    site = 'ITC Network'

    def request_status(self):
        url = "https://itcnetwork.se/uppdrag/"
        headers = {
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
        }

        response = requests.get(url, headers=headers)

        print(f'{self.__class__.site} > Response:', response.status_code)
        return response
    

    def scrape_jobs_payloads_dict(self, response):
        scraped_html = BeautifulSoup(response.text, "html.parser")
        tag_job_div = "div.maf_feed_single.maf_wid100"
        
        job_posts = scraped_html.select(tag_job_div)
        print(f'{self.__class__.site} > Nmr of scraped adds:', len(job_posts))
        

        job_payloads = {}
        for job in job_posts:
            site = ITCNetworkScraper.site
            tag_job_title = job.select_one("h3.maf_feed_title b")
            site_id = tag_job_title.get_text(strip=True) if tag_job_title else None
            id = f'{site}-{site_id}'
            job_payloads[id] = str(job)
        return job_payloads

    

    def parse_bronze_data(self, new_payloads):
        bronze_data = pd.DataFrame(columns=AbstractScraper.bronze_columns)

        
        for id, payload in new_payloads.items():
            payload = BeautifulSoup(payload, "html.parser")
            
            tag_job_title = payload.select_one("h3.maf_feed_title b")
            tags_area = payload.select("div.maf_feed_block")
            
            site = ITCNetworkScraper.site
            site_id = tag_job_title.get_text(strip=True) if tag_job_title else None
            job_title = site_id
            area = None
            due_date = None
            work_location = None
            work_type = None
            link = f'https://itcnetwork.se/uppdrag/' 
            ingestion_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # <--- timestamp här
            is_new = True
            
            for tag in tags_area:
                label = tag.find("label")
                if label and "Kompetens" in label.get_text():
                    tag_area = tag.find("p")
                    area = tag_area.get_text(strip=True) if tag_area else None
                    break

            bronze_data.loc[len(bronze_data)] = [id, site, site_id, job_title, area, due_date, work_location, work_type, link, ingestion_ts, is_new]
        print(f'{self.__class__.site} > Parsing bronze data:', len(bronze_data))
        return bronze_data

        

        
        