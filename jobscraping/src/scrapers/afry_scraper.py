from src.scrapers.abstract_scraper import AbstractScraper
import requests
import pandas as pd
from datetime import datetime
import os
import json
import ast
    

class AfryScraper(AbstractScraper):
    site = 'Afry'

    def request_status(self):
        url = "https://afry.com/sv/api/assignment-list"


        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://aliant.recman.page/jobs?sort=newest",
            "X-CSRF-TOKEN": "694-1334-250910123734-95bda24a27252a6ddffd471f9e57c22b8e41f4600506a059f483cbde2f2a1328b748",   # if you see this in Network, copy it too
            "Cookie": "recman=drd5t8o7u0h47bo4sotqccb6br0951sk; organisation=aliant.recman.page%3B694%3B1334%3B1334; axe_xs=694-1334-250910123734-95bda24a27252a6ddffd471f9e57c22b8e41f4600506a059f483cbde2f2a1328b748; cookie_accept=0%3B1" #sometimes Recman requires session cookies
        }

        response = requests.get(url, headers = headers)
        print(f'{self.__class__.site} > Response:', response.status_code)
        return response
        
    
    def scrape_jobs_payloads_dict(self, response):
        scraped_data = response.json()   # parse till Python-dict
        job_posts = scraped_data["Adverts"]
        print(f'{self.__class__.site} > Nmr of scraped adds:', len(job_posts))

        job_payloads = {}
        for job in job_posts:
            site = AfryScraper.site
            id = f'{site}-{job['Id']}'
            job_payloads[id] = str(job)
        
        return job_payloads
    

    def parse_bronze_data(self, new_payloads):
        bronze_data = pd.DataFrame(columns=AbstractScraper.bronze_columns)
        
        for id, payload in new_payloads.items():
          
            site = AfryScraper.site
            payload = ast.literal_eval(payload)
            
            site_id = payload['Id']
            job_title = payload['Title']
            areas = []
            for skill in payload['CompetenceAreas']:
                areas.append(skill['Name'])
            
            area = " ".join(areas)
            due_date = payload['LastApplyDate']

            cities = []
            for city in payload['Cities']:
                cities.append(city['Name'])
            
            work_location = " ".join(cities)
            work_type = None
            link = payload['DetailUrl']
            ingestion_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # <--- timestamp här
            is_new = True
           
            bronze_data.loc[len(bronze_data)] = [id, site, site_id, job_title, area, due_date, work_location, work_type, link, ingestion_ts, is_new]
        
        print(f'{self.__class__.site} > Parsing bronze data:', len(bronze_data))
        return bronze_data
    

        

        