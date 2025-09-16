from src.abstract_scraper import AbstractScraper
import requests
import pandas as pd
from datetime import datetime
import ast
    

class EworkScraper(AbstractScraper):
    site = 'Ework'

    def request_status(self):
        url = "https://app.verama.com/api/public/job-requests"

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://app.verama.com/sv/job-requests",
            "x-frontend-version": "a4c15af0",
            "x-session": "a9b80196-b28a-42a3-a83f-c1059ef946af"
        }

        # Parametrar för att hämta alla jobb (storlek 100)
        params = {
            "page": 0,
            "size": 1000,  # hämta upp till 100 jobb
            "sort": "firstDayOfApplications,DESC",
            "location.country": "Sweden",
            "location.countryCode": "SWE",
            "location.suggestedPhoneCode": "SE",
            "location.locationId": "here:cm:namedplace:20298368",
            "location.id": "NaN",
            "location.signature": "",
            "dedicated": "false",
            "favouritesOnly": "false",
            "recommendedOnly": "false",
            "query": ""
        }

        response = requests.get(url, headers=headers, params=params)
        print(f'{self.__class__.site} > Response:', response.status_code)
        return response
    

    def return_raw_job_posts_data(self, response):
        data = response.json()   # parse till Python-dict
        # alla annonser
        job_posts = data["content"]
        print(f'{self.__class__.site} > Nmr of scraped adds:', len(job_posts))
        
        raw_data = pd.DataFrame(columns=['site', 'site_id', 'raw_payload'])
        for job in job_posts:
            site = EworkScraper.site
            site_id = str(job['id'])
            raw_data.loc[len(raw_data)] = [site, site_id, job]
        return raw_data
    

    def parse_bronze_data(self, last_raw_data):
        bronze_data = pd.DataFrame(columns=['site', 'site_id','job_title', 'area', 'created', 'start_date', 'end_date', 'duration', 'due_date', 'work_location', 'work_type', 'link', 'raw_payload', 'ingestion_ts'])

        
        for idx, row in last_raw_data.iterrows():
            site = row['site']
            site_id = row['site_id']
        
            payload = row['raw_payload']
            job_title = payload['title']
            area = ''
            try: 
                for skills_dict in payload['skills']:
                    skill = skills_dict['skill']['name']
                    area += f'{skill}, ' 
                area = area.strip(', ')
            except: 
                area = None 
            
            
            created = payload['createdDate']
            start_date = payload['startDate']
            end_date = payload['endDate']
            duration = None 
            due_date = payload['lastDayOfApplications']

            work_location = ''
            try:  
                for location_dict in payload['locations']:
                    location = location_dict['city']
                    work_location += f'{location}, '
                work_location = work_location.strip(', ')
            except: 
                work_location = None 

            work_type = ''
            try: 
                remoteness = payload['remoteness']
                if remoteness == 0: 
                    work_type = 'På plats'
                elif remoteness == 100: 
                    work_type = 'Remote'
                else: 
                    work_type = 'Hybrid'
            except: 
                work_type = None

            link = f'https://app.verama.com/sv/job-requests/{site_id}'
            ingestion_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # <--- timestamp här

            bronze_data.loc[len(bronze_data)] = [site, site_id, job_title, area, created, start_date, end_date, duration, due_date, work_location, work_type, link, payload, ingestion_ts]
        print(f'{self.__class__.site} > Parsing bronze data:', len(bronze_data))
        return bronze_data

        

        
        