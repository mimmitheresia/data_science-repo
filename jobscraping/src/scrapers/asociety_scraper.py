from src.scrapers.abstract_scraper import AbstractScraper
from src.utils import return_regex_string_match, slugify_title_for_link
import requests
import pandas as pd
from datetime import datetime
import os
import json, re
import ast
    

class ASocietyScraper(AbstractScraper):
    site = 'A Society'

    def request_status(self):
        url = "https://www.asocietygroup.com/sv/uppdrag?_rsc=9il7j"
        headers = {}

        response = requests.get(url, headers=headers)
        print(f'{self.__class__.site} > Response:', response.status_code)
        return response
    
    def return_raw_job_posts_data(self, response):    
        pattern_job_title = r'(\{.*?requisition_name.*?\})'
        pattern_site_id = r'\\"abstract_id\\":\\"(.*?)\\"|\"abstract_id\":\"(.*?)\"' 
        scraped_html = response.text 
        job_posts = re.findall(pattern_job_title, scraped_html, re.DOTALL)
        print(f'{self.__class__.site} > Nmr of scraped adds:', len(job_posts))
        
        job_payloads = {}
        for job in job_posts:
            site = ASocietyScraper.site
            site_id = return_regex_string_match(pattern_site_id, job)
            id = f'{site}-{site_id}'
            job_payloads[id] = str(job)
        
        return job_payloads
    

    def parse_bronze_data(self, new_payloads):
        bronze_data = pd.DataFrame(columns=AbstractScraper.bronze_columns)
        
        pattern_site_id = r'\\"abstract_id\\":\\"(.*?)\\"|\"abstract_id\":\"(.*?)\"'
        pattern_job_title = r'\\"requisition_name\\":\\"(.*?)\\"|\"requisition_name\":\"(.*?)\"'
        pattern_area = r'\\"requisition_servicecategoryid\\":\\"(.*?)\\"|\"requisition_servicecategoryid\":\"(.*?)\"'
        pattern_due_date = r'\\"requisition_offerduedate\\":\\"(.*?)\\"|\"requisition_offerduedate\":\"(.*?)\"'
        pattern_work_location = r'\\"requisition_locationid\\":\\"(.*?)\\"|\"requisition_locationid\":\"(.*?)\"'
        pattern_work_type = r'\\"requisition_remotework\\":\\"(.*?)\\"|\"requisition_remotework\":\"(.*?)\"'


        for id, payload in new_payloads.items():
            
            site = ASocietyScraper.site
    
            site_id = return_regex_string_match(pattern_site_id, payload)
            job_title = return_regex_string_match(pattern_job_title, payload)
            area = return_regex_string_match(pattern_area, payload)

            due_date = return_regex_string_match(pattern_due_date, payload)

            work_location = return_regex_string_match(pattern_work_location, payload)
            work_type = return_regex_string_match(pattern_work_type, payload)
            link = f'https://www.asocietygroup.com/sv/uppdrag/{slugify_title_for_link(job_title)}-{site_id}'
            ingestion_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # <--- timestamp här

            bronze_data.loc[len(bronze_data)] = [id, site, site_id, job_title, area, due_date, work_location, work_type, link, ingestion_ts]
        
        print(f'{self.__class__.site} > Parsing bronze data:', len(bronze_data))
        return bronze_data
    



        

        
        