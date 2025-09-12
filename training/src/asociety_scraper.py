from src.abstract_scraper import AbstractScraper
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
        html = response.text 
        job_title_pattern = r'(\{.*?requisition_name.*?\})'
        job_posts = re.findall(job_title_pattern, html, re.DOTALL)
   
        print(f'{self.__class__.site} > Nmr of scraped adds:', len(job_posts))
        return job_posts


    def parse_raw_data(self, job_posts):
        raw_data = pd.DataFrame(columns=['site', 'site_id','job_title', 'raw_payload', 'ingestion_ts'])
        
        pattern_site_id = r'\\"abstract_id\\":\\"(.*?)\\"|\"abstract_id\":\"(.*?)\"' 
        pattern_job_title = r'\\"requisition_name\\":\\"(.*?)\\"|\"requisition_name\":\"(.*?)\"'

        for job in job_posts:
            site = ASocietyScraper.site
            site_id = return_regex_string_match(pattern_site_id, job)
            job_title = return_regex_string_match(pattern_job_title, job)
            ingestion_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # <--- timestamp här
            raw_data.loc[len(raw_data)] = [site, site_id, job_title, job, ingestion_ts]
         
        return raw_data
    

    def parse_bronze_data(self, last_raw_data):
        bronze_data = pd.DataFrame(columns=['site', 'site_id','job_title', 'area', 'created', 'start_date', 'end_date', 'duration', 'due_date', 'work_location', 'work_type', 'link', 'ingestion_ts'])
        
        pattern_area = r'\\"requisition_servicecategoryid\\":\\"(.*?)\\"|\"requisition_servicecategoryid\":\"(.*?)\"'
        pattern_created = r'\\"requisition_publisheddateandtime\\":\\"(.*?)\\"|\"requisition_publisheddateandtime\":\"(.*?)\"'
        pattern_start_date = r'\\"requisition_startdate\\":\\"(.*?)\\"|\"requisition_startdate\":\"(.*?)\"'
        pattern_due_date = r'\\"requisition_offerduedate\\":\\"(.*?)\\"|\"requisition_offerduedate\":\"(.*?)\"'
        pattern_work_location = r'\\"requisition_locationid\\":\\"(.*?)\\"|\"requisition_locationid\":\"(.*?)\"'
        pattern_work_type = r'\\"requisition_remotework\\":\\"(.*?)\\"|\"requisition_remotework\":\"(.*?)\"'


        for idx, row in last_raw_data.iterrows():
            site = row['site']
            site_id = row['site_id']
            job_title = row['job_title']
        
            payload = ast.literal_eval(row['raw_payload'])
            area = return_regex_string_match(pattern_area, payload)

            created = return_regex_string_match(pattern_created, payload)
            start_date = return_regex_string_match(pattern_start_date, payload)
            end_date = None
            duration = None
            due_date = return_regex_string_match(pattern_due_date, payload)

            work_location = return_regex_string_match(pattern_work_location, payload)
            work_type = return_regex_string_match(pattern_work_type, payload)
            link = f'https://www.asocietygroup.com/sv/uppdrag/{slugify_title_for_link(job_title)}-{id}'
            ingestion_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # <--- timestamp här

            bronze_data.loc[len(bronze_data)] = [site, site_id, job_title, area, created, start_date, end_date, duration, due_date, work_location, work_type, link, ingestion_ts]
        
        print(f'{self.__class__.site} > Parsing bronze data:', len(bronze_data))
        return bronze_data
    



        

        
        