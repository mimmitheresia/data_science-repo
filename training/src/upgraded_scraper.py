from src.abstract_scraper import AbstractScraper
from src.utils import return_regex_string_match, slugify_title_for_link
import requests
import pandas as pd
from datetime import datetime
import os
import json, re
import ast
import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
import asyncio
    
async def scrape_jobs():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        response = await page.goto("https://upgraded.se/lediga-uppdrag/")
        status_code = response.status
        await page.wait_for_timeout(2000)
        html = await page.content()
        await browser.close()

        soup = BeautifulSoup(html, "html.parser")
        return soup, status_code


class UpgradedScraper(AbstractScraper):
    site = 'Upgraded'

    async def request_status(self):
        soup, status_code = await scrape_jobs()
        response = soup
        print(f'{self.__class__.site} > Status code:', status_code)
        return response


    def return_raw_job_posts_data(self, response):
  
        job_div_pattern = "tr.konsultuppdrag__table-row"
        job_posts = response.select(job_div_pattern)
   
        print(f'{self.__class__.site} > Nmr of scraped adds:', len(job_posts))
        return job_posts
    
    def request_status2(self):
        url = "https://upgraded.se/lediga-uppdrag/"
        headers = {}

        response = requests.get(url, headers=headers)
        print(f'{self.__class__.site} > Response:', response.status_code)
        return response


    def return_raw_job_posts_data2(self, response):
        soup = BeautifulSoup(response.text, "html.parser")
        job_div_pattern = "tr.konsultuppdrag__table-row"
        job_posts = soup.select(job_div_pattern)
   
        print(f'{self.__class__.site} > Nmr of scraped adds:', len(job_posts))
        return job_posts


    def parse_raw_data(self, job_posts):
        raw_data = pd.DataFrame(columns=['site', 'site_id','job_title', 'raw_payload', 'ingestion_ts'])
        
        for job in job_posts:
            tag_title = job.select_one("h5.entry-title")
            tag_site_id = job.select_one("a[href^='https://upgraded.se/konsultuppdrag/']")

            site = UpgradedScraper.site
            site_id = tag_site_id.get("href") if tag_site_id else ""
            job_title = tag_title.get_text(strip=True) if tag_title else ""
            ingestion_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 

            raw_data.loc[len(raw_data)] = [site, site_id, job_title, str(job), ingestion_ts]  
        return raw_data
    

    def parse_bronze_data(self, last_raw_data):
        bronze_data = pd.DataFrame(columns=['site', 'site_id','job_title', 'area', 'created', 'start_date', 'end_date', 'duration', 'due_date', 'work_location', 'work_type', 'link', 'ingestion_ts'])
        
        for idx, row in last_raw_data.iterrows():
            site = row['site']
            site_id = row['site_id']
            job_title = row['job_title']
        
            payload = row['raw_payload']
            payload = BeautifulSoup(payload, "html.parser")
            
            tag_due_date = payload.select_one("td.konsultuppdrag-column-3")
            tag_multiple_info = payload.select("span")

            area = None
            created = None
            start_date = None
            end_date = None
            due_date = tag_due_date.get_text(strip=True) if tag_due_date else None
            duration = None

            work_location = None
            work_type = None 

               
     
            multiple_info_list = []
            for span in tag_multiple_info:
                info = span.get_text(strip=True)
                multiple_info_list.append(info)
            
            work_location = multiple_info_list[2]
            work_type = multiple_info_list[4]
            area = multiple_info_list[6]
            
            link = site_id
            ingestion_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            bronze_data.loc[len(bronze_data)] = [site, site_id, job_title, area, created, start_date, end_date, duration, due_date, work_location, work_type, link, ingestion_ts]
        
        print(f'{self.__class__.site} > Parsing bronze data:', len(bronze_data))
        return bronze_data
    



        

        
        