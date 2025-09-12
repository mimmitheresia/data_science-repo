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
        response = await page.goto("https://www.nikita.se/lediga-uppdrag/")
        status_code = response.status
        await page.wait_for_timeout(2000)
        html = await page.content()
        await browser.close()

        soup = BeautifulSoup(html, "html.parser")
        return soup, status_code


class NikitaScraper(AbstractScraper):
    site = 'Nikita'

    async def request_status(self):
        soup, status_code = await scrape_jobs()
        response = soup
        print(f'{self.__class__.site} > Status code:', status_code)
        return response


    def return_raw_job_posts_data(self, response):
  
        job_div_pattern = "li.open-position-item.opened"
        job_posts = response.select(job_div_pattern)
   
        print(f'{self.__class__.site} > Nmr of scraped adds:', len(job_posts))
        return job_posts


    def parse_raw_data(self, job_posts):
        raw_data = pd.DataFrame(columns=['site', 'site_id','job_title', 'raw_payload', 'ingestion_ts'])
        
        for job in job_posts:
            tag_title = job.select_one("span.open-position-title")
            tag_site_id = job.select_one("a.open-position-list-link")
          
            site = NikitaScraper.site
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
            
            area = None 
            created = None 
            tag_created_day = payload.select_one("span.open-position-date-day")
            tag_created_month = payload.select_one("span.open-position-date-month")
            created_day = tag_created_day.get_text(strip=True) if tag_created_day else ""
            created_month = tag_created_month.get_text(strip=True) if tag_created_month else ""
            created_year = datetime.today().year
            created_str = f'{created_day} {created_month} {created_year}'
            date_obj = datetime.strptime(created_str, "%d %b %Y")
            created = date_obj.strftime("%Y-%m-%d")

            start_date = None
            end_date = None
            duration = None
            due_date = None 

            work_location = None
            work_type = None 
            link = site_id
            ingestion_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            bronze_data.loc[len(bronze_data)] = [site, site_id, job_title, area, created, start_date, end_date, duration, due_date, work_location, work_type, link, ingestion_ts]
        
        print(f'{self.__class__.site} > Parsing bronze data:', len(bronze_data))
        return bronze_data
    



        

        
        