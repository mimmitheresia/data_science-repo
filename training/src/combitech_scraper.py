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
        response = await page.goto("https://www.combitech.se/karriar/lediga-jobb/")
        status_code = response.status
        await page.wait_for_timeout(2000)
        html = await page.content()
        await browser.close()

        soup = BeautifulSoup(html, "html.parser")
        return soup, status_code


class CombitechScraper(AbstractScraper):
    site = 'Combitech'

    async def request_status(self):
        soup, status_code = await scrape_jobs()
        response = soup
        print(f'{self.__class__.site} > Status code:', status_code)
        return response


    def return_raw_job_posts_data(self, response):
  
        job_div_pattern = "div.block.w-full.mb-4.md\\:pb-0.md\\:mb-0.lg\\:pb-4"
        job_posts = response.select(job_div_pattern)
   
        print(f'{self.__class__.site} > Nmr of scraped adds:', len(job_posts))
        return job_posts


    def parse_raw_data(self, job_posts):
        raw_data = pd.DataFrame(columns=['site', 'site_id','job_title', 'raw_payload', 'ingestion_ts'])
        
        for job in job_posts:
            tag_title = job.select_one("#job-title")
            tag_site_id = job.select_one("a.cursor-pointer")

            site = CombitechScraper.site
            site_id = tag_site_id.get("onclick","").replace("location.href=", "").replace("'", "")
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
            
            tag_area = payload.select_one("#job-type")
            tag_due_date = payload.select_one("h5.font-normal")
            tag_location = payload.select_one("#job-locations")

            # Job area
            
            area = tag_area.get("data-value", "").strip() if tag_area else ""

            created = None 
            start_date = None
            end_date = None
            duration = None 
            due_date = None

            work_location = tag_location.get_text(strip=True) if tag_location else ""
            work_type = None
            link = 'https://www.combitech.se' + site_id
            ingestion_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 

            if tag_due_date:
                txt = tag_due_date.get_text(strip=True)
                if "Sista ansökningsdag:" in txt:
                    due_date = txt.replace("Sista ansökningsdag:", "").strip()



            bronze_data.loc[len(bronze_data)] = [site, site_id, job_title, area, created, start_date, end_date, duration, due_date, work_location, work_type, link, ingestion_ts]
        
        print(f'{self.__class__.site} > Parsing bronze data:', len(bronze_data))
        return bronze_data
    



        

        
        