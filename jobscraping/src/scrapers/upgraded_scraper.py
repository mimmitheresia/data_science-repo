from src.scrapers.abstract_scraper import AbstractScraper
import requests 
import pandas as pd
from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup
import re




class UpgradedScraper(AbstractScraper):
    site = "Upgraded"

    def __init__(self):
        self.base_url = "https://upgraded.se/lediga-uppdrag/"
        self.ajax_url = "https://upgraded.se/wp-admin/admin-ajax.php"
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
        })

    def get_nonce(self):
        r = self.session.get(self.base_url)
        soup = BeautifulSoup(r.text, "html.parser")
        # find "nonce" inside page script
        match = re.search(r'"nonce":"([a-f0-9]+)"', r.text)
        if not match:
            raise ValueError("Nonce not found")
        return match.group(1)

    def request_status(self):
        nonce = self.get_nonce()
        data = {
            "action": "do_filter_posts",
            "nonce": nonce,
            "params[ort-term]": "alla-orter",
            "params[roll-term]": "alla-roller",
            "params[kund-term]": "alla-kunder",
            "params[ansokdate-term]": "sortering",
            "params[search-term]": ""
        }
        response = self.session.post(self.ajax_url, data=data)
        print(f"{self.__class__.site} > Response:", response.status_code)
        return response
  
    

    def scrape_jobs_payloads_dict(self, response):
        tag_job_div = "td.konsultuppdrag-column-1"
        response = response.json()
        scraped_html = BeautifulSoup(response["content"], "html.parser")
        job_posts = scraped_html.select(tag_job_div)
        print(f'{self.__class__.site} > Nmr of scraped adds:', len(job_posts))

        job_payloads = {}
        for job in job_posts: 
            tag_link = job.find("a", href=True)
            
            site = UpgradedScraper.site
            site_id = tag_link['href']
            id = f'{site}-{site_id}'
            job_payloads[id] = str(job)
        
        return job_payloads


    def parse_bronze_data(self, new_payloads):
        bronze_data = pd.DataFrame(columns=AbstractScraper.bronze_columns)

        for id, payload in new_payloads.items():
            payload = BeautifulSoup(payload, "html.parser")
            
            tag_job_title = payload.find("h5", class_="entry-title")
            tags_all_span = payload.select("span")
            tag_link = payload.find("a", href=True)  # finds the first <a> with hre
            
            site = UpgradedScraper.site
            site_id = id.replace(f'{UpgradedScraper.site}-','')   
            job_title = tag_job_title.get_text(strip=True)
            area = None 
            due_date = None 
            work_location = None
            work_type = None

            area = tags_all_span[6].get_text(strip=True)
            due_date = tags_all_span[-1].get_text(strip=True)
            work_location = tags_all_span[2].get_text(strip=True)
            work_type = tags_all_span[4].get_text(strip=True)
            link = tag_link['href']
            ingestion_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            is_new = True
            
            bronze_data.loc[len(bronze_data)] = [id, site, site_id, job_title, area, due_date, work_location, work_type, link, ingestion_ts, is_new]
        
        print(f'{self.__class__.site} > Parsing bronze data:', len(bronze_data))
        return bronze_data
    



        

        
        