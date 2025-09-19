from src.scrapers.abstract_scraper import AbstractScraper
import requests
import pandas as pd
from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup



class UpgradedScraper(AbstractScraper):
    site = "Upgraded"

    def __init__(self):
        self.url = "https://upgraded.se/wp-admin/admin-ajax.php"

    def request_status(self, nonce="72e84adecc"):
        # If nonce is None, you might need to scrape it first from the page
        if not nonce:
            raise ValueError("You need a valid nonce from the page")

        headers = {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-language": "sv-SE,sv;q=0.9,en-US;q=0.8,en;q=0.7",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "origin": "https://upgraded.se",
            "referer": "https://upgraded.se/lediga-uppdrag/",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
            "x-requested-with": "XMLHttpRequest"
        }

        data = {
            "action": "do_filter_posts",
            "nonce": nonce,
            "params[ort-term]": "alla-orter",
            "params[roll-term]": "alla-roller",
            "params[kund-term]": "alla-kunder",
            "params[ansokdate-term]": "sortering",
            "params[search-term]": ""
        }

        response = requests.post(self.url, headers=headers, data=data)
        print(f"{self.__class__.site} > Response:", response.status_code)
        return response
    

    def return_raw_job_posts_data(self, response):
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
            
            bronze_data.loc[len(bronze_data)] = [id, site, site_id, job_title, area, due_date, work_location, work_type, link, ingestion_ts]
        
        print(f'{self.__class__.site} > Parsing bronze data:', len(bronze_data))
        return bronze_data
    



        

        
        