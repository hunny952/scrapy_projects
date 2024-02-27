# -*- coding: utf-8 -*-
import scrapy
from urllib.parse import urlencode
import logging
from scrapy.crawler import CrawlerProcess
from datetime import datetime
import os
import configparser

logging.getLogger('scrapy').propagate = False

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.join(ROOT_DIR, 'config.ini')

config = configparser.ConfigParser()
config.read(CONFIG_DIR)

def get_url(url, api_key):
    """
    Generate the proxy URL using Scraper API for the given URL.

    Args:
        url (str): The URL to be proxied.
        api_key (str): scraperapi key to be provided in config.ini file

    Returns:
        str: The proxied URL.
    """
    payload = {'api_key': api_key, 'url': url}
    proxy_url = 'http://api.scraperapi.com/?' + urlencode(payload)
    return proxy_url

class ThegazetteSpider(scrapy.Spider):
    """
    Spider class to scrape data from thegazette.co.uk website.
    """
    name = 'thegazette'
    
    def start_requests(self):
        """
        Generate initial requests to start scraping.

        Yields:
            scrapy.Request: Initial request objects.
        """
        api_key = config.get('Api', 'api_key')
        logging.debug("*** Your Api key *** : %s", api_key)
        if not api_key:
            raise ValueError("Please Enter Your ScrapperApi Key in config.ini file")
        logging.info("START GETTING NOTICE DETAILS FOR FIRST 15 PAGES PLEASE WAIT ...")
        for size in range(1, 16):
            simple_url = f"https://www.thegazette.co.uk/all-notices/notice?text=&categorycode=G105000000&noticetypes=&location-postcode-1=&location-distance-1=1&location-local-authority-1=&numberOfLocationSearches=1&start-publish-date=&end-publish-date=&edition=&london-issue=&edinburgh-issue=&belfast-issue=&sort-by=&results-page-size=10&results-page={size}"
            yield scrapy.Request(url=get_url(simple_url, api_key), callback=self.parse_thegazette_response, meta={'url': simple_url, 'api_key': api_key})

    def parse_thegazette_response(self, response):
        """
        Parse the response from the initial requests to extract URLs of notices.

        Args:
            response (scrapy.http.Response): The response object.

        Yields:
            scrapy.Request: Request objects to scrape individual notice pages.
        """
        try:
            main_url = response.meta['url']
            api_key = response.meta['api_key']
            logging.debug("*** PROCESS MAIN URL *** : %s", main_url)
            website = "https://www.thegazette.co.uk"
            all_notes_urls = response.css("#search-results .title a ::attr(href)").extract()
            for url in all_notes_urls:
                url = website + url
                yield scrapy.Request(url=get_url(url, api_key), callback=self.parse_notice_page, meta={'main_url': main_url, 'url': url})
        except Exception as e:
            logging.error('An error occurred in parse_keyword_response method: %s', e)

    def parse_notice_page(self, response):
        """
        Parse the individual notice pages to extract details.

        Args:
            response (scrapy.http.Response): The response object.
        """
        try:

            data = {"MAIN_URL":None, "URL":None, "title":None, 
                    "description":None, "notice_details":None, 
                    "TYPE": None, "Notice_type": None,
                    "Publication_date": None, "Edition":None, 
                    "Notice_ID":None, "Company_number":None,
                    "Notice_code":None}

            data["MAIN_URL"] = response.meta.get('main_url')
            data["URL"] = response.meta.get('url')
            logging.debug("*** PROCESS *** : %s", data.get("URL"))
            logging.debug("response: %s", response.status)

            data["title"] = response.css("title ::text").extract_first().split(" | ")[0].strip()
            logging.debug("title: %s", data.get("title"))
        
            # Extracting other relevant information from the notice page
            description = response.css('div[data-gazettes="P"] > p[data-gazettes="Text"] ::text').extract()
            if not description:
                description = response.css('div[data-gazettes="Notice"] p ::text').extract()
            data["description"] = [" ".join(item.split()) for item in description]

            notice_detail_headers = response.css("dt ::text").extract()
            notice_detail_values = response.css("dd ::text").extract()
            notice_detail_values = [" ".join(item.split()) for item in notice_detail_values if item.strip() and item.strip() != 'Notice timeline for company number']
            
            notice_details = dict(zip(notice_detail_headers, notice_detail_values))
            data["notice_details"] = notice_details

            if notice_details:
                data["TYPE"] = notice_details.get('Type:')
                data["Notice_type"] = notice_details.get('Notice type:')
                data["Publication_date"] = notice_details.get('Publication date:') or notice_details.get('Earliest publish date:')
                data["Edition"] = notice_details.get('Edition:')
                data["Notice_ID"] = notice_details.get('Notice ID:')
                data["Company_number"] = notice_details.get('Company number:')
                data["Notice_code"] = notice_details.get('Notice code:')

            logging.debug(f"FOUND DATA: {data}")
            yield data

        except Exception as e:
            logging.error('An error occurred while parsing parse_notice_page Method: %s', e)

filename = f'output_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.csv'
process = CrawlerProcess(settings={'FEEDS': {filename: {'format': 'csv'}}})
process.crawl(ThegazetteSpider)
process.start()
