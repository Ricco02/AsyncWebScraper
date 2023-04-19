from selenium.webdriver.support import expected_conditions as ec
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium import webdriver
from parsel import Selector
import pandas as pd
import numpy as np
import aiohttp
import logging
import asyncio
import json
import time
import csv
import os


#used in get_advert_list.iterate_throught_advert_list()
def generator(driver):
    #you may implement logic to get amount of pages here
    for page_number in range(1,AMOUNT_OF_PAGES):
        yield "https://www.website.com/adverts/listing/page="+str(page_number)


class page_scraper:simultaneous
    def __init__(self) -> None:
      #set your own based on if your response is json
        self.important_keys_from_json=['id','market','advertiserType','services','description','features','Area','Build_year','Building_floors_num',
                                      'Building_material','Building_ownership','Building_type','City','City_id','Construction_status','Country',
                                      'Equipment_types','Extras_types','Floor_no','Heating','Media_types','OfferType','ProperType','Province','Rent',
                                      'Security_types','Subregion','Windows_type','Rooms_num','latitude','longitude','address',
                                      'name','phones','images']

    def parse_link_for_requests(self,link):
        return #implement your logic for parsing requests

    
    async def request(self, session, link, semaphore):
        await semaphore.acquire()
        async with session.get(link) as resp:
            try:
                response= await resp.json()
                await asyncio.sleep(0.1)
                return response
            except Exception as exception:
                logging.error(f"Following error ocured: {exception}")
            finally:
                semaphore.release()            
        
    async def get_advert_data(self, list_of_links):
        results=[]
        parsed_results=[]
        semaphore=asyncio.BoundedSemaphore(40) #set your own amount of simultaneous connections
        async with aiohttp.ClientSession() as session:
            coros = [self.request(session, self.parse_link_for_requests(link), semaphore) for link in list_of_links]
            for coro in asyncio.as_completed(coros):
                temporary = await coro
                try:
                    results.append(temporary)
                except Exception as exception:
                    logging.error(f"Following error ocured: {exception}")
        for result in results:
            parsed_results.append({x[0]: x[1] if len(x)>1 else None for x in self.response_parser(result)}) #turn values spit out by response parser into dict
        return parsed_results
    
    def response_parser(self,json_dicts_to_parse):
        yielded=[]
        #you can implement your own parsing logic here, im my case response was multiple nested dictionares
        for key, value in json_dicts_to_parse.items():
            if isinstance(value, dict):
                yield from self.response_parser(value)
            elif key in json_dicts_to_parse and key not in yielded and key in self.important_keys_from_json:
                yielded.append(key)
                yield key, value
            else:
                pass


class data_files_handler:
    def __init__(self) -> None:
        #here may be set directory to save/read file
        pass

    def save_to_file(self,file_name, data) -> None:
        if file_name=='scraped_data.csv':
            df=pd.DataFrame(data)
            df.to_csv(file_name)
            logging.info(f"{file_name} saved!")
        elif file_name=='links_for_scraping.csv':
            with open('links_for_scraping.csv','w') as fi:
                writer=csv.writer(fi)
                writer.writerow(data)
        else:
            logging.error('No data saved!')

    def load_links_from_csv(self,file_name) -> list:
        if file_name=='scraped_links.csv':
            with open(file_name,'r') as fi:
                reader=csv.reader(fi)
                return list(reader)[0]
        else:
            logging.error('No data loaded!')


class get_advert_list:
    def __init__(self, driver) -> None:
        self.driver=driver
        self.links_for_scrap=generator(driver=driver)

    def scrap_page_for_advert_links(self,list_of_advert_links) -> None:
        self.driver.find_element(By.TAG_NAME, 'HTML').send_keys(Keys.END) #used to trigger scripts loading data on page
        time.sleep(0.5) #Without it WebDriverWait was not waitihk somehow
        WebDriverWait(driver=self.driver, timeout=15).until(ec.presence_of_all_elements_located((By.XPATH,'//a[contains(@data-cy, listing)]'))) #adust to your website
        selector=Selector(text=self.driver.page_source)
        for item in selector.xpath('//a[contains(@data-cy, "listing-item-link")]/@href').extract(): #adjust to your website
            list_of_advert_links.append(item)

    def iterate_throught_advert_pages(self,list_of_advert_links):
        for link in self.links_for_scrap:
            try:
                self.driver.get(link)
                self.scrap_page_for_advert_links(list_of_advert_links=list_of_advert_links)
            except Exception as error:
                logging.error(f"Page {link} failed to load due to following error {error}")
        return list_of_advert_links


if __name__ == "__main__":
    timeout=30
    list_of_advert_links=[]
    start_time = time.perf_counter()

    FORMAT = '%(asctime)s %(message)s'
    level=logging.INFO
    logging.basicConfig(filename='scraper_logs.log', filemode='w',level=level,format=FORMAT)
    logging.info("Script started!")

    data_files_handler_=data_files_handler()

    options=Options()
    options.add_argument("--window-size=1920,1080") #set your own
    options.add_argument("--headless")
    path=ChromeDriverManager().install()
    driver=webdriver.Chrome(executable_path=path,options=options)  
    driver.set_page_load_timeout(timeout)

    if os.path.exists('links_for_scraping.csv'):
        list_of_advert_links=data_files_handler_.load_links_from_csv('links_for_scraping.csv')
    else:
        get_advert_list_=get_advert_list(driver=driver)
        list_of_advert_links=get_advert_list_.iterate_throught_advert_pages(list_of_advert_links=list_of_advert_links)

    logging.info("Scraped links fo adverts in {} seconds ---".format((time.perf_counter() - start_time)))
    data_files_handler_.save_to_file('links_for_scraping.csv',list_of_advert_links)

    page_scraper_=page_scraper()
    outcome=asyncio.run(page_scraper_.get_advert_data(list_of_advert_links))

    data_files_handler_.save_to_file('scraped_data.csv',outcome)

    logging.info(list_of_advert_links)
    logging.info("Script succesfully completed in {} seconds ---".format((time.perf_counter() - start_time)))
