# Async Web Scraper

This is a web scraper designed to scrape real estate data from the X website. The scraper is built using Python and the following packages:

   - selenium
   - webdriver_manager
   - parsel
   - pandas
   - numpy
   - aiohttp
   - logging
   - asyncio
   - json
   - time
   - csv
   - os
   
In addition, Google Chrome needs to be installed, and the chrome driver version needs to be compatible with the Google Chrome browser. The program will automatically download the required Chrome driver version using the "webdriver_manager" package.

## What websites might work

Scraper was designed to work with specific types of websites:
- list of adverts is displayed on separated pages
- website uses javascript (if it doesn't there are faster methods that do not need selenium)
- there is a way to get advert data with GET method

## Usage

1) set AMOUNT_OF_PAGES to the number of pages you can/want to scrap - line 23 (if there is 1000 ads and 10 per page, there is 100 pages to scrape)
2) adjust json keys values - line 30 - that is what will be taken from json response, if your API returns different type then you need to change response parsing logic (line 70)
3) implement your own logic for requests (return 'yourwebsite.usa/api/call'+sth') on line 37
4) adjust xpath - line 119 and 121 
5) run script, it should be working!

The program will first scrape links to all available apartment advertisements. It will then use aiohttp to download the advertisement data from the website's json. Finally, the collected data is parsed into a CSV file.

## Outcome

The program will create two CSV files:

   - "links_for_scraping.csv" - contains the links to all available apartment advertisements 
   - "scraped_data.csv" - contains the parsed advertisement data

## Disclaimer
That script was written by amateur for as a hobby project. Program may run slow when compared to scrapers written by profesionals.
