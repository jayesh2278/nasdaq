import scrapy
import json
import requests
import os
import time
import re
import pandas as pd
from seleniumwire import webdriver
from selenium.webdriver.common.by import By


class EvestmentSpider(scrapy.Spider):
    name = "evestment"

    def convert_headers_to_dict(self,headers_text):
        headers_dict = dict(re.findall(r'(?P<name>.*?): (?P<value>.*?)\r?\n', headers_text))
        headers_dict.pop('accept-encoding', None)  
        headers_dict.pop('content-length',None)
        headers_dict['dnt']='1'
        return headers_dict
    
    def get_header(self):
            # url = "https://www.nasdaq.com/solutions/evestment"
            url = "https://app.evestment.com/next/autologin.aspx"

            # Set up Chrome options
            options = webdriver.ChromeOptions()
            options.add_argument('--headless')
            driver = webdriver.Chrome(options=options)

            driver.get(url)
            # time.sleep(10)
            # login_link = driver.find_element(By.XPATH, ".//a[@class='jupiter22-c-nav__cta-button ']")
            # login_link.click()
            time.sleep(10)
            # driver.switch_to.window(driver.window_handles[1])
            username_input = driver.find_element(By.XPATH,'.//input[@placeholder="username/email"]')
            username_input.click()
            username_input.send_keys('lisa.hochhauser@blackrock.com')

            user_password = driver.find_element(By.XPATH,'.//input[@placeholder="your password"]')
            user_password.click()
            user_password.send_keys('Infra2020')

            driver.find_element(By.XPATH,'.//button[@aria-label="Log In"]').click()

            time.sleep(20)

            driver.find_element(By.XPATH,'.//a[contains(text(), "Continue")]').click()

            time.sleep(10)

            driver.get('https://app.evestment.com/Analytics/#!UHJvZmlsZVNlYXJjaA')
            time.sleep(10)

            driver.find_element(By.XPATH,'//div[text() = "Documents"]').click()
            time.sleep(10)
            headers_text = ""
            for request in driver.requests:
                    if request.response:
                        if (request.url).startswith('https://app.evestment.com/analytics/globalsearch/api/globalsearch/documents?_dc'):
                            headers_text = request.headers
                            print(type(headers_text))
                            break

            headers = self.convert_headers_to_dict(str(headers_text))
            driver.quit()
            return headers

    def start_requests(self):
        self.headers = self.get_header()
        print("###############################       #################")
        print(self.headers)
        print("#####################            #################   #########")
        yield scrapy.Request(url="https://example.com/",callback=self.parse)

    def is_file_present(self,folder_path, file_name):
        file_path = os.path.join(folder_path, file_name)
        return os.path.isfile(file_path)
    
    def check_conditions(self,id):
        if not os.path.isfile('output.csv'):
            return True
        else:
            df = pd.read_csv('output.csv')
            if id in df['id'].values:
                return False
            return True   
    
    def parse(self,response):
        url = "https://app.evestment.com/analytics/globalsearch/api/globalsearch/documents?_dc=1706329659506"
        
        payload = json.dumps({
        "searchSessionId": "90b2e9ed-57cc-4f34-87dc-f902d6943336",
        "excludeDocumentTypes": [],
        "excludedFilterCodes": [],
        "skip": 0,
        # How many results you want to get from API, MAX=10000
        "take": 10000,
        "sort": "[{\"property\":\"_score\",\"direction\":\"DESC\"}]",
        "filter": "[{\"property\":\"searchText\",\"value\":\"\"},{\"property\":\"resultViewType\",\"value\":\"detailed\"},{\"property\":\"selectedCurrencyId\",\"value\":1},{\"property\":\"selectedFrequencyId\",\"value\":1}]"
        })
        
        response = requests.request("POST", url, headers=self.headers, data=payload)

        data = json.loads(response.text)
        for item in data.get("page").get("items"):
                id = item.get('id')
                displyname = item.get("displayName")
                date = item.get('date')
                pagecount = item.get("pageCount")
                consultant = item.get("consultantFirm")
                if consultant:
                        consultant = consultant.get('text')
                else:
                        consultant = None      
                publicplan = item.get("publicPlan")
                authortype = publicplan.get("authorType").get('text')
                authorDescription = publicplan.get("authorDescription")
                stretergy = publicplan.get("strategyDescription")
                notes = publicplan.get("comments")

                
                folder_path = "downloaded_files"
                os.makedirs(folder_path, exist_ok=True)
                
                if self.check_conditions(id):
                    yield {
                        "id": id,
                        "displyname": displyname,
                        "date": date,
                        "pagecount": pagecount,
                        "consultant": consultant,
                        "authortype": authortype,
                        "authorDescription": authorDescription,
                        "stretergy": stretergy,
                        "notes": notes,
                        "headers":self.headers,
                    } 
                else:
                    print("already present")