import json, os

import datetime as dt 
import polars   as pl 

from config.SeleniumSettings    import SeleniumSettings 
from config.DBInterfacePostgres import DBInterface

class KakaoScraper:
    def __init__(self, selenium_object: SeleniumSettings):
        self.selenium_object = selenium_object 
        self.current_date    = str(dt.datetime.now().date())
        self.selenium_object.driver_settings()

    def kakao_scraper_settings_method(self, output_file_name: str, sql_type: str, server_name: str, hostname: str, table_name: str, schema_name: str, kakao_pages_dict: dict):
        self.output_file_name = output_file_name 
        self.kakao_pages_dict = kakao_pages_dict 
        self.table_name       = table_name
        self.schema_name      = schema_name 
        # self.__db_interface   = DBInterface()
        # self.__db_interface.connection_settings(sql_type, os.getenv("ADMIN_NAME"), os.getenv("ADMIN_PWD"), hostname, server_name)

        self.kakao_data_dictionary = {
            "brand_name"      : [],
            "extraction_date" : [],
            "friends_count"   : []
        }

    def kakao_friend_count_extraction(self):
        for (brand_name, page_url) in self.kakao_pages_dict.items():
            self.selenium_object.driver.get(page_url)
            friend_count = int(self.selenium_object.wait_for_element_and_return_element("txt_friends").text.split(" ")[1].replace(",", ""))
            results_list = [brand_name, self.current_date, friend_count]

            for (key, value) in zip(self.kakao_data_dictionary.keys(), results_list):
                self.kakao_data_dictionary[key].append(value)

    def save_data(self):
        file_type   = self.output_file_name.split(".")[-1].lower()
        output_data = pl.DataFrame(self.kakao_data_dictionary)
        # self.__db_interface.upload_to_database(self.table_name, output_data, schema_name = self.schema_name)

        match (file_type):
            case "xlsx" : output_data.write_excel(self.output_file_name.format(self.current_date))
            case "csv"  : output_data.write_csv(self.output_file_name.format(self.current_date))

if (__name__ == "__main__"):
    with open("./config/KakaoScraperConfig.json", "r", encoding = "utf-8") as f:
        config_dict = json.load(f)
    
    scraper_object = KakaoScraper(SeleniumSettings(**config_dict["KakaoScraper"]["constructor"]))
    scraper_object.kakao_scraper_settings_method(**config_dict["KakaoScraper"]["kakao_scraper_settings_method"])
    scraper_object.kakao_friend_count_extraction()
    scraper_object.save_data()