import scrapy
import json
import time
from scrapy.http import FormRequest
# from kafka import KafkaProducer
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from scrapy.http import TextResponse
import traceback
from pyvirtualdisplay import Display
from selenium.webdriver.common.keys import Keys
import MySQLdb
import requests
import time


class ProductSpider(scrapy.Spider):
    name = "coy"
    allowed_domains = ["https://www.mataharimall.com"]
    start_urls = ["https://www.mataharimall.com"]

    def __init__(self,conn):
        self.conn = conn
        # path_to_chromedriver = 'D://chromedriver'
        # self.driver = webdriver.Chrome(executable_path = path_to_chromedriver)
        # self.driver = webdriver.PhantomJS()
        display = Display(visible=0, size=(800,600))
        display.start()
        self.driver = webdriver.Firefox()

    @classmethod
    def from_crawler(cls, crawler):
        conn = MySQLdb.connect(
            host=crawler.settings['MYSQL_HOST'],
            port=crawler.settings['MYSQL_PORT'],
            user=crawler.settings['MYSQL_USER'],
            passwd=crawler.settings['MYSQL_PASS'],
            db=crawler.settings['MYSQL_DB'])
        return cls(conn)

    def parse(self, response):
        cursor = self.conn.cursor()
        try:
            a = 0
            for tidur in range(0, 100):
                time.sleep(1)
                try:
                    sql = "select url from matahari_category"
                    cursor.execute(sql)
                    results = cursor.fetchall()
                    for ulang in range(0, 31):
                        a = results[ulang]
                        url = str(a).replace(",", "").replace("'", "").replace("(", "").replace(")", "")
                        print "====================================="
                        print(url)
                        print "====================================="
                        self.driver.get(url)
                        # import pdb;pdb.set_trace()
                        try:
                            count = 0
                            for halaman in range(1, 400):
                                try:
                                    for i in range(1,100):
                                        response = TextResponse(url=response.url, body=self.driver.page_source,encoding='utf-8')
                                        product_url = response.xpath('//*[contains(@id,"products-wrap")]/div/div[2]/div[2]/section/article['+str(i)+']/div[2]/a/@href').extract_first()
                                            # //*[@id="products-wrap"]/div/div[2]/div[2]/section/article[1]/a
                                        if product_url == None:
                                            product_url = response.xpath('// *[contains(@id,"products-wrap")]/div/div[2]/div[2]/section/article['+str(i)+']/a/@href').extract_first()
                                        elif product_url == None:
                                            product_url = response.xpath('//*[contains(@id,"block-system-main")]/div/div[1]/div/div[2]/div/div/div['+str(i)+']/div/a/@href').extract_first()
                                        try:
                                            product_url = product_url.split('ct=')[1]
                                            product_url = product_url.split('&vg')[0]
                                            product_url = product_url.replace("%3A%2F%2F","://").replace("%2F","/").encode('utf-8')
                                        except Exception, e:
                                            print e
                                        count +=1
                                        # if count == 200:
                                        # import pdb;pdb.set_trace()
                                        # else:
                                        print count
                                        if product_url != None:
                                            if "https://www.mataharimall.com" in product_url:
                                                pass
                                            else:
                                                product_url = "https://www.mataharimall.com" + product_url
                                            try:
                                                status = ""
                                                status_feed = ""
                                                product_url = product_url.encode('utf-8')
                                                sql = "INSERT INTO `matahari_url`(`product_url`, `status`,`status_feed`) VALUES ('{}','{}','{}') ".format(product_url,status,status_feed)
                                                cursor.execute(sql)
                                                self.conn.commit()
                                                print "======================================="
                                                print product_url
                                                print "INSERT SUKSES"
                                                print "======================================="
                                            except:
                                                print "==============================================================================="
                                                print "Data Duplicate"
                                                print product_url
                                                print "==============================================================================="
                                                pass
                                except:
                                    pass
                                # import pdb;pdb.set_trace()
                                try:
                                    coy = url + "?per_page=100&page=" + str(halaman+1) + "&date_filter="
                                    self.driver.get(coy)
                                    hal = self.driver.current_url
                                    batas = hal.split('&page=')[1]
                                    batas = batas.split('&date_filter=')[0].encode('utf-8')
                                    batas = int(batas)
                                    plus = int(halaman + 1)
                                    if batas != (plus):
                                        break
                                except:
                                    pass
                        except:
                            pass
                except:
                        pass

        except:
            pass
        db.close()
        try:
            self.driver.close()
        except:
            pass
