import scrapy
from logging import exception
import datetime
from kafka import KafkaProducer, KafkaConsumer
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.proxy import *
from scrapy.http import TextResponse
from pyvirtualdisplay import Display
from scrapy.utils.project import get_project_settings
from impala.dbapi import connect
import setting
import traceback
import MySQLdb
import sys
import json
import demjson

import time


class producer:
    def __init__(self):
        self.conn = MySQLdb.connect(
            host=setting.host,
            port=setting.port,
            user=setting.user,
            passwd=setting.passwd,
            db=setting.db)
        self.connect = self.conn
        # path_to_chromedriver = 'D://chromedriver'
        # self.driver = webdriver.Chrome(executable_path = path_to_chromedriver)
        # self.driver = webdriver.Chrome()
        # driver.get("http://www.google.com")
        # service_args = [setting.proxy]
        # self.driver = webdriver.PhantomJS(service_args = service_args)
        myProxy = setting.firefox_proxy
        proxy = Proxy({
            'proxyType': ProxyType.MANUAL,
            'httpProxy': myProxy,
            'ftpProxy': myProxy,
            'sslProxy': myProxy,
        })
        display = Display(visible=0, size=(800, 600))
        display.start()
        self.driver = webdriver.Firefox(proxy=proxy)
    def parse(self):
        cur = self.conn.cursor()
        cou = self.conn.cursor()
        try:
            # import pdb;pdb.set_trace()
            count = "select count(*)from matahari_url where status = ''"
            sql = "select product_url from matahari_url where status = ''"
            cur.execute(sql)
            cou.execute(count)
            results = cur.fetchall()
            b = cou.fetchall()
            terus = str(b).replace(",", "").replace("'", "").replace("(", "").replace(")", "").replace("[", "").replace("]", "").replace("L", "")
            print (terus)
            terus = int(terus)
            print "============================================"
            print (terus)
            # import pdb;pdb.set_trace()
            count = 0
            for ulang in range(0, terus):
                try:
                    print (ulang)
                    count += 1
                    a = results[ulang]
                    url = str(a).replace(",", "").replace("'", "").replace("(", "").replace(")", "")
                    try:
                        # url = "https://www.mataharimall.com/yamaha-all-new-soul-gt-125-bravery-black-2016-otr-jabodetabek--281443.html"
                        self.driver.get(url)
                        response = TextResponse(url=url, body=self.driver.page_source, encoding='utf-8')
                        # import pdb;pdb.set_trace()
                        try:
                            #ambil detail product dan penjual
                            # self.driver.save_screenshot('SCEEN1.png')
                            # import pdb;pdb.set_trace()
                            product_url = url
                            try:
                                penjual_url = MySQLdb.escape_string(response.xpath('//*[contains(@class,"store-info")]/a/@href').extract_first())
                            except:
                                penjual_url = None
                            try:
                                penjual = MySQLdb.escape_string(response.xpath('//*[contains(@class,"store-info")]/a/text()').extract_first())
                            except:
                                penjual = None
                            product = MySQLdb.escape_string(response.xpath('//*[contains(@class,"product-title")]/text()').extract_first())
                            harga = response.xpath('//*[contains(@class,"listing-item-price-web")]/text()').extract_first()
                            if harga == None:
                                harga = response.xpath('//*[contains(@class,"fshn-price")]/text()').extract_first()
                            kondisi = "Baru"
                            try:
                                kategori = MySQLdb.escape_string(response.xpath('//*[contains(@id,"products-wrap")]/div/div[1]/div/div/ul/li[1]/a/span/text()').extract_first())
                            except:
                                kategori = MySQLdb.escape_string(response.xpath('//*[contains(@id,"header")]/div[3]/div/div/div[2]/div/ul/li[2]/a/span/text()').extract_first())
                            if "Wanita" or "Pria" in kategori:
                                pass
                            else:kategori = "Fashion " + kategori
                            dilihat = 0
                            terjual = 0
                            update_terakhir = None
                            lokasi = None
                            berat = 0
                            import pdb;pdb.set_trace()
                            deskripsi1 = response.xpath('//*[contains(@class,"field-item even")]/p/text()').extract()
                            deskripsi = response.xpath('//*[contains(@class,"field-item even")]/following-sibling::*/descendant-or-self::text()').extract()#/p[normalize-space()]
                            if deskripsi != []:
                                deskripsi = deskripsi1 + deskripsi
                            else:
                                deskripsi = deskripsi1
                            deskripsi = ''.join(deskripsi)
                            deskripsi = deskripsi.replace("\n","").replace("\t"," ").encode('utf-8')
                            product = product.replace("\\n", "").replace("\\\"", "").replace("\\", "field-item even")
                            kategori = kategori.replace("\n", " ")
                            harga = harga.replace(".", "").replace("Rp ","").encode('utf-8')
                            if "\n\n" in deskripsi:
                                deskripsi = ''
                            print "=================================================="
                            akhir = json.dumps(
                                {'type': 'mataharimall', 'product_url': product_url, 'penjual_url': penjual_url,'kategori': kategori,
                                 'product_name': product, 'harga': int(harga),'update_terakhir':update_terakhir, 'berat': int(berat),
                                 'kondisi': kondisi,'dilihat':dilihat,'terjual':terjual,'deskripsi':deskripsi,'penjual': penjual,
                                 'lokasi': lokasi})
                            try:
                                for kaf in range(0, 20):
                                    try:
                                        prod = KafkaProducer(bootstrap_servers=setting.broker)
                                        prod.send(setting.kafka_topic, b"{}".format(akhir))
                                        print "=================================================="
                                        print "SUKSES SEND TO KAFKA"
                                        print "=================================================="
                                        print akhir
                                        status = "done"
                                        sql = "UPDATE matahari_url SET status = '{}' WHERE product_url = '{}'".format(status, url)
                                        cur.execute(sql)
                                        self.conn.commit()
                                        kaf = 1
                                    except:
                                        pass
                                    if kaf == 1:
                                        break
                            except Exception, e:
                                print e
                        except Exception, e:
                            print e
                    except:
                        pass
                except Exception, e:
                    print e
        except Exception, e:
            print e
        cur.close()
        try:
            self.driver.close()
        except Exception, e:
            print e


if __name__ == '__main__':
    p = producer()
    p.parse()