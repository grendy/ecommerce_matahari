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
                        # url = "https://fashion.mataharimall.com/adore-kemeja-panjang-neci-putih-1792625.html"
                        # url = "https://www.mataharimall.com/samsung-travel-charger-galaxy-s6-s7-s7edge-note-a-series-15w-2a-fast-charging-putih-original-1283858.html"
                        self.driver.get(url)
                        response = TextResponse(url=url, body=self.driver.page_source, encoding='utf-8')
                        # import pdb;pdb.set_trace()
                        try:
                            product_url = url
                            product = response.xpath('//*[contains(@class, "product-title")]/text()').extract_first().encode('utf-8')
                            harga = response.xpath('//*[contains(@class,"listing-item-price-web")]/text()').extract_first()
                            if harga == None:
                                harga = response.xpath('//*[contains(@class,"fshn-price")]/text()').extract_first()
                            deskripsi = response.xpath('//*[contains(@class,"field-item")]/p/text()').extract()
                            penjual_url = response.xpath('//*[contains(@class,"store-info")]/a/@href').extract_first().encode('utf-8')
                            penjual = response.xpath('//*[contains(@class,"store-info")]/a/text()').extract_first().encode('utf-8')
                            kategori = response.xpath('//*[contains(@class,"breadcrumbs-wrapper category-top")]/ul/li[2]/a/span/text()').extract_first().encode('utf-8')
                            harga = harga.replace("Rp ","").replace(".","").encode('utf-8')
                            deskripsi = ''.join(deskripsi).encode('utf-8')
                            if "Fashion Wanita" or "Fashion Pria" in kategori:pass
                            elif "Wanita" or "Pria" in kategori:
                                kategori = "Fashion " + kategori
                            dilihat = 0
                            terjual = 0
                            update_terakhir = None
                            lokasi = None
                            berat = 0
                            kondisi = "Baru"
                            product = product.replace("\\n", "").replace("\\\"", "").replace("\\", "")
                            kategori = kategori.replace("\n", "")
                            if "\n\n" in deskripsi:
                                deskripsi = []
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