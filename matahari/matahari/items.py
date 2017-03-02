# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class MatahariItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass
class tcategory(scrapy.Item):
	nama_kategori = scrapy.Field()
	name = scrapy.Field()

class tproduct(scrapy.Item):
	product_url = scrapy.Field()
	kategori_url = scrapy.Field()
	# kategori = scrapy.Field()
	# kategori_url = scrapy.Field()
	# nama_product = scrapy.Field()
	# harga = scrapy.Field()
	# kondisi = scrapy.Field()
	# berat = scrapy.Field()
	# dilihat = scrapy.Field()
	# update_terakhir = scrapy.Field()
	# terjual = scrapy.Field()
	# deskripsi = scrapy.Field()
	# feed_nama = scrapy.Field()
	# tanggal = scrapy.Field()
	# sentimen = scrapy.Field()
	# pesan = scrapy.Field()
	# penjual = scrapy.Field()
	# lokasi = scrapy.Field()
	# produk_terjual = scrapy.Field()
