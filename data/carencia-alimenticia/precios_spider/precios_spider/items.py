# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class PreciosSpiderItem(scrapy.Item):
    fecha = scrapy.Field()
    origen = scrapy.Field()
    edo_destino = scrapy.Field()
    destino = scrapy.Field()
    precio_min = scrapy.Field()
    precio_max = scrapy.Field()
    precio_frec = scrapy.Field()
    obs = scrapy.Field()
    producto = scrapy.Field()

    