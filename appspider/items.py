# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class AppItem(scrapy.Item):
    package_name = scrapy.Field()
    icon = scrapy.Field()
    company_name = scrapy.Field()
    category = scrapy.Field()
    rank = scrapy.Field()
    rank_type = scrapy.Field()
