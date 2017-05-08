# -*- coding: utf-8 -*-
import json
from urllib import urlencode

import scrapy
import time
from scrapy import Request, Selector
from scrapy.http.cookies import CookieJar

from appspider.items import AppItem


class AnnieSpider(scrapy.Spider):
    name = "annie"

    start_urls = 'https://www.appannie.com/account/login'
    category_base_urls = 'https://www.appannie.com/apps/google-play/top-chart'
    table_url = 'https://www.appannie.com/ajax/top-chart/table/?'

    username = "huiyin@lewatek.com"
    password = "ziwatek1654"

    def start_requests(self):
        return [Request(self.start_urls, callback=self.start_login)]

    def start_login(self, response):
        cookie_list = []
        for cl in response.headers.getlist('Set-Cookie'):
            cookie_list.append(cl.split(';')[0])
        cookies = dict(item.split("=") for item in cookie_list)
        token = cookies['csrftoken']
        auth = {
            "next": '/apps/google-play/top-chart/',
            "username": self.username,
            "password": self.password,
            "csrfmiddlewaretoken": token
        }
        meta = {
            'csrftoken': token
        }
        return scrapy.FormRequest(self.start_urls, callback=self.request_category, formdata=auth, meta=meta)

    def request_category(self, response):
        category_raw_str = Selector(response=response).xpath('/html/body/script[4]').extract()[0].split(';')
        category_json = json.loads(category_raw_str[8].split('filterSettings = ')[1])
        metadata_json = json.loads(category_raw_str[9].split('pageVal.metadata = ')[1])
        app_types = category_json['category']['context'][1][2]
        family_types = category_json['category']['context'][2][2]
        game_types = category_json['category']['context'][3][2]
        country_list = category_json['countries']['context']['items'][1][2]

        date = time.strftime("%Y-%m-%d", time.localtime(time.time() - 3600 * 24))
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'X-CSRFToken': response.meta['csrftoken']
        }
        for category in app_types:
            params = {
                "market": "google-play",
                "country_code": 'US',
                "category": category[0],
                "date": date,
                "rank_sorting_type": "rank",
                "page_size": 500,
                "order_by": "sort_order",
                "order_type": "desc",
                "page_number": 0,
            }
            url = self.table_url + urlencode(params)
            yield scrapy.Request(url=url, callback=self.parse_table, headers=headers, meta={'category': category[0]})
        pass

    def parse_table(self, response):
        table = json.loads(response.body)
        for row in table['table']['rows']:
            for app in row[1:]:
                info = app[0]
                if 'url' in info:
                    item = AppItem(
                        package_name=info['url'].replace('/apps/google-play/app/', '').replace('/details/', ''),
                        icon=info['icon'], company_name=info['company_name'], category=response.meta['category'])
                yield item
        pass
