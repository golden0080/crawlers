# -*- coding: utf-8 -*-
import scrapy
from scrapy.selector import Selector

import os


class AptCrawlerSpider(scrapy.Spider):
    name = 'apt_crawler'
    allowed_domains = ['craigslist.org']
    custom_settings = {
        'DOWNLOAD_DELAY':
        0.1,
        'FEED_URI':
        '/home/gh/projects/craigslist/craigs/data/%(name)s-%(area_code)s-%(area_zip)s-%(availability)s-%(time)s.jl',
        'FEED_FORMAT':
        'jsonlines',
    }
    START_URL_TEMPLATE = 'https://{area_code}.craigslist.org/search/apa?query={area_zip}&availabilityMode={availability}&sale_date=all+dates'

    DEFAULT_AREA = 'sfbay'
    DEFAULT_ZIP = '94121'
    DEFAULT_AVAILABILITY = 0
    AVAILABLE_WITHIN_30 = 1
    AVAILABLE_BEYOND_30 = 2

    def __init__(self, *args, **kwargs):
        area_code = kwargs.get('area_code', AptCrawlerSpider.DEFAULT_AREA)
        area_zip = kwargs.get('area_zip', AptCrawlerSpider.DEFAULT_ZIP)
        availability_mode = kwargs.get('availability',
                                       AptCrawlerSpider.DEFAULT_AVAILABILITY)
        self.area_code = area_code
        self.area_zip = area_zip
        self.availability = availability_mode

    def start_requests(self):
        urls = [
            AptCrawlerSpider.START_URL_TEMPLATE.format(
                area_code=self.area_code,
                area_zip=self.area_zip,
                availability=self.availability)
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parseList)

    def parseList(self, response):
        all_results = response.css('li.result-row').extract()
        for idx, row in enumerate(all_results):
            sel = Selector(text=row)
            data_pid = sel.css('.result-row::attr(data-pid)').extract_first()
            title = sel.css('a.result-title::text').extract_first()
            post_link = sel.css('a.result-title::attr(href)').extract_first()
            yield scrapy.Request(post_link, callback=self.parsePost)

            housing = raw_housing = sel.css('.housing::text').extract_first()
            if raw_housing:
                parts = raw_housing.split('\n')
                housing = [p.strip() for p in parts]
                housing = [p.strip('- ') for p in parts]
                housing = [p for p in housing if p]

            price = raw_price = sel.css('.result-price::text').extract_first()
            if raw_price.startswith('$'):
                price = raw_price[1:]

            yield {
                'pid': data_pid,
                'type': 'list',
                'price': price,
                'housing-type': housing,
                'neighborhood': sel.css('.result-hood::text').extract_first(),
                'title': title,
            }

        # Get next page
        next_page_link = response.css(
            'a.button.next::attr(href)').extract_first()
        yield response.follow(next_page_link, self.parseList)

    def parsePost(self, response):
        # TODO(gh): maybe with some NLP
        map_and_attrs = Selector(
            text=response.css('.mapAndAttrs').extract_first())

        map_tag = Selector(text=map_and_attrs.css('#map').extract_first())
        attr_group_pair = map_and_attrs.css('.attrgroup').extract()

        raw_reply_link = response.url
        parts = raw_reply_link.split('/')
        pid = None
        if len(parts) > 0:
            pid = parts[-1]
            if pid.endswith('.html'):
                pid = pid[:-5]

        house_types = []
        available_date = None
        tags = []
        if len(attr_group_pair) > 0:
            housing_type_attr_group = Selector(text=attr_group_pair[0])
            house_types = housing_type_attr_group.css(
                '.shared-line-bubble>b::text').extract()
            available_date = housing_type_attr_group.css(
                '.shared-line-bubble::attr(data-date)').extract_first()
        if len(attr_group_pair) > 1:
            other_attr_group = Selector(text=attr_group_pair[-1])
            tags = other_attr_group.css('span::text').extract()

        yield {
            'pid': pid,
            'type': 'post',
            'housing': house_types,
            'tags': tags,
            'available-date': available_date,
            'latitude': map_tag.css('::attr(data-latitude)').extract_first(),
            'longitude': map_tag.css('::attr(data-longitude)').extract_first(),
        }
