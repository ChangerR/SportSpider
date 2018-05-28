#! /usr/bin/env python
# -*- coding: utf-8 -*-
import scrapy
import json
from datetime import date, timedelta

def crawl_date(begin):
    while begin < date.today():
        yield begin.isoformat()
        begin = begin + timedelta(days = 1)
    
class SportSpider(scrapy.Spider):
    name = 'sport_spider'

    #start_urls = ['http://odds.500.com/index_history_2018-05-13.shtml']
    #start_urls = ['http://trade.500.com/jczq/?date=2018-05-13&playtype=both']
    
    def start_requests(self):
        for day in crawl_date(date(2014, 1, 1)):
            yield scrapy.Request(url='http://trade.500.com/jczq/?date=%s&playtype=both' % (day), 
                callback=self.parse)

    def parse(self, response):
        key = response.css('div.bet_date').xpath('@date').extract_first()
        for table in response.css('table.bet_table'):
            for tr in table.css('tr'):
                tds = tr.css('td')
                if len(tds) == 12:
                    bet = {
                        'date' : tr.xpath('@pdate').extract_first(),
                        'key' : key
                    }
                    bet['seq'] = tds[0].xpath('./a/text()').extract_first()
                    bet['league'] = tds[1].xpath('./span/a/text()').extract_first()
                    bet['time'] = tds[2].xpath('./span/text()').extract_first()
                    bet['homexname'] = tds[3].xpath('./a/@title').extract_first()
                    bet['homexname_alias'] =  tds[3].xpath('./a/text()').extract_first()
                    bet['score'] = tds[4].xpath('./a/text()').extract_first()
                    bet['awaysxname'] = tds[5].xpath('./a/@title').extract_first()
                    bet['awaysxname_alias'] =  tds[5].xpath('./a/text()').extract_first()
                    bet['concede'] = tds[6].xpath('./p//text()').extract()
                    bet['bet'] = tds[7].xpath('./div/span/text()').extract()
                    bet['misc'] = tds[8].xpath('./a/@href').extract()
                self.log(json.dumps(bet))
                request = scrapy.Request(url=bet['misc'][1], callback=self.parse_yazhi)
                request.meta['item'] = bet
                yield request

    def parse_yazhi(self, response):
        bet = response.meta['item']
        for table in response.css('table#datatb'):
            bet['yazhi'] = []
            for tr in table.xpath('./tr[count(td)=$cnt]', cnt=7):
                tds = tr.xpath('./td')
                row = {}
                row['no'] = tds[0].xpath('./p/text()')[1].extract().strip()
                row['plgs'] = tds[1].xpath('./p/a/@title').extract_first()
                row['imm_data'] = tds[2].xpath('./table/tbody/tr/td/text()').extract()
                row['imm_data_chg_time'] = tds[3].xpath('./time/text()').extract_first()
                row['org_data'] = tds[4].xpath('./table/tbody/tr/td/text()').extract()
                row['org_time'] = tds[5].xpath('./time/text()').extract_first()
                bet['yazhi'].append(row)
        yield bet