# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from redis.client import Redis
from redis import BlockingConnectionPool

class SportPipeline(object):

    collection_name = 'sport_items'

    def __init__(self, redis_host, redis_port):
        self.redis_host = redis_host
        self.redis_port = redis_port

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            redis_host=crawler.settings.get('REDIS_HOST'),
            redis_port=crawler.settings.get('REDIS_PORT')
        )

    def open_spider(self, spider):
        self.client = Redis(host=self.redis_host, port=self.redis_port,
            connection_pool=BlockingConnectionPool())

    def close_spider(self, spider):
        self.client = None

    def process_item(self, item, spider):
        self.client.hset(item['key'], item['seq'], item)
        return item
