import scrapy
from datetime import datetime

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

from database.database_manager import DatabaseManager


class NewsSpider(scrapy.Spider):
    name = "news_spider"
    start_urls = [
        'https://www.capitalethiopia.com',
        'https://ethiopianmonitor.com',
        'https://www.ethiopianreporter.com',
        'https://ethsat.com',
        'https://www.aigaforum.com'
    ]

    def __init__(self, *args, **kwargs):
        super(NewsSpider, self).__init__(*args, **kwargs)
        self.db_manager = DatabaseManager()

    async def parse(self, response):
        for article in response.xpath('//div[@class="article"]'):
            title = article.xpath('.//h2/text()').get()
            url = article.xpath('.//a/@href').get()
            publication_date = article.xpath('.//span[@class="date"]/text()').get()
            content = article.xpath('.//div[@class="content"]/text()').get()
            
            # Insert news data into the database
            news_id = await self.db_manager.insert_news_data(title, url, publication_date, content)
            
            if news_id:
                print("Inserted news data with ID:", news_id)
            else:
                print("Failed to insert news data")

            yield {
                'title': title,
                'url': url,
                'publication_date': publication_date,
                'content': content
            }

    async def insert_data_source(self, source_name, source_url, last_scraped):
        source_id = self.db_manager.insert_data_source(source_name=source_name, source_url=source_url, last_scraped=last_scraped)
        return source_id
