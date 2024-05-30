import scrapy
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables from .env file, if present
load_dotenv()

class AmharicNewsSpider(scrapy.Spider):
    name = "amharic_news"
    start_urls = ['https://www.bbc.com/amharic/']

    def __init__(self):
        super().__init__()
        # Establish a connection to the PostgreSQL database using environment variables
        self.conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),  # Or the IP address of your PostgreSQL container
            port=os.getenv('DB_PORT')
        )
        self.cur = self.conn.cursor(cursor_factory=RealDictCursor)

    def close(self, reason):
        # Close the cursor and connection when the spider is closed
        self.cur.close()
        self.conn.close()

    def parse(self, response):
        # Extract all article links from the page
        article_links = response.css('a[href*="/amharic/articles/"]::attr(href)').getall()
        
        for link in article_links:
            # Ensure the link is an absolute URL
            if not link.startswith('http'):
                link = response.urljoin(link)
            yield scrapy.Request(url=link, callback=self.parse_article)

    def parse_article(self, response):
        # Use Scrapy selector to find all paragraph elements
        paragraphs = response.css('p::text').getall()[0]
        title = response.css('title::text').get()  # Get the first title element
        header = response.css('h1::text').get()  # Get the first h1 element
        categories = response.css('li.bbc-1g3396x a::text').getall()
        categories_str = ', '.join(categories)
        date_tag = response.css('time.bbc-fqsgh5::attr(datetime)').get()
        if date_tag:
            date_posted = datetime.strptime(date_tag, '%Y-%m-%d').date()  # Convert to date object
        else:
            date_posted = None
        # Prepare the data to be inserted
        data_to_insert = {
            'title': title,
            'header': header,
            'content': ''.join(paragraphs),
            'categories': categories_str,
            'date':date_posted,
            'url': response.url,  
        }

        # Insert the data into the database
        self.cur.execute("""
            INSERT INTO raw_amharic_data (title, header, content, categories, date_posted, url)
            VALUES (%(title)s, %(header)s, %(content)s, %(categories)s, %(date)s, %(url)s)
        """, data_to_insert)

        # Commit the transaction
        self.conn.commit()

        self.log(f'Scraped item: {data_to_insert}')  # Log the item to the console
