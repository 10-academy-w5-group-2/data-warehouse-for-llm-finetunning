import scrapy
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

# Load environment variables from .env file, if present
load_dotenv()

class AmharicNewsSpider(scrapy.Spider):
    name = "amharic_news"
    start_urls = ['https://www.bbc.com/amharic/articles/cg33ge4l23vo']

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
        # Use Scrapy selector to find all paragraph elements
        paragraphs = response.css('p::text').getall()[0]
        title = response.css('title::text').getall()[0]  # Get the first title element
        header = response.css('h1::text').getall()[0]  # Get the first h1 element
        categories = response.css('li.bbc-1g3396x a::text').getall()
        categories_str = ', '.join(categories)
        # Prepare the data to be inserted
        data_to_insert = {
            'title': title,
            'header': header,
            'content': ''.join(paragraphs),
            'categories': categories_str
        }

        # Insert the data into the database
        self.cur.execute("""
            INSERT INTO raw_amharic_data (title, header, content, categories)
            VALUES (%(title)s, %(header)s, %(content)s, %(categories)s)
        """, data_to_insert)

        # Commit the transaction
        self.conn.commit()

        self.log(f'Scraped item: {data_to_insert}')  # Log the item to the console