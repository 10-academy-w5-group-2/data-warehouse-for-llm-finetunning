import psycopg2
from psycopg2.extras import execute_values

import os
from dotenv import load_dotenv

load_dotenv()

db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')
db_port = os.getenv('DB_PORT')


class DatabaseManager:
    def __init__(self):
        self.connection = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        self.connection.autocommit = True

    async def insert_raw_text_data(self, content, source_id, date_collected):
        with self.connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO raw_text_data (content, source_id, date_collected) VALUES (%s, %s, %s) RETURNING id;",
                (content, source_id, date_collected)
            )
            return cursor.fetchone()[0]

    async def insert_language(self, language_name):
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(
                    "INSERT INTO languages (language_name) VALUES (%s) RETURNING id;",
                    (language_name,)
                )
                return cursor.fetchone()[0]
            except psycopg2.errors.UniqueViolation:
                cursor.execute(
                    "UPDATE languages SET language_name = %s WHERE language_name = %s RETURNING id;",
                    (language_name, language_name)
                )
                return cursor.fetchone()[0]  

    async def insert_source(self, source_name, source_type, language_id):
        with self.connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO sources (source_name, source_type, language_id) VALUES (%s, %s, %s) RETURNING id;",
                (source_name, source_type, language_id)
            )
            return cursor.fetchone()[0]

    async def insert_cleaned_text_data(self, raw_id, content):
        with self.connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO cleaned_text_data (raw_id, content) VALUES (%s, %s) RETURNING id;",
                (raw_id, content)
            )
            return cursor.fetchone()[0]

    async def insert_audio_data(self, audio_path, transcript, source_id, date_collected):
        with self.connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO audio_data (audio_path, transcript, source_id, date_collected) VALUES (%s, %s, %s, %s) RETURNING id;",
                (audio_path, transcript, source_id, date_collected)
            )
            return cursor.fetchone()[0]

    async def insert_data_source(self, source_name, source_url, last_scraped):
        with self.connection.cursor() as cursor:
            try:
                await cursor.execute(
                    "INSERT INTO data_sources (source_name, source_url, last_scraped) VALUES (%s, %s, %s) RETURNING id;",
                    (source_name, source_url, last_scraped)
                )
                return cursor.fetchone()[0]
            except psycopg2.errors.UniqueViolation:
                # If the data source already exists, update its last_scraped value
                cursor.execute(
                    "UPDATE data_sources SET last_scraped = %s WHERE source_name = %s;",
                    (last_scraped, source_name)
                )
            self.connection.commit()  # Commit the transaction

    async def insert_news_data(self, title, url, publication_date, content):
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(
                    "INSERT INTO news_raw_data (title, url, publication_date, content) VALUES (%s, %s, %s, %s) RETURNING id;",
                    (title, url, publication_date, content)
                )
                return cursor.fetchone()[0]
            except psycopg2.Error as e:
                print("Error inserting news data:", e)
                return None

    async def get_raw_text_data(self):
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT * FROM raw_text_data;")
            return cursor.fetchall()
    
    def close(self):
        self.connection.close()

# Usage example
if __name__ == "__main__":
    db = Database(dbname="your_db", user="your_user", password="your_password", host="localhost", port="5432")

    # Insert into languages
    language_id = db.insert_language("Amharic")

    # Insert into sources
    source_id = db.insert_source("Source Name", "Type", language_id)

    # Insert into raw_text_data
    raw_text_id = db.insert_raw_text_data("This is raw text content.", source_id, "2024-05-21")

    # Insert into cleaned_text_data
    cleaned_text_id = db.insert_cleaned_text_data(raw_text_id, "This is cleaned text content.")

    # Insert into audio_data
    audio_id = db.insert_audio_data("/path/to/audio", "This is a transcript.", source_id, "2024-05-21")

    # Insert into data_sources
    data_source_id = db.insert_data_source("Source Name", "http://sourceurl.com", "2024-05-21 12:00:00")

    db.close()
