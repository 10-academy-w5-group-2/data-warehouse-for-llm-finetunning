import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import re

# Load environment variables from.env file, if present
load_dotenv()

def connect_to_db():
    """Establishes a connection to the PostgreSQL database."""
    conn = psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),  # Or the IP address of your PostgreSQL container
        port=os.getenv('DB_PORT')
    )
    return conn

def get_raw_data(conn):
    """Fetches raw text data from the database."""
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT id, title, header, content, categories FROM raw_amharic_data;")
    rows = cur.fetchall()
    cur.close()
    return rows

def clean_amharic_text(text):
    """Cleans Amharic text by removing HTML tags and normalizing whitespace."""
    soup = BeautifulSoup(text, 'html.parser')
    text = soup.get_text(strip=True)
    # Remove special characters
    text = re.sub(r'[^\w\s]', '', text)
    # Remove amharic special characters
    text = re.sub('[\!\@\#\$\%\^\«\»\&\*\(\)\…\[\]\{\}\;\“\”\›\’\‘\"\'\:\,\.\‹\/\<\>\?\\\\|\`\´\~\-\=\+\፡\።\፤\;\፦\፥\፧\፨\፠\፣]', '',text) 
    text = re.sub(r'\s+', ' ', text).strip()
    # Remove numbers
    text = re.sub('[A-Za-z0-9]','',text)
    return text

def store_cleaned_data(cleaned_rows):
    """Stores cleaned data back into the database in a new table."""
    conn = connect_to_db()
    cur = conn.cursor()
    for row in cleaned_rows:
        cur.execute("""
            INSERT INTO cleaned_data (title, header, content, categories)
            VALUES (%s, %s, %s, %s);
        """, (row['title'], row['header'], row['content'], row['categories']))
    conn.commit()
    cur.close()
    conn.close()

def main():
    # Connect to the database and fetch raw data
    conn = connect_to_db()
    rows = get_raw_data(conn)
    print("Retrieved Data:")
    for row in rows:
        print(row)
    conn.close()

    # Process and clean the data
    cleaned_rows = []
    for row in rows:
        cleaned_row = {}
        for key, value in row.items():
            if isinstance(value, str):
                cleaned_value = clean_amharic_text(value)
                cleaned_row[key] = cleaned_value
            else:
            # If not a string, just pass the value through
                cleaned_row[key] = value
        cleaned_rows.append(cleaned_row)

    print("\nCleaned Data:")
    for row in cleaned_rows:
        print(row)

    # Store the cleaned data back into the database
    store_cleaned_data(cleaned_rows)

if __name__ == "__main__":
    main()