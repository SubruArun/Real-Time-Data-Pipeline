import psycopg2
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Database configuration
DB_CONFIG = {
    "user": "postgres",
    "password": "postgres", # use your password
    "host": "localhost",
    "port": 5432
}

try:
    conn = psycopg2.connect(**DB_CONFIG)
    logging.info("Successfully connected to PostgreSQL")
    conn.close()
except psycopg2.Error as e:
    logging.error(f"Failed to connect to PostgreSQL: {e}")
