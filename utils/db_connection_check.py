import psycopg2
import logging
from log_config import log_info

# Database configuration
DB_CONFIG = {
    "user": "postgres",
    "password": "postgres", # use your password
    "host": "localhost",
    "port": 5432
}

try:
    conn = psycopg2.connect(**DB_CONFIG)
    log_info("info", "Successfully connected to PostgreSQL")
    conn.close()
except psycopg2.Error as e:
    log_info("error", f"Failed to connect to PostgreSQL: {e}")
