import psycopg2
from psycopg2 import sql
import logging

from db_schema import query_create_raw_data, query_update_raw_data, query_create_aggregated_data, query_update_aggregated_data, \
                      db_config, raw_data, aggregated_data


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Database:
    def __init__(self, config):
        self.config = config
        self.connection = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(**self.config)
            logging.info("Connected to PostgreSQL database successfully.")
        except Exception as e:
            logging.error(f"Failed to connect to the database: {e}")
            raise

    def close(self):
        if self.connection:
            self.connection.close()
            logging.info("Database connection closed.")

    def create_tables(self):
        try:
            with self.connection.cursor() as cur:
                # Create raw_sensor_data table
                cur.execute(query_create_raw_data)

                # Create aggregated_metrics table
                cur.execute(query_create_aggregated_data)

                # Create indexes
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_raw_sensor_data_timestamp
                    ON raw_sensor_data (timestamp);
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_aggregated_metrics_sensor_id
                    ON aggregated_metrics (sensor_id);
                    """
                )

            self.connection.commit()
            logging.info("Tables created successfully (if not already present).")
        except Exception as e:
            logging.error(f"Failed to create tables: {e}")
            self.connection.rollback()
            raise

    def update_raw_data(self, raw_data):
        try:
            with self.connection.cursor() as cur:
                for record in raw_data:
                    cur.execute(query_update_raw_data, (
                        record['sensor_id'], record['location'], record['latitude'],
                        record['longitude'], record['timestamp'], record['pressure'],
                        record['temperature'], record['humidity']
                    ))
            self.connection.commit()
            logging.info("Raw sensor data inserted/updated successfully.")
        except Exception as e:
            logging.error(f"Failed to upsert raw data: {e}")
            self.connection.rollback()
            raise

    def update_aggregated_data(self, aggregated_data):
        try:
            with self.connection.cursor() as cur:
                for record in aggregated_data:
                    cur.execute(query_update_aggregated_data, (
                        record['sensor_id'], record['location'], record['latitude'],
                        record['longitude'], record['min_pressure'], record['max_pressure'],
                        record['avg_pressure'], record['std_pressure'], record['min_temperature'],
                        record['max_temperature'], record['avg_temperature'], record['std_temperature'],
                        record['min_humidity'], record['max_humidity'], record['avg_humidity'],
                        record['std_humidity']
                    ))
            self.connection.commit()
            logging.info("Aggregated metrics inserted/updated successfully.")
        except Exception as e:
            logging.error(f"Failed to upsert aggregated metrics: {e}")
            self.connection.rollback()
            raise


if __name__ == "__main__":
    db = Database(db_config)
    try:
        db.connect()
        db.create_tables()
        db.update_raw_data(raw_data)
        db.update_aggregated_data(aggregated_data)
    finally:
        db.close()
