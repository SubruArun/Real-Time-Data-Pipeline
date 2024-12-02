import psycopg2
import psycopg2.extras
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from psycopg2 import OperationalError

from utils.db_schema import query_create_sensor_raw_data, query_update_sensor_raw_data, query_create_sensor_aggregated_metrics, query_update_sensor_aggregated_metrics
from utils.log_config import log_info


class Database:
    def __init__(self, config):
        self.config = config
        self.connection = None

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_fixed(5),
        retry=retry_if_exception_type(Exception),
        reraise=True
    )
    def connect(self):
        try:
            self.connection = psycopg2.connect(**self.config)
            log_info("info", "Connected to PostgreSQL database successfully.")
        except Exception as e:
            log_info("error", f"Failed to connect to the database: {e}")
            raise

    def close(self):
        if self.connection:
            self.connection.close()
            log_info("info", "Database connection closed.")

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_fixed(5),
        retry=retry_if_exception_type(Exception),
        reraise=True
    )
    def create_tables(self):
        try:
            with self.connection.cursor() as cur:
                # Create raw_sensor_data table
                cur.execute(query_create_sensor_raw_data)

                # Create aggregated_metrics table
                cur.execute(query_create_sensor_aggregated_metrics)

                # Create indexes
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_sensor_raw_data_timestamp
                    ON sensor_raw_data (timestamp);
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_sensor_aggregated_metrics_sensor_id
                    ON sensor_aggregated_metrics (sensor_id);
                    """
                )

            self.connection.commit()
            log_info("info", "Tables created successfully (if not already present).")
        except Exception as e:
            log_info("error", f"Failed to create tables: {e}")
            self.connection.rollback()
            raise

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_fixed(5),
        retry=retry_if_exception_type(Exception),
        reraise=True
    )
    def update_sensor_raw_data(self, raw_data):
        try:
            with self.connection.cursor() as cur:
                raw_data = raw_data.to_dict(orient='records')
                for record in raw_data:
                    cur.execute(query_update_sensor_raw_data, (
                        record['sensor_id'], record['location_id'], record['latitude'],
                        record['longitude'], record['timestamp'], record['pressure'],
                        record['temperature'], record['humidity']
                    ))
            self.connection.commit()
            log_info("info", "Raw sensor data inserted/updated successfully.")
        except Exception as e:
            log_info("error", f"Failed to upsert raw data: {e}")
            self.connection.rollback()
            raise

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_fixed(5),
        retry=retry_if_exception_type(Exception),
        reraise=True
    )
    def update_sensor_aggregated_metrics(self, aggregated_data):
        try:
            with self.connection.cursor() as cur:
                aggregated_data = aggregated_data.to_dict(orient='records')
                for record in aggregated_data:
                    metadata_json = psycopg2.extras.Json(record['metadata'])
                    cur.execute(query_update_sensor_aggregated_metrics, (
                        record['sensor_id'], record['location_id'], record['latitude'],
                        record['longitude'], record['min_pressure'], record['max_pressure'],
                        record['avg_pressure'], record['std_pressure'], record['min_temperature'],
                        record['max_temperature'], record['avg_temperature'], record['std_temperature'],
                        record['min_humidity'], record['max_humidity'], record['avg_humidity'],
                        record['std_humidity'], metadata_json
                    ))
            self.connection.commit()
            log_info("info", "Aggregated metrics inserted/updated successfully.")
        except Exception as e:
            log_info("error", f"Failed to upsert aggregated metrics: {e}")
            self.connection.rollback()
            raise
