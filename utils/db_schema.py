db_config = {
        "user": "postgres",
        "password": "postgres",
        "host": "localhost",
        "port": 5432
    }

query_update_sensor_raw_data = """
        INSERT INTO sensor_raw_data (sensor_id, location_id, latitude, longitude, timestamp, pressure, temperature, humidity)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (sensor_id, timestamp) DO NOTHING;
    """

query_create_sensor_raw_data = """
        CREATE TABLE IF NOT EXISTS sensor_raw_data (
            id SERIAL PRIMARY KEY,
            sensor_id INT NOT NULL,
            location_id INT NOT NULL,
            latitude FLOAT NOT NULL,
            longitude FLOAT NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            pressure FLOAT NOT NULL,
            temperature FLOAT NOT NULL,
            humidity FLOAT NOT NULL,
            UNIQUE(sensor_id, timestamp)
        );
    """

query_create_sensor_aggregated_metrics = """
        CREATE TABLE IF NOT EXISTS sensor_aggregated_metrics (
        sensor_id INT PRIMARY KEY,
        location_id INT NOT NULL,
        latitude FLOAT NOT NULL,
        longitude FLOAT NOT NULL,
        min_pressure FLOAT,
        max_pressure FLOAT,
        avg_pressure FLOAT,
        std_pressure FLOAT,
        min_temperature FLOAT,
        max_temperature FLOAT,
        avg_temperature FLOAT,
        std_temperature FLOAT,
        min_humidity FLOAT,
        max_humidity FLOAT,
        avg_humidity FLOAT,
        std_humidity FLOAT,
        metadata JSONB NOT NULL,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """

query_update_sensor_aggregated_metrics = """
        INSERT INTO sensor_aggregated_metrics (
            sensor_id, location_id, latitude, longitude,
            min_pressure, max_pressure, avg_pressure, std_pressure,
            min_temperature, max_temperature, avg_temperature, std_temperature,
            min_humidity, max_humidity, avg_humidity, std_humidity, metadata
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (sensor_id)
        DO UPDATE SET
            location_id = EXCLUDED.location_id,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            min_pressure = EXCLUDED.min_pressure,
            max_pressure = EXCLUDED.max_pressure,
            avg_pressure = EXCLUDED.avg_pressure,
            std_pressure = EXCLUDED.std_pressure,
            min_temperature = EXCLUDED.min_temperature,
            max_temperature = EXCLUDED.max_temperature,
            avg_temperature = EXCLUDED.avg_temperature,
            std_temperature = EXCLUDED.std_temperature,
            min_humidity = EXCLUDED.min_humidity,
            max_humidity = EXCLUDED.max_humidity,
            avg_humidity = EXCLUDED.avg_humidity,
            std_humidity = EXCLUDED.std_humidity,
            metadata = EXCLUDED.metadata || sensor_aggregated_metrics.metadata, -- Merging metadata
            last_updated = CURRENT_TIMESTAMP;
    """

query_fetch_sensor_aggregated_metrics = query = """
            SELECT sensor_id, location_id, latitude, longitude,
                min_pressure, max_pressure, avg_pressure, std_pressure,
                min_temperature, max_temperature, avg_temperature, std_temperature,
                min_humidity, max_humidity, avg_humidity, std_humidity
            FROM sensor_aggregated_metrics
            WHERE sensor_id = ANY(%s);
        """
