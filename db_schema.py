db_config = {
        "user": "postgres",
        "password": "postgres",
        "host": "localhost",
        "port": 5432
    }

raw_data = [
            {
                "sensor_id": 1,
                "location": "City Center",
                "latitude": 42.7,
                "longitude": 23.3,
                "timestamp": "2024-11-29 12:00:00",
                "pressure": 1012.5,
                "temperature": 22.3,
                "humidity": 60.0
            }
        ]

aggregated_data = [
            {
                "sensor_id": 1,
                "location": "City Center",
                "latitude": 42.7,
                "longitude": 23.3,
                "min_pressure": 1012.0,
                "max_pressure": 1013.0,
                "avg_pressure": 1012.5,
                "std_pressure": 0.5,
                "min_temperature": 22.0,
                "max_temperature": 23.0,
                "avg_temperature": 22.5,
                "std_temperature": 0.5,
                "min_humidity": 59.0,
                "max_humidity": 61.0,
                "avg_humidity": 60.0,
                "std_humidity": 1.0
            }
        ]

query_update_aggregated_data = """
        INSERT INTO aggregated_metrics (
            sensor_id, location, latitude, longitude,
            min_pressure, max_pressure, avg_pressure, std_pressure,
            min_temperature, max_temperature, avg_temperature, std_temperature,
            min_humidity, max_humidity, avg_humidity, std_humidity
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (sensor_id, location, latitude, longitude)
        DO UPDATE SET
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
            last_updated = CURRENT_TIMESTAMP;
    """
    
query_update_raw_data = """
        INSERT INTO raw_sensor_data (sensor_id, location, latitude, longitude, timestamp, pressure, temperature, humidity)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (sensor_id, timestamp) DO NOTHING;
    """

query_create_raw_data = """
        CREATE TABLE IF NOT EXISTS raw_sensor_data (
            id SERIAL PRIMARY KEY,
            sensor_id INT NOT NULL,
            location TEXT NOT NULL,
            latitude FLOAT NOT NULL,
            longitude FLOAT NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            pressure FLOAT NOT NULL,
            temperature FLOAT NOT NULL,
            humidity FLOAT NOT NULL,
            UNIQUE(sensor_id, timestamp)
        );
    """

query_create_aggregated_data = """
        CREATE TABLE IF NOT EXISTS aggregated_metrics (
            id SERIAL PRIMARY KEY,
            sensor_id INT NOT NULL,
            location TEXT NOT NULL,
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
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(sensor_id, location, latitude, longitude)
        );
    """

    