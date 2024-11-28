# Real-Time-Data-Pipeline
Design and implement a scalable real-time data pipeline that monitors a directory for incoming data, processes it based on specific criteria, and stores the transformed data in a relational database for further analysis.

## Task Description:
- Data Ingestion & Monitoring:
    - The pipeline should continuously monitor a folder named “data” for incoming CSV files.
    - The files will contain raw sensor data from various sources. These sources will mimic sensor data that is collected periodically, such as temperature, humidity, or pressure readings from different locations.
    - The folder should be monitored regularly (every 5–10 seconds) to detect new files and trigger the processing immediately when new files are detected.
- Data Source:
    - The dataset contains continuous time-series data from multiple sensors, which could include timestamps, sensor ID, location, and multiple measurement values (e.g., temperature, humidity, or pressure).
    - Dataset Source (https://www.kaggle.com/datasets/hmavrodiev/sofia-air-quality-dataset)
- Data Validation and Transformation:
    - Implement validation checks to ensure the incoming data conforms to certain quality standards:
        - No null values in key fields (e.g., sensor ID, timestamp, readings).
        - Correct data types (e.g., numeric values for sensor readings).
        - Acceptable ranges for sensor readings (e.g., temperature between -50°C and 50°C).
    - Any data that fails validation should be moved to a quarantine/ folder, and a detailed error log should be maintained (including the reason for failure).
    - Valid data should be transformed into a standard format, e.g., normalizing the sensor values, parsing dates, or standardizing units of measurement.
- Data Analysis:
    - After validation, calculate aggregated metrics such as the minimum, maximum, average, and standard deviation for each sensor type and data source within the file.
    - Tag the processed data with metadata such as the data source, timestamp, and file name.
    - Store the aggregated data in a structured format for further analysis.
- Database Storage:
    - Design a schema in a relational database (PostgreSQL preferred) to store:
        - The raw sensor data.
        - The aggregated metrics calculated during the data analysis phase.
    - Ensure the schema is optimized for querying and future scalability. Consider indexing, data normalization, and efficient storage for large datasets.
    - Insert the raw data and computed aggregates into their respective tables, with appropriate timestamps.
- Scalability Considerations:
    - While your solution will process a single folder, imagine scaling this pipeline to handle millions of files per day.
    - Provide a brief outline of how you would design the pipeline to scale horizontally. Consider using distributed processing frameworks such as Apache Kafka or Apache Spark, or cloud-based data services such as AWS Lambda or Google Cloud Pub/Sub.
    - Include any optimizations you would implement for high-volume data ingestion and processing.
- Automation & Fault Tolerance:
    - The pipeline should run continuously without manual intervention.
    - If any part of the pipeline fails (e.g., a file is corrupted or the database is unavailable), log the error and attempt recovery without crashing the entire pipeline.
    - Implement a retry mechanism for failed operations, ensuring that no data is lost or ignored during outages or failures.



