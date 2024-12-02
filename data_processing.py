import pandas
import time
from datetime import datetime
from pathlib import Path

from db_manager import Database
from utils.db_schema import db_config
from utils.log_config import log_info


def data_post_processing(dataset_df, dataset_path):
    # logs
    log_info("info", f"Data Post-Processing Started for file {dataset_path}")

    # necessary columns check
    required_columns = {"sensor_id", "location_id", "latitude", "longitude", "timestamp", "pressure", "temperature", "humidity"}
    if not required_columns.issubset(dataset_df.columns):
        raise ValueError(f"Dataset must contain the following columns: {required_columns}")

    # group by sensor_id
    grouped_analysis_df = dataset_df.groupby(["sensor_id", "location_id", "latitude", "longitude"]).agg(
        min_pressure=("pressure", "min"),
        max_pressure=("pressure", "max"),
        avg_pressure=("pressure", "mean"),
        std_pressure=("pressure", "std"),
        min_temperature=("temperature", "min"),
        max_temperature=("temperature", "max"),
        avg_temperature=("temperature", "mean"),
        std_temperature=("temperature", "std"),
        min_humidity=("humidity", "min"),
        max_humidity=("humidity", "max"),
        avg_humidity=("humidity", "mean"),
        std_humidity=("humidity", "std"),
    ).reset_index()

    # round off to 3 decimal places - this is wrt to standardization
    rounded_columns = [
        'min_pressure', 'max_pressure', 'avg_pressure', 'std_pressure',
        'min_temperature', 'max_temperature', 'avg_temperature', 'std_temperature',
        'min_humidity', 'max_humidity', 'avg_humidity', 'std_humidity'
    ]
    grouped_analysis_df[rounded_columns] = grouped_analysis_df[rounded_columns].round(3)

    # fn to get metadata
    def collect_metadata(group):
        source_metadata = [
            {
                "timestamp": row["timestamp"],
                "pressure": row["pressure"],
                "temperature": row["temperature"],
                "humidity": row["humidity"],
            }
            for _, row in group.iterrows()
        ]
        # why like this - because in db each file can have a metadata kept seperately for further verifications if necessary
        return {dataset_path: source_metadata}

    metadata_df = (
        dataset_df.groupby("sensor_id")
        .apply(collect_metadata)
        .reset_index(name="metadata")
    )

    combined_df = pandas.merge(grouped_analysis_df, metadata_df, on="sensor_id")
    combined_df["last_updated"] = datetime.now()  # update current time/created time as timestamp

    # logs
    log_info("info", f"Data Post-Processing Completed for file {dataset_path}")

    return combined_df

def data_standardisation(dataset_df):
    # logs
    log_info("info", "Data Standardisation Started")

    float_columns = dataset_df.select_dtypes(include=['float64']).columns
    # round to 3 decimal places
    dataset_df[float_columns] = dataset_df[float_columns].round(3)

    # logs
    log_info("info", "Data Standardisation Complete")
    return dataset_df

def data_validation_check(row):
    # valid pressure check can be added if known, but not added here since the unit is unknown (0 to 165k range mentioned in dataset)

    reasons = []

    # empty data - need to strip string values before checking for null
    row = row.apply(lambda x: x.strip() if isinstance(x, str) else x)
    if row.isnull().any():
        reasons.append("NULL value found in one or more columns")

    # invalid data type
    data_type_check_columns = ['pressure', 'temperature', 'humidity', 'latitude', 'longitude', 'sensor_id']
    for column in data_type_check_columns:
        if not isinstance(row[column], (int, float)):
            reasons.append(f"Invalid data type in column '{column}'")

    # invalid data
    if 'temperature' in row and not (-50 <= row['temperature'] <= 60):   # (-145 to 61.2 range mentioned in dataset)
        reasons.append("Temperature out of valid range (-50 to 60)")
    if 'humidity' in row and not (0 <= row['humidity'] <= 100):
        reasons.append("Humidity out of valid range (0 to 100)")
    if 'latitude' in row and not (-90 <= row['latitude'] <= 90):         # (42.6 to 42.7 range mentioned in dataset)
        reasons.append("Latitude out of valid range (-90 to 90)")
    if 'longitude' in row and not (-180 <= row['longitude'] <= 180):     # (23.2 to 23.4 range mentioned in dataset)
        reasons.append("Longitude out of valid range (-180 to 180)")
    
    validity = False if reasons else True
    return pandas.Series({"reason": ", ".join(reasons) if reasons else None, "validity": validity})

def data_pre_processing(dataset_path):
    ''' DATA PRE-PROCESSING '''
    log_info("info", f"Pre-Processing Started for file {dataset_path}")
    pre_processing_start_time = time.time()
    dataset_df = pandas.read_csv(str(Path(dataset_path).resolve(strict=False))).drop_duplicates()
    dataset_df = dataset_df.rename(columns={"lat": "latitude", "lon": "longitude", "location":"location_id"})
    dataset_df = dataset_df.loc[:, ~dataset_df.columns.str.contains('^Unnamed')]
    validation_check_dataset_df = dataset_df.apply(lambda row: data_validation_check(row), axis=1)
    dataset_df = pandas.concat([dataset_df, validation_check_dataset_df], axis=1)

    valid_data_df = dataset_df[dataset_df['validity']==True]
    invalid_data_df = dataset_df[dataset_df['validity']==False]

    # logs
    log_info("info", 
        "Data Validation Complete:\n"
        f"\t\t\t\t\t- Total rows in dataset: {len(dataset_df)}\n"
        f"\t\t\t\t\t- Valid rows: {len(valid_data_df)}\n"
        f"\t\t\t\t\t- Invalid rows: {len(invalid_data_df)}"
    )

    # remove unnecessary columns
    valid_data_df = valid_data_df.drop(columns=['validity', 'reason'])
    valid_data_df = data_standardisation(valid_data_df)

    invalid_data_df.to_csv("./quarantine/invalid_air_quality_data_part_1.csv", index=False)
    # valid_data_df.to_csv("./quarantine/valid_air_quality_data_part_1.csv", index=False)

    pre_processing_end_time = time.time()
    pre_processing_elapsed_time = pre_processing_end_time - pre_processing_start_time
    log_info("info", f"Pre-Processing Completed for file {dataset_path}")

    # data analysis
    aggregated_data = data_post_processing(valid_data_df, dataset_path)

    db_write_start_time = time.time()
    # connect database and start write
    log_info("info", f"Starting Database Write for {dataset_path}")
    db = Database(db_config)
    try:
        db.connect()
        db.create_tables()
        db.update_sensor_raw_data(valid_data_df)
        db.update_sensor_aggregated_metrics(aggregated_data)
    finally:
        db.close()
    log_info("info", f"Database Write Completed for {dataset_path}")
    db_write_end_time = time.time()
    db_write_elapsed_time = db_write_end_time - db_write_start_time

    log_info("info", f"Total time taken for pre-processing : {pre_processing_elapsed_time:.2f} seconds")
    log_info("info", f"Total time taken for database operations : {db_write_elapsed_time:.2f} seconds")
