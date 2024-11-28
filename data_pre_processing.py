import pandas
import logging
import time


# configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def data_standardisation(dataset_df):
    float_columns = dataset_df.select_dtypes(include=['float64']).columns
    # round to 3 decimal places
    dataset_df[float_columns] = dataset_df[float_columns].round(3)

    # logs
    logging.info("Data Standardisation Complete")
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
    logging.info(f"Pre-Processing Started for file {dataset_path}")
    start_time = time.time()
    dataset_df = pandas.read_csv(dataset_path).drop_duplicates()
    # dataset_df = pandas.read_csv("air_quality_data_part_1.csv").drop_duplicates()
    dataset_df = dataset_df.rename(columns={"lat": "latitude", "lon": "longitude"})
    dataset_df = dataset_df.loc[:, ~dataset_df.columns.str.contains('^Unnamed')]
    validation_check_dataset_df = dataset_df.apply(lambda row: data_validation_check(row), axis=1)
    dataset_df = pandas.concat([dataset_df, validation_check_dataset_df], axis=1)

    valid_data_df = dataset_df[dataset_df['validity']==True]
    invalid_data_df = dataset_df[dataset_df['validity']==False]

    # logs
    logging.info(
        "Data Validation Complete:\n"
        f"\t\t\t\t\t- Total rows in dataset: {len(dataset_df)}\n"
        f"\t\t\t\t\t- Valid rows: {len(valid_data_df)}\n"
        f"\t\t\t\t\t- Invalid rows: {len(invalid_data_df)}"
    )

    # remove unnecessary columns
    valid_data_df = valid_data_df.drop(columns=['validity', 'reason'])
    valid_data_df = data_standardisation(valid_data_df)

    invalid_data_df.to_csv("./quarantine/invalid_air_quality_data_part_1.csv", index=False)
    valid_data_df.to_csv("./quarantine/valid_air_quality_data_part_1.csv", index=False)

    end_time = time.time()
    elapsed_time = end_time - start_time
    logging.info(f"Pre-Processing Completed for file {dataset_path}")
    logging.info(f"Total time taken for pre-processing : {elapsed_time:.2f} seconds")
