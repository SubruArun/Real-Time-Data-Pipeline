import pandas

dataset_df = pandas.read_csv("air_quality_dataset.csv")

num_parts = 10
part_size = len(dataset_df) // num_parts

for i in range(num_parts):
    start_index = i * part_size
    if i == num_parts - 1:
        part_df = dataset_df[start_index:]
    else:
        part_df = dataset_df[start_index:start_index + part_size]
    
    part_df.to_csv(f"./sample_data/air_quality_data_part_{i + 1}.csv", index=False)
