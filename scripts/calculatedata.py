import pandas as pd

# Load the data from CSV
src_output_data = pd.read_csv("/home/egrabovaj/FlinkProject/FlinkTpcDs/scripts/graph_files/out_filter.csv")
#src_output_data = pd.read_csv("/home/egrabovaj/out_filter.csv")

# Calculate the average values for specified columns
average_rate_tuple_s = src_output_data['rate_tuple_S'].mean()
average_rate_MB_s = src_output_data['rate_MB_S'].mean()
average_busy_time = src_output_data['accumulated-busy-time'].mean()

print(f"\n'average_rate_tuple_s': {average_rate_tuple_s}")
print(f"\n'average_rate_MB_s': {average_rate_MB_s}")
print(f"\n'average_busy_time': {average_busy_time}")
