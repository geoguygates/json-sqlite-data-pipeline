import os
from config import ROOT_DIR, extract_bucket_pathname
import extract_functions as ef

# Extract sqlite data from source directory

sqlite_pathname = os.path.join(ROOT_DIR, '0_source_data')
sqlite_filename = 'case_study.db'

sqlite_table_names = ef.get_sqlite_table_names(
    sqlite_pathname, sqlite_filename)

list_of_sqlite_dfs = ef.create_dfs_from_sqlite_tables(
    sqlite_pathname, sqlite_filename, sqlite_table_names)

sqlite_dfs = dict(zip(sqlite_table_names, list_of_sqlite_dfs))

# Extract json data from source directory

json_pathname = os.path.join(ROOT_DIR, '0_source_data', 'readings')

smart_meter_readings_df = ef.add_json_files_into_df(json_pathname)

# Write each of the four dataframes to individual parquet files

for key, value in sqlite_dfs.items():
    value.to_parquet(f'{extract_bucket_pathname}\{key}_df.parquet')

smart_meter_readings_df.to_parquet(
    f'{extract_bucket_pathname}\smart_meter_readings_df.parquet')
