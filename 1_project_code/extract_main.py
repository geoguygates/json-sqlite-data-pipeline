import os
import psutil
from config import ROOT_DIR, extract_bucket_pathname
import extract_functions as ef

# Calling psutil.cpu_precent() for 3 seconds
print(f'The CPU usage is: {psutil.cpu_percent(3)}')

# Getting % usage of virtual_memory ( 3rd field)
print(f'RAM memory % used: {psutil.virtual_memory()[2]}')

# Extract sqlite data from source directory

sqlite_pathname = os.path.join(ROOT_DIR, '0_source_data')

sqlite_filename = 'case_study.db'

sqlite_table_names = ef.get_sqlite_table_names(sqlite_pathname, sqlite_filename)

list_of_sqlite_dfs = ef.create_dfs_from_sqlite_tables(sqlite_pathname, sqlite_filename, sqlite_table_names)

sqlite_dfs = dict(zip(sqlite_table_names, list_of_sqlite_dfs))

# Extract individual json files from source directory and batch write to a sqlite table

json_inpath = os.path.join(ROOT_DIR, '0_source_data', 'readings')
json_outpath = os.path.join(ROOT_DIR, '2_project_code_output', 'extract_bucket')

ef.move_json_files_into_db(sqlite_pathname, sqlite_filename, json_inpath, dbname='testdb')


# Write each of the four dataframes to individual parquet files

#sqlite
#for key, value in sqlite_dfs.items():
    #value.to_parquet(f'{extract_bucket_pathname}\{key}_df.parquet')

#json
# smart_meter_readings_df.to_parquet(
#     f'{extract_bucket_pathname}\smart_meter_readings_df.parquet')
