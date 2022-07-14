import pandas as pd
import sqlite3
import glob
import json
import logging



### Context Manager for sqlite DBs

class sqlite:
    """Context Manager for sqlite databases"""
    def __init__(self, file_name: str):
        self.file_name = file_name
        self.connection = sqlite3.connect(self.file_name)
        
    def __enter__(self):
        logging.info("Calling __enter__()")
        return self.connection.cursor()

    def __exit__(self, exc_class, exc, traceback):
        logging.info("Calling __exit__()")
        self.connection.commit()
        self.connection.close()

### Functions for handling sqlite data

# 1) New
def get_sqlite_table_names(pathname: str, filename: str):
    """Retrieve sqlite DB table names and store in list"""
    with sqlite(f'{pathname}\{filename}') as cursor:
        cursor.execute("SELECT name FROM sqlite_master WHERE type ='table';")
        tables = list(sum(cursor.fetchall(), ()))
    return tables

# # 1)  Original 
# def get_sqlite_table_names(pathname: str, filename: str):
#     """Retrieve sqlite DB table names and store in list"""
#     connection = sqlite3.connect(f'{pathname}\{filename}')
#     cursor = connection.cursor()
#     cursor.execute("SELECT name FROM sqlite_master WHERE type ='table';")
#     tables = list(sum(cursor.fetchall(), ()))
#     return tables


# 2) New
def create_dfs_from_sqlite_tables(pathname: str, filename: str, dbtables: list):
    """Unpack sqlite DB tables into a list of dataframes"""
    list_of_dfs = []
    for table in dbtables:
        with sqlite(file_name = f'{pathname}\{filename}') as cursor:
            query = cursor.execute(f'SELECT * From {table}')
            columns = [column[0] for column in query.description]
            df = pd.DataFrame.from_records(data=query.fetchall(), columns=columns)
            list_of_dfs.append(df)
    return list_of_dfs

# # 2)  Original 
# def create_dfs_from_sqlite_tables(pathname: str, filename: str, tables: list):
#     """Unpack sqlite DB tables into a list of dataframes"""
#     list_of_dfs = []
#     for table in tables:
#         connection = sqlite3.connect(f'{pathname}\{filename}')
#         query = connection.execute(f'SELECT * From {table}')
#         columns = [column[0] for column in query.description]
#         df = pd.DataFrame.from_records(data=query.fetchall(), columns=columns)
#         list_of_dfs.append(df)
#     return list_of_dfs


### Functions for handling .json data

# 3) New
def move_json_files_into_db(sqlite_pathname: str, sqlite_filename: str, json_inpath: str, dbname: str):
    """One at a time read json files from a specified directory then append them to a sqlite db table"""
    with sqlite(file_name = f'{sqlite_pathname}\{sqlite_filename}') as cursor:
        cursor.execute(f'CREATE TABLE IF NOT EXISTS {dbname}(interval_start text, consumption_delta real, meterpoint_id text)')
        for file in glob.glob(f'{json_inpath}\*.json'):
            with open(file, 'r') as file:
                json_file = json.load(file)
                df = pd.DataFrame(data=json_file['data'], columns=json_file['columns'])
                df.to_sql('smart_meter_readings', con=sqlite.connection, if_exists="append", index=False)


# # 3)  Original 
# def add_json_files_into_df(pathname: str):
#     """Retrieve json files from directory then add them into a single dataframe"""
#     list_of_dfs = []
#     for file in glob.glob(f'{pathname}\*.json'):
#         json_file = json.load(open(file))
#         df = pd.DataFrame(data=json_file['data'], columns=json_file['columns'])
#         list_of_dfs.append(df)
#     smart_meter_readings_df = pd.concat(list_of_dfs)
#     return smart_meter_readings_df