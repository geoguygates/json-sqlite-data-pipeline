import pandas as pd
import sqlite3
import glob
import json


# Functions for handling sqlite data


def get_sqlite_table_names(pathname: str, filename: str):
    """Retrieve sqlite DB table names and store in list"""
    connection = sqlite3.connect(f'{pathname}\{filename}')
    cursor = connection.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type ='table';")
    tables = list(sum(cursor.fetchall(), ()))
    return tables


def create_dfs_from_sqlite_tables(pathname: str, filename: str, tables: list):
    """Unpack sqlite DB tables into a list of dataframes"""
    list_of_dfs = []
    for table in tables:
        connection = sqlite3.connect(f'{pathname}\{filename}')
        query = connection.execute(f'SELECT * From {table}')
        columns = [column[0] for column in query.description]
        df = pd.DataFrame.from_records(data=query.fetchall(), columns=columns)
        list_of_dfs.append(df)
    return list_of_dfs


# Functions for handling .json data


def add_json_files_into_df(pathname: str):
    """Retrieve json files from directory then add them into a single dataframe"""
    list_of_dfs = []
    for file in glob.glob(f'{pathname}\*.json'):
        json_file = json.load(open(file))
        df = pd.DataFrame(data=json_file['data'], columns=json_file['columns'])
        list_of_dfs.append(df)
    smart_meter_readings_df = pd.concat(list_of_dfs)
    return smart_meter_readings_df
