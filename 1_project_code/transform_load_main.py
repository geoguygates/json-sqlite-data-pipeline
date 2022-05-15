import pandas as pd
import sqlite3
import os
from config import ROOT_DIR

# Load all 4 extracted data frames from file folder
extract_bucket_pathname = os.path.join(
    ROOT_DIR, '2_project_code_output', 'extract_bucket')

agreement_df = pd.read_parquet(
    f'{extract_bucket_pathname}\\agreement_df.parquet')
meterpoint_df = pd.read_parquet(
    f'{extract_bucket_pathname}\\meterpoint_df.parquet')
product_df = pd.read_parquet(f'{extract_bucket_pathname}\\product_df.parquet')
smart_meter_readings_df = pd.read_parquet(
    f'{extract_bucket_pathname}\\smart_meter_readings_df.parquet')

# Generate Table of all active agreements

agreement_product_df = agreement_df.merge(product_df, how='left', left_on=[
                                          'product_id'], right_on=['product_id'])

active_agreements_df = agreement_product_df.loc[((agreement_product_df['agreement_valid_from'] < '2021-01-01') & (
    agreement_product_df['agreement_valid_to'] > '2021-01-01') | (agreement_product_df['agreement_valid_to'].isnull()))]

# Filter df to only columns asked for in the problem statement
active_agreements_df = active_agreements_df[[
    'agreement_id', 'meterpoint_id', 'display_name', 'is_variable']]

# Generate Table of the aggregate total consumption and count of meterpoints

agg_total_consumption_count_of_meterpoints = smart_meter_readings_df.groupby(
    ['interval_start']).agg({'consumption_delta': 'sum', 'meterpoint_id': 'count'}).reset_index()
agg_total_consumption_count_of_meterpoints.columns = [
    'time', 'consumption_sum', 'meterpoint_count']

# Load the two analytic tables into parquet files in a destination folder
transform_load_bucket_pathname = os.path.join(
    ROOT_DIR, '2_project_code_output', 'transform_load_bucket')

active_agreements_df.to_parquet(
    f'{transform_load_bucket_pathname}\\active_agreements_df.parquet')
agg_total_consumption_count_of_meterpoints.to_parquet(
    f'{transform_load_bucket_pathname}\\agg_total_consumption_count_of_meterpoints_df.parquet')
