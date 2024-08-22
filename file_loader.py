'''
Author: Syed Hasan
Project: Data ingestion from local storage to s3 into chunks based on date
'''

import boto3
import os
import pandas as pd
import pyarrow as pa # parquet engine library
import logging
import datetime

# debugger setup
start_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
print(start_time)

logging.basicConfig(
    filename="file_loader_"+start_time+".log",
    encoding="utf-8",
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.DEBUG, )
log = logging.getLogger(__name__)

s3 = boto3.client('s3')
bucket_name = "taxi-datasetetl"
month = 'march'  # for file subfolder location

# 1 -Extract
file_name = 'yellow_tripdata_2022-03'
extension = '.parquet' # to make it re-usable for different file types

df = pd.read_parquet(file_name + extension, 'pyarrow')

log.info('data loaded' + file_name)

log.info('columns: ' + df.columns)

# 2- Transform
log.info(df.info(verbose=False))


dates = df.tpep_dropoff_datetime.dt.date.unique() # get unique dates
log.info('Dates File Count: ' + str(len(dates)))

for date_filter in dates:
    sub_df = df[df.tpep_dropoff_datetime.dt.date == date_filter] # filteration - Transformation

    log.info(str(date_filter) + ': Transformation Complete')
    object_name = file_name[:-7] + str(date_filter) + extension
    # print(object_name)
    log.info('object name: ' + file_name + str(date_filter) + extension)
    s3_url = 's3://' + bucket_name + '/' + month + '/' + object_name

    # 3- Load
    try:
        sub_df.to_parquet(s3_url, 'pyarrow', index=False)
        log.debug('Loaded File to S3: ' + s3_url)
    except Exception as e:
        log.error(e)
        log.debug('error uploading parquet file: ' + s3_url)

    # sub_df.to_parquet(file_name+str(date_filter)+extension,'pyarrow') # to store locally

    log.info(s3_url + ' Complete')
    # break #uncomment to load single file
log.info('Done!')
