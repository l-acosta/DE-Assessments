import time
import sys
import sqlite3
from datetime import datetime
import pandas as pd
import os
from pathlib import Path


INPUT_FILE = "city_temperature.csv"
current_dir = Path(__file__).parent.resolve()

def read_file(file_path):

    df= pd.read_csv(file_path, sep=',', header=0, encoding  = 'utf-8',
                    dtype={'Region':str, 'Country':str, 'State':str, 'City':str,
                           'Month':int, 'Day':int, 'Year':int, 'AvgTemperature':float}
                    )

    return df

def staging_data(df):

    df_clean = pd.DataFrame(df)

    # Validate dates
    ## Delete invalid days
    df_clean = df_clean[df_clean['Day'] > 0]
    ## Delete invalid years
    df_clean = df_clean[df_clean['Year'] > 1990]
    ## Add datetime to data set
    df_clean['Date'] = pd.to_datetime(df_clean[['Year','Month','Day']])

    # Validate temperatures
    df_clean = df_clean[df_clean['AvgTemperature'] > -99]


    return df_clean

def presenting_data(df):

    df_pres = pd.DataFrame(df)

    # Calculate Last Year's Monthly Average
    df_MonthAvg = df.groupby(['Region','Country','Year','Month']).agg({'AvgTemperature': 'mean'}).reset_index()

    # Maybe should use rolling mean here, not sure
    #df_pres= df_MonthAvg.join(df_MonthAvg, sort=False, how='left', on=['Region','Country','Year','Month'], right_on=['Region','Country','Year'-1,'Month'])

    return df_MonthAvg


def into_bucket(df, schema:str, file_name:str):

    # create reports folder if not exists
    if not os.path.exists(f"{current_dir}/{schema}/"):
        os.mkdir(f"{current_dir}/{schema}/")
    
    path=(f"{current_dir}/{schema}/")
    df.to_csv(path + file_name + '.csv', header=list(df.columns), index=True)
    print('File saved in local bucket : ' + path + file_name + '.csv')


if __name__ == "__main__":
    try:

        # Extract data
        print('-- Reading input file ...')
        df_raw = read_file(INPUT_FILE)

        # Save as CSV file to simulate an S3 bucket
        into_bucket(df=df_raw, schema='raw_data', file_name='raw_temperature')

        # Transform and clean data
        print('-- Cleansing data ...')
        df_stg = staging_data(df_raw)

        # Save as CSV file to simulate an S3 bucket
        into_bucket(df=df_stg, schema='stagin', file_name='stg_temperature')

        # Select presentation
        print('-- Generating output data ...')
        df_present = presenting_data(df_stg)

        # Save as CSV file to simulate an S3 bucket
        into_bucket(df=df_present, schema='presentation', file_name='temperature_odp')

    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(e)
    finally:
        sys.exit()