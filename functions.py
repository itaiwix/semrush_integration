#################
#### Imports ####
#################
import json
import re
import time
from datetime import datetime, date, timedelta

import pandas as pd
import requests
import urllib
from urllib.parse import urlparse

###################
#### Functions ####
###################

# function used to read json file
def read_json(path_file):
    path = path_file
    with open(path, "r") as jsonfile:
        parameters = json.load(jsonfile)
    return parameters

# function gets encoded utc time and translates it into the format of "%Y-%m-%d, %H:%M:%S"
def uncode_utc(utc):
    return time.strftime("%Y-%m-%d, %H:%M:%S", time.localtime(int(utc)))

# semrush updates the reports once a month so the function problematically calculates the date we should have data for
def mid_month():
    today = datetime.today().strftime('%Y%m%d')

    today_date = date.today()
    first = today_date.replace(day=1)
    lastMonth = first - timedelta(days=1)
    previous_month = lastMonth.strftime("%Y%m")

    if int(today[
           6:]) > 20:  # Data is updated two weeks after the end of the month for the previous month, we will have once a month run on the 16th
        return str(today[:4]) + str(today[4:6]) + str(15)
    else:
        return str(previous_month[:4]) + str(previous_month[4:6]) + str(15)

# creates the get request html for the database, separated by the dimension which represents the report type
def request(dimension, call_type, api_key, features, date_mid_month='null', url='null', domain="null", geo="us",
            service_url="https://api.semrush.com", phrase="null"):
    if dimension == 'domain':
        params = {
            "type": call_type,
            'key': api_key,
            'database': geo,
            'export_columns': features,
            'display_date': date_mid_month,
            'display_limit': '10',
            'domain': domain,
        }
    elif dimension == 'phrase':
        params = {
            "type": call_type,
            'key': api_key,
            'phrase': phrase,
            'database': geo,
            'export_columns': features,
            'display_date': date_mid_month,
            'display_limit': '10',
        }
    elif dimension == 'url':
        params = {
            "type": call_type,
            'key': api_key,
            'target': url,
            'target_type': 'url',
            'export_columns': features,
            'display_limit': '10',
        }
    response = requests.get(service_url, params=params)
    return response

# parse the response from the api, save the results in a list
def parse_response(call_data):
    results = []
    data = call_data.decode('unicode_escape')
    lines = data.split('\r\n')
    lines = list(filter(bool, lines))
    columns = lines[0].split(';')
    for line in lines[1:]:
        result = {}
        for response, datum in enumerate(line.split(';')):
            result[columns[response]] = datum.strip('"\n\r\t')
        results.append(result)

    return results

# function makes the actual call to the api, phrase based reports
# for the phrase based reports the request is done per keyword and per database (each geo has another one)
# the result of the function is a df
# as protection the function checks first if we didn't already updated today the tables
def get_phrase_data(dimension, call_type, features, service_url, api_key, date_mid_month, kw_list, db_list, current_time, last_update_date):
    if current_time.date() != last_update_date:
        list_container = []
        for keyword in range(0, len(kw_list)):
            kw = kw_list[keyword]
            for database in range(0, len(db_list)):
                db = db_list[database]
                response = request(dimension=dimension, call_type=call_type, api_key=api_key, geo=db,
                                   features=features, service_url=service_url, phrase=kw,
                                   date_mid_month=date_mid_month)
                parsed_response = parse_response(call_data=response.content)

                df = pd.json_normalize(parsed_response)
                df['keyword'] = kw
                df['database'] = db
                list_container.append(df)

        df_final = pd.concat(list_container, ignore_index=True)
        df_final['display_date'] = datetime.strptime(date_mid_month, '%Y%m%d').date()
        df_final['time_updated'] = current_time.strftime("%Y-%m-%d, %H:%M:%S")
        return df_final
    else:
        print('Table is already updated')

# function makes the actual call to the api, url based reports
# for the url based reports the request is done per url (no separation to databases)
# the result of the function is a df
# as protection the function checks first if we didn't already updated today the tables
def get_url_data(dimension, call_type, features, service_url, api_key, url_list, current_time, last_update_date):
    if current_time.date() != last_update_date:
        list_container = []
        for url in range(0, len(url_list)):
            url = url_list[url]
            response = request(dimension=dimension, call_type=call_type, api_key=api_key, url=url,
                               features=features, service_url=service_url)
            parsed_response = parse_response(call_data=response.content)

            df = pd.json_normalize(parsed_response)
            list_container.append(df)

        df_final = pd.concat(list_container, ignore_index=True)
        df_final['display_date'] = df_final.apply(lambda row: uncode_utc(row['last_seen']), axis=1)
        df_final['time_updated'] = current_time.strftime("%Y-%m-%d, %H:%M:%S")
        return df_final
    else:
        print('Table is already updated')

# execute presto sql scripts
def execute_sql(sql_script, pc):
    print('Execute sql from quix')
    try:
        sql_script = sql_script
        pc.execute_batch_sql(sql_script)
        print('Success, query done')
    except:
        raise Exception('Error - functions_general - execute_sql')

# get data from presto table, bring it as dataframe
def get_quix_data(sql_script, pc):
    print('Get data from quix')
    try:
        sql_script = sql_script
        df = pc.execute_sql_pandas(sql_script)
        print('Managed to bring data from quix')
        return df
    except:
        raise Exception('Error - functions_general - get_quix_data')

# presto script for inserting google sheet tables into presto tables
def update_input_tables(create_presto_table, exesting_google_sheet_table):
    script = f'''
    drop table if exists {create_presto_table};
    create table if not exists {create_presto_table} as

    select * from {exesting_google_sheet_table};
    '''
    return script

# one of the report's outputs is serp features, which are coded as a serial numbers. The function checks which serp features are there in order to create mapping
def inspect_features(row, symbol):
    features = row.split(',')
    for feature in features:
        if re.search(symbol, feature):
            return 1
    return 0

# function creates mapping based on the inspect_features function, doing 0/1 columns for availability of serp features
def map_features(df, new_column, symbol, old_column):
    df[new_column] = df.apply(lambda row: inspect_features(row[old_column], symbol), axis=1)

# bring the latest update data of a table
def get_last_update_date(table, pc):
    last_update_date = get_quix_data(f'select max(time_updated) as last_update from {table}', pc=pc).last_update[0][:10]
    return datetime.strptime(last_update_date, '%Y-%m-%d').date()

# load data into bq table
def load_db(data_frame, credentials, table):
    data_frame.to_gbq(credentials=credentials, destination_table=f'{table}', if_exists='append', chunksize=10000)

