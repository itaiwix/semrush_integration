#################
#### Imports ####
#################

import json
import re
import time
import urllib
from datetime import datetime, date, timedelta
from urllib.parse import urlparse

import pandas as pd
import requests
from ba_infra.presto_connection import WixPrestoConnection
from google.auth import credentials
from google.oauth2 import service_account

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
            "?type": call_type,
            'key': api_key,
            'database': geo,
            'export_columns': features,
            'display_date': date_mid_month,
            'display_limit': '10',
            'domain': domain,
        }
    elif dimension == 'phrase':
        params = {
            "?type": call_type,
            'key': api_key,
            'phrase': phrase,
            'database': geo,
            'export_columns': features,
            'display_date': date_mid_month,
            'display_limit': '10',
        }
    elif dimension == 'url':
        params = {
            "?type": call_type,
            'key': api_key,
            'target': url,
            'target_type': 'url',
            'export_columns': features,
            'display_limit': '10',
        }

    data = urllib.parse.urlencode(params, doseq=True)
    request = urllib.parse.urljoin(service_url, data)
    request = request.replace(r'%3F', r'?')

    response = requests.get(request)

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

# function makes the actual call to the api
# for the phase based reports the request is done per keyword and per database (each geo has another one)
# for the url based reports the request is done per url (no separation to databases)
# the result of the function is a df
# as protection the function checks first if we didn't already updated today the tables
def get_data(dimension, call_type, features, service_url, api_key, date_mid_month, kw_list, db_list, url_list,
             current_time, last_update_date):
    if current_time.date() != last_update_date:
        if dimension == 'phrase':
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

        elif dimension == 'url':
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
            print('No such dimension, so far we only have phrase/url')
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
        print('Error - functions_general - execute_sql')

# get data from presto table, bring it as dataframe
def get_quix_data(sql_script, pc):
    print('Get data from quix')
    try:
        sql_script = sql_script
        input = pc.execute_sql(sql_script, add_cols_metadata=True)
        data = input.get("rows")
        columns = input.get("col_names")
        df = pd.DataFrame(data, columns=columns)
        print('Managed to bring data from quix')
        return df
    except:
        print('Error - functions_general - get_quix_data')

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
    list_indicators = []
    for feature in range(0, len(features)):
        if re.search(symbol, features[feature]) is not None:
            indicator = 1
            list_indicators.append(indicator)
        else:
            indicator = 0
            list_indicators.append(indicator)
    _sum = sum(list_indicators)
    if _sum == 0:
        return 0
    else:
        return 1

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


###############################
####  Database connections ####
###############################
pc = WixPrestoConnection()
bq_credentials = service_account.Credentials.from_service_account_file('insert path to the key', scopes=["https://www.googleapis.com/auth/cloud-platform"])

#####################
####  Parameters ####
#####################

api_key = read_json(path_file='insert path to the key']
global_parameters = read_json(path_file='G:/Shared drives/acquisition/Itaib - Projects/seo/semrush/configuration.json')

# Set dates
date_mid_month = mid_month()   # Data updated two weeks after the end of the month, for the previous month
current_time = datetime.now()

# Set Tables
google_sheet_kw_table = 'gdrive.bi.marketing_itaib_semrush_kw_gs'
google_sheet_url_table = 'gdrive.bi.marketing_itaib_semrush_url_gs'
google_sheet_db_table = 'gdrive.bi.marketing_itaib_semrush_database_gs'

presto_kw_list = 'sandbox.itaib.seo_kw_semrush'
presto_url_list = 'sandbox.itaib.seo_url_semrush'
presto_db_list = 'sandbox.itaib.seo_databases_semrush'

bq_delta_table_semrush_kw_traffic = 'temp.semrush_kw_traffic_delta'
bq_delta_table_semrush_url_backlinks = 'temp.semrush_url_backlinks_delta'
bq_delta_table_semrush_kw_serp_features_map = 'temp.semrush_kw_serp_features_map_delta'

# Update input tables
script_kw_list = update_input_tables(create_presto_table=presto_kw_list, exesting_google_sheet_table=google_sheet_kw_table)
execute_sql(sql_script=script_kw_list, pc=pc)
script_url_list = update_input_tables(create_presto_table=presto_url_list, exesting_google_sheet_table=google_sheet_url_table)
execute_sql(sql_script=script_kw_list, pc=pc)
script_db_list = update_input_tables(create_presto_table=presto_db_list, exesting_google_sheet_table=google_sheet_db_table)
execute_sql(sql_script=script_kw_list, pc=pc)

# Get input tables
kw_list = get_quix_data(f'select * from {presto_kw_list}', pc=pc).keyword.tolist()
url_list = get_quix_data(f'select * from {presto_url_list}', pc=pc).url.tolist()
db_list = get_quix_data(f'select * from {presto_db_list}', pc=pc).database.tolist()

# Last update dates on final tables
last_update_date_semrush_kw_traffic = get_last_update_date(table=f'marketing_internal_bq.{bq_delta_table_semrush_kw_traffic}', pc=pc)
last_update_date_semrush_url_backlinks = get_last_update_date(table=f'marketing_internal_bq.{bq_delta_table_semrush_url_backlinks}', pc=pc)
last_update_date_semrush_kw_serp_features_map = get_last_update_date(table=f'marketing_internal_bq.{bq_delta_table_semrush_kw_serp_features_map}', pc=pc)

# Parameters for report 'Keyword Overview'
dimension_keyword_overview = global_parameters['report_keyword_overview']['dimension']
service_url_keyword_overview = global_parameters['report_keyword_overview']['service_url']
call_type_keyword_overview = global_parameters['report_keyword_overview']['call_type']
features_keyword_overview = global_parameters['report_keyword_overview']['features']
table_schema_keyword_overview = global_parameters['report_keyword_overview']['final_table_schema']

# Parameters for report 'Organic Results'
dimension_organic_results = global_parameters['report_organic_results']['dimension']
service_url_organic_results = global_parameters['report_organic_results']['service_url']
call_type_organic_results = global_parameters['report_organic_results']['call_type']
features_organic_results = global_parameters['report_organic_results']['features']
serp_features_map_organic_results = global_parameters['report_organic_results']['feature_mapping']
table_schema_organic_results = global_parameters['report_organic_results']['final_table_schema']

# Parameters for report 'Indexed Pages'
dimension_indexed_pages = global_parameters['report_indexed_pages']['dimension']
service_url_indexed_pages = global_parameters['report_indexed_pages']['service_url']
call_type_indexed_pages = global_parameters['report_indexed_pages']['call_type']
features_indexed_pages = global_parameters['report_indexed_pages']['features']
table_schema_indexed_pages = global_parameters['report_indexed_pages']['final_table_schema']

#########################
#### Request Reports ####
#########################

## Report - Keyword Overview ##
# Request Data
final_df_phrase_this = get_data(dimension=dimension_keyword_overview, call_type=call_type_keyword_overview,
                                features=features_keyword_overview, service_url=service_url_keyword_overview,
                                api_key=api_key, date_mid_month=date_mid_month, kw_list=kw_list, db_list=db_list,
                                url_list=url_list, current_time=current_time, last_update_date=last_update_date_semrush_kw_traffic)
# Organize Data
final_df_phrase_this = final_df_phrase_this.rename(columns={'Search Volume': 'search_volume'})  # bq can't accept 'Search Volume' as column name

## Report - Organic Results ##
# Request Data
final_df_phrase_organic = get_data(dimension=dimension_organic_results, call_type=call_type_organic_results,
                                   features=features_organic_results, service_url=service_url_organic_results,
                                   api_key=api_key, date_mid_month=date_mid_month, kw_list=kw_list, db_list=db_list,
                                   url_list=url_list, current_time=current_time, last_update_date=last_update_date_semrush_url_backlinks)
# Organize Data
for key, value in serp_features_map_organic_results.items():
    symbol = '^' + str(key) + '$'
    map_features(df=final_df_phrase_organic, new_column=value, symbol=symbol, old_column='SERP Features')
del final_df_phrase_organic['SERP Features']

## Report - Indexed Pages ##
# Request Data
final_df_backlinks_pages = get_data(dimension=dimension_indexed_pages, call_type=call_type_indexed_pages,
                                    features=features_indexed_pages, service_url=service_url_indexed_pages,
                                    api_key=api_key, date_mid_month=date_mid_month, kw_list=kw_list, db_list=db_list,
                                    url_list=url_list, current_time=current_time, last_update_date=last_update_date_semrush_kw_serp_features_map)
# Organize Data
del final_df_backlinks_pages['last_seen']

###########################
#### Load data to  BQ  ####
###########################

load_db(data_frame=final_df_phrase_this, credentials=bq_credentials, table=bq_delta_table_semrush_kw_traffic)
load_db(data_frame=final_df_phrase_organic, credentials=bq_credentials, table=bq_delta_table_semrush_url_backlinks)
load_db(data_frame=final_df_backlinks_pages, credentials=bq_credentials, table=bq_delta_table_semrush_kw_serp_features_map)
