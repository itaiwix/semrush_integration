
import re
import time
import urllib
from datetime import datetime, date, timedelta
from urllib.parse import urlparse

import pandas as pd
import requests
from ba_infra.presto_connection import WixPrestoConnection


def uncode_utc(utc):
    return time.strftime("%Y-%m-%d, %H:%M:%S", time.localtime(int(utc)))


def mid_month():
    today = datetime.today().strftime('%Y%m%d')

    today_date = date.today()
    first = today_date.replace(day=1)
    lastMonth = first - timedelta(days=1)
    previous_month = lastMonth.strftime("%Y%m")

    if int(today[6:]) > 20:  # Data is updated two weeks after the end of the month for the previous month, we will have once a month run on the 16th
        return str(today[:4])+str(today[4:6])+str(15)
    else:
        return str(previous_month[:4])+str(previous_month[4:6])+str(15)


def request(dimension, call_type, api_key, features, date_mid_month='null', url='null', domain="null", geo="us", service_url="https://api.semrush.com", phrase="null"):
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


def parse_response(call_data):
    results = []
    data = call_data.decode('unicode_escape')
    lines = data.split('\r\n')
    lines = list(filter(bool, lines))
    columns = lines[0].split(';')
    for line in lines[1:]:
        result = {}
        for i, datum in enumerate(line.split(';')):
            result[columns[i]] = datum.strip('"\n\r\t')
        results.append(result)

    return results


def get_data(dimension, call_type, features, service_url, api_key, date_mid_month, kw_list, db_list, url_list, current_time, last_update_date):
    if current_time.date() == last_update_date:
        if dimension == 'phrase':
            list_container = []
            for i in range(0, len(kw_list)):
                kw = kw_list[i]
                for g in range(0, len(db_list)):
                    db = db_list[g]
                    response = request(dimension=dimension, call_type=call_type, api_key=api_key, geo=db, features=features, service_url=service_url, phrase=kw, date_mid_month=date_mid_month)
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
            for i in range(0, len(url_list)):
                url = url_list[i]
                response = request(dimension=dimension, call_type=call_type, api_key=api_key, url=url, features=features, service_url=service_url)
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


def execute_sql(sql_script):
    print('Execute sql from quix')
    try:
        pc = WixPrestoConnection()
        sql_script = sql_script
        pc.execute_batch_sql(sql_script)
        print('Success, query done')
    except:
        print('Error - functions_general - execute_sql')


def get_quix_data(sql_script):
    print('Get data from quix')
    try:
        pc = WixPrestoConnection()
        sql_script = sql_script
        input = pc.execute_sql(sql_script, add_cols_metadata=True)
        data = input.get("rows")
        columns = input.get("col_names")
        df = pd.DataFrame(data, columns=columns)
        print('Managed to bring data from quix')
        return df
    except:
        print('Error - functions_general - get_quix_data')


def update_input_tables():
    script = '''
    drop table if exists sandbox.itaib.seo_kw_semrush;
    create table if not exists sandbox.itaib.seo_kw_semrush as

    select * from gdrive.bi.marketing_itaib_semrush_kw_gs;
    
    drop table if exists sandbox.itaib.seo_url_semrush;
    create table if not exists sandbox.itaib.seo_url_semrush as

    select * from gdrive.bi.marketing_itaib_semrush_url_gs;
    
    drop table if exists sandbox.itaib.seo_databases_semrush;
    create table if not exists sandbox.itaib.seo_databases_semrush as

    select * from gdrive.bi.marketing_itaib_semrush_database_gs;
    '''

    return script


def create_presto_table(table_name, df, col_dict, state):
    data = df.values.tolist()

    pc = WixPrestoConnection()

    if state == 'new':
        pc.write_data(table_name=table_name, data=data, col_dict=col_dict)
    elif state == 'replace':
        pc.write_data(table_name=table_name, data=data, if_exists=state)
    elif state == 'append':
        pc.write_data(table_name=table_name, data=data, if_exists=state)
    else:
        print('No such state, only new/replace/append')

def inspect_features(row, symbol):

    features = row.split(',')
    r = []
    for i in range(0, len(features)):
        if re.search(symbol, features[i]) is not None:
            a = 1
            r.append(a)
        else:
            a = 0
            r.append(a)
    s = sum(r)
    if s == 0:
        return 0
    else:
        return 1


def map_features(df, new_column, symbol, old_column):
    df[new_column] = df.apply(lambda row: inspect_features(row[old_column], symbol), axis=1)


def get_last_update_date(table):
    last_update_date = get_quix_data(f'select max(time_updated) as last_update from {table}').last_update[0][:10]
    return datetime.strptime(last_update_date, '%Y-%m-%d').date()
