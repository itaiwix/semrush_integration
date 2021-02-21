#################
#### Imports ####
#################

from datetime import datetime

import functions as func

###########################
#### Global Parameters ####
###########################

api_key = # Upon production the key will be taken from airflow storage, during development the key is taken from text file

# Set dates
date_mid_month = func.mid_month()   # Data updated two weeks after the end of the month, for the previous month
current_time = datetime.now()

# Update input tables
script = func.update_input_tables()
func.execute_sql(sql_script=script)

# Get input tables
db_list = func.get_quix_data('select * from sandbox.itaib.seo_databases_semrush').database.tolist()
kw_list = func.get_quix_data('select * from sandbox.itaib.seo_kw_semrush limit 2').keyword.tolist()  # todo: remove 'limit 10' when script is ready
url_list = func.get_quix_data('select * from sandbox.itaib.seo_url_semrush limit 2').url.tolist()  # todo: remove 'limit 10' when script is ready

# Last update dates on final tables
last_update_date_semrush_kw_traffic = func.get_last_update_date(table='sandbox.itaib.semrush_kw_traffic')
last_update_date_semrush_url_backlinks = func.get_last_update_date(table='sandbox.itaib.semrush_url_backlinks')
last_update_date_semrush_kw_serp_features_map = func.get_last_update_date(table='sandbox.itaib.semrush_kw_serp_features_map')

#########################
#### Request Reports ####
#########################

#### Keyword Reports ####

# Set Local Parameters
dimension = 'phrase'
service_url = 'https://api.semrush.com'

## Table 1.1 - Keyword Overview ##

# Set Local Parameters
call_type = 'phrase_this'
features = 'Nq'
# Request Data
final_df_phrase_this = func.get_data(dimension=dimension, call_type=call_type, features=features, service_url=service_url,
                                     api_key=api_key, date_mid_month=date_mid_month, kw_list=kw_list, db_list=db_list,
                                     url_list=url_list, current_time=current_time, last_update_date=last_update_date_semrush_kw_traffic)
## Table 1.2 - Organic Results ##

# Set Local Parameters
call_type = 'phrase_organic'
features = 'Dn,Ur,Fp'
serp_features_map = {
    '0': 'instant_answer',
    '6':  'site_links',
    '11': 'featured_snippet',
    '22': 'faq',
}
# Request Data
final_df_phrase_organic = func.get_data(dimension=dimension, call_type=call_type, features=features, service_url=service_url,
                                        api_key=api_key, date_mid_month=date_mid_month, kw_list=kw_list, db_list=db_list,
                                        url_list=url_list, current_time=current_time, last_update_date=last_update_date_semrush_url_backlinks)
# Organize Data
for key, value in serp_features_map.items():
    symbol = '^' + str(key) + '$'
    func.map_features(df=final_df_phrase_organic, new_column=value, symbol=symbol, old_column='SERP Features')
del final_df_phrase_organic['SERP Features']

#### Backlinks Reports ####

# Set Local Parameters
dimension = 'url'
service_url = 'https://api.semrush.com/analytics/v1/'

## Table 2 - Indexed Pages ##

# Set Local Parameters
call_type = 'backlinks_pages'
features = 'source_url,backlinks_num,domains_num,last_seen'
# Request Data
final_df_backlinks_pages = func.get_data(dimension=dimension, call_type=call_type, features=features, service_url=service_url,
                                         api_key=api_key, date_mid_month=date_mid_month, kw_list=kw_list, db_list=db_list,
                                         url_list=url_list, current_time=current_time, last_update_date=last_update_date_semrush_kw_serp_features_map)
# Organize Data
del final_df_backlinks_pages['last_seen']

#####################
#### Save Tabels ####
#####################

date_types_phrase_this = {
    'database': 'varchar',
    'phrase': 'varchar',
    'traffic': 'integer',
    'display_date': 'varchar',
    'time_updated': 'varchar'
}
func.create_presto_table(table_name='sandbox.itaib.semrush_kw_traffic', df=final_df_phrase_this, col_dict=date_types_phrase_this, state='append')

date_types_phrase_organic = {
    'keyword': 'varchar',
    'database': 'varchar',
    'display_date': 'varchar',
    'Domain': 'varchar',
    'Url': 'varchar',
    'instant_answer': 'integer',
    'site_links': 'integer',
    'featured_snippet': 'integer',
    'faq': 'integer',
    'time_updated': 'varchar'
}
func.create_presto_table(table_name='sandbox.itaib.semrush_url_backlinks', df=final_df_phrase_organic, col_dict=date_types_phrase_organic, state='append')

date_types_backlinks_pages = {
    'source_url': 'varchar',
    'backlinks_num': 'integer',
    'domains_num': 'integer',
    'time_updated': 'varchar',
    'last_seen_date': 'varchar'
}
func.create_presto_table(table_name='sandbox.itaib.semrush_kw_serp_features_map', df=final_df_backlinks_pages, col_dict=date_types_backlinks_pages, state='append')
