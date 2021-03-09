#################
#### Imports ####
#################
import os.path
from datetime import datetime
from pathlib import Path

from ba_infra.presto_connection import WixPrestoConnection
from google.oauth2 import service_account

import functions as func

#####################
####  Parameters ####
#####################
DATA_FOLDER = Path(os.getcwd())

BQ_CREDENTIALS = service_account.Credentials.from_service_account_file(DATA_FOLDER / "key.json", scopes=["https://www.googleapis.com/auth/cloud-platform"])
API_KEY = func.read_json(path_file=DATA_FOLDER / "key_api.json")['api_key']

GLOBAL_PARAMETERS = func.read_json(path_file=DATA_FOLDER / "configuration.json")

# Set dates
DATE_MID_MONTH = func.mid_month()   # Data updated two weeks after the end of the month, for the previous month
CURRENT_TIME = datetime.now()

# Set Tables
GOOGLE_SHEET_KW_TABLE = 'gdrive.bi.marketing_itaib_semrush_kw_gs'
GOOGLE_SHEET_URL_TABLE = 'gdrive.bi.marketing_itaib_semrush_url_gs'
GOOGLE_SHEET_DB_TABLE = 'gdrive.bi.marketing_itaib_semrush_database_gs'

PRESTO_KW_LIST = 'sandbox.itaib.seo_kw_semrush'
PRESTO_URL_LIST = 'sandbox.itaib.seo_url_semrush'
PRESTO_DB_LIST = 'sandbox.itaib.seo_databases_semrush'

BQ_DELTA_TABLE_SEMRUSH_KW_TRAFFIC = 'temp.semrush_kw_traffic_delta'
BQ_DELTA_TABLE_SEMRUSH_URL_BACKLINKS = 'temp.semrush_url_backlinks_delta'
BQ_DELTA_TABLE_SEMRUSH_KW_SERP_FEATURES_MAP = 'temp.semrush_kw_serp_features_map_delta'

# Update input tables
pc = WixPrestoConnection()
script_kw_list = func.update_input_tables(create_presto_table=PRESTO_KW_LIST, exesting_google_sheet_table=GOOGLE_SHEET_KW_TABLE)
func.execute_sql(sql_script=script_kw_list, pc=pc)
script_url_list = func.update_input_tables(create_presto_table=PRESTO_URL_LIST, exesting_google_sheet_table=GOOGLE_SHEET_URL_TABLE)
func.execute_sql(sql_script=script_kw_list, pc=pc)
script_db_list = func.update_input_tables(create_presto_table=PRESTO_DB_LIST, exesting_google_sheet_table=GOOGLE_SHEET_DB_TABLE)
func.execute_sql(sql_script=script_kw_list, pc=pc)

# Get input tables
KW_LIST = func.get_quix_data(f'select * from {PRESTO_KW_LIST}', pc=pc).keyword.tolist()
URL_LIST = func.get_quix_data(f'select * from {PRESTO_URL_LIST}', pc=pc).url.tolist()
DB_LIST = func.get_quix_data(f'select * from {PRESTO_DB_LIST}', pc=pc).database.tolist()

# Last update dates on final tables
LAST_UPDATE_DATE_SEMRUSH_KW_TRAFFIC = func.get_last_update_date(table=f'marketing_internal_bq.{BQ_DELTA_TABLE_SEMRUSH_KW_TRAFFIC}', pc=pc)
LAST_UPDATE_DATE_SEMRUSH_URL_BACKLINKS = func.get_last_update_date(table=f'marketing_internal_bq.{BQ_DELTA_TABLE_SEMRUSH_URL_BACKLINKS}', pc=pc)
LAST_UPDATE_DATE_SEMRUSH_KW_SERP_FEATURES_MAP = func.get_last_update_date(table=f'marketing_internal_bq.{BQ_DELTA_TABLE_SEMRUSH_KW_SERP_FEATURES_MAP}', pc=pc)

# Parameters for report 'Keyword Overview'
DIMENSION_KEYWORD_OVERVIEW = GLOBAL_PARAMETERS['report_keyword_overview']['dimension']
SERVICE_URL_KEYWORD_OVERVIEW = GLOBAL_PARAMETERS['report_keyword_overview']['service_url']
CALL_TYPE_KEYWORD_OVERVIEW = GLOBAL_PARAMETERS['report_keyword_overview']['call_type']
FEATURES_KEYWORD_OVERVIEW = GLOBAL_PARAMETERS['report_keyword_overview']['features']

# Parameters for report 'Organic Results'
DIMENSION_ORGANIC_RESULTS = GLOBAL_PARAMETERS['report_organic_results']['dimension']
SERVICE_URL_ORGANIC_RESULTS = GLOBAL_PARAMETERS['report_organic_results']['service_url']
CALL_TYPE_ORGANIC_RESULTS = GLOBAL_PARAMETERS['report_organic_results']['call_type']
FEATURES_ORGANIC_RESULTS = GLOBAL_PARAMETERS['report_organic_results']['features']
SERP_FEATURES_MAP_ORGANIC_RESULTS = GLOBAL_PARAMETERS['report_organic_results']['feature_mapping']

# Parameters for report 'Indexed Pages'
DIMENSION_INDEXED_PAGES = GLOBAL_PARAMETERS['report_indexed_pages']['dimension']
SERVICE_URL_INDEXED_PAGES = GLOBAL_PARAMETERS['report_indexed_pages']['service_url']
CALL_TYPE_INDEXED_PAGES = GLOBAL_PARAMETERS['report_indexed_pages']['call_type']
FEATURES_INDEXED_PAGES = GLOBAL_PARAMETERS['report_indexed_pages']['features']

#########################
#### Request Reports ####
#########################

## Report - Keyword Overview ##
# Request Data
final_df_phrase_this = func.get_phrase_data(dimension=DIMENSION_KEYWORD_OVERVIEW, call_type=CALL_TYPE_KEYWORD_OVERVIEW,
                                            features=FEATURES_KEYWORD_OVERVIEW, service_url=SERVICE_URL_KEYWORD_OVERVIEW,
                                            api_key=API_KEY, date_mid_month=DATE_MID_MONTH, kw_list=KW_LIST, db_list=DB_LIST,
                                            current_time=CURRENT_TIME, last_update_date=LAST_UPDATE_DATE_SEMRUSH_KW_TRAFFIC)
# Organize Data
final_df_phrase_this = final_df_phrase_this.rename(columns={'Search Volume': 'search_volume'})  # bq can't accept 'Search Volume' as column name

## Report - Organic Results ##
# Request Data
final_df_phrase_organic = func.get_phrase_data(dimension=DIMENSION_ORGANIC_RESULTS, call_type=CALL_TYPE_ORGANIC_RESULTS,
                                               features=FEATURES_ORGANIC_RESULTS, service_url=SERVICE_URL_ORGANIC_RESULTS,
                                               api_key=API_KEY, date_mid_month=DATE_MID_MONTH, kw_list=KW_LIST, db_list=DB_LIST,
                                               current_time=CURRENT_TIME, last_update_date=LAST_UPDATE_DATE_SEMRUSH_URL_BACKLINKS)
# Organize Data
for key, value in SERP_FEATURES_MAP_ORGANIC_RESULTS.items():
    symbol = '^' + str(key) + '$'
    func.map_features(df=final_df_phrase_organic, new_column=value, symbol=symbol, old_column='SERP Features')
del final_df_phrase_organic['SERP Features']

## Report - Indexed Pages ##
# Request Data
final_df_backlinks_pages = func.get_url_data(dimension=DIMENSION_INDEXED_PAGES, call_type=CALL_TYPE_INDEXED_PAGES,
                                             features=FEATURES_INDEXED_PAGES, service_url=SERVICE_URL_INDEXED_PAGES,
                                             api_key=API_KEY, url_list=URL_LIST, current_time=CURRENT_TIME, last_update_date=LAST_UPDATE_DATE_SEMRUSH_KW_SERP_FEATURES_MAP)
# Organize Data
del final_df_backlinks_pages['last_seen']

###########################
#### Load data to  BQ  ####
###########################

func.load_db(data_frame=final_df_phrase_this, credentials=BQ_CREDENTIALS, table=BQ_DELTA_TABLE_SEMRUSH_KW_TRAFFIC, method='append')
func.load_db(data_frame=final_df_phrase_organic, credentials=BQ_CREDENTIALS, table=BQ_DELTA_TABLE_SEMRUSH_URL_BACKLINKS, method='append')
func.load_db(data_frame=final_df_backlinks_pages, credentials=BQ_CREDENTIALS, table=BQ_DELTA_TABLE_SEMRUSH_KW_SERP_FEATURES_MAP, method='append')

