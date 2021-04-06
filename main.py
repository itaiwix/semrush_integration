#################
#### Imports ####
#################
import os.path
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

# Set Tables
GOOGLE_SHEET_KW_TABLE, GOOGLE_SHEET_URL_TABLE, GOOGLE_SHEET_DB_TABLE = func.get_table_parameters(parameters_file=GLOBAL_PARAMETERS, table_type='google_sheets')
PRESTO_KW_TABLE, PRESTO_URL_TABLE, PRESTO_DB_TABLE = func.get_table_parameters(parameters_file=GLOBAL_PARAMETERS, table_type='presto_tables')

# Set query parameters
pc = WixPrestoConnection()
KW_LIST = func.get_query_parameters(input_table=GOOGLE_SHEET_KW_TABLE, storage_table=PRESTO_KW_TABLE, pc=pc)
URL_LIST = func.get_query_parameters(input_table=GOOGLE_SHEET_URL_TABLE, storage_table=PRESTO_URL_TABLE, pc=pc)
DB_LIST = func.get_query_parameters(input_table=GOOGLE_SHEET_DB_TABLE, storage_table=PRESTO_DB_TABLE, pc=pc)

###################################
#### Report - Keyword Overview ####
###################################
# Get Parameters
DIMENSION_KEYWORD_OVERVIEW, SERVICE_URL_KEYWORD_OVERVIEW, CALL_TYPE_KEYWORD_OVERVIEW, FEATURES_KEYWORD_OVERVIEW, SERP_FEATURES_MAP_KEYWORD_OVERVIEW, BQ_DELTA_TABLE_SEMRUSH_KW_TRAFFIC = func.get_report_parameters(parameters_file=GLOBAL_PARAMETERS, report_type='report_keyword_overview')
LAST_UPDATE_DATE_SEMRUSH_KW_TRAFFIC = func.get_last_update_date(table=f'marketing_internal_bq.{BQ_DELTA_TABLE_SEMRUSH_KW_TRAFFIC}', pc=pc)
# Request Data
final_df_phrase_this = func.get_phrase_data(dimension=DIMENSION_KEYWORD_OVERVIEW, call_type=CALL_TYPE_KEYWORD_OVERVIEW,
                                            features=FEATURES_KEYWORD_OVERVIEW, service_url=SERVICE_URL_KEYWORD_OVERVIEW,
                                            api_key=API_KEY, date_mid_month=DATE_MID_MONTH, kw_list=KW_LIST, db_list=DB_LIST,
                                            last_update_date=LAST_UPDATE_DATE_SEMRUSH_KW_TRAFFIC)
# Organize Data
final_df_phrase_this = final_df_phrase_this.rename(columns={'Search Volume': 'search_volume'})  # bq can't accept 'Search Volume' as column name
# Load to bq
func.load_db(data_frame=final_df_phrase_this, credentials=BQ_CREDENTIALS, table=BQ_DELTA_TABLE_SEMRUSH_KW_TRAFFIC, method='append')

##############################
## Report - Organic Results ##
##############################
# Get Parameters
DIMENSION_ORGANIC_RESULTS, SERVICE_URL_ORGANIC_RESULTS, CALL_TYPE_ORGANIC_RESULTS, FEATURES_ORGANIC_RESULTS, SERP_FEATURES_MAP_ORGANIC_RESULTS, BQ_DELTA_TABLE_SEMRUSH_URL_BACKLINKS = func.get_report_parameters(parameters_file=GLOBAL_PARAMETERS, report_type='report_organic_results')
LAST_UPDATE_DATE_SEMRUSH_URL_BACKLINKS = func.get_last_update_date(table=f'marketing_internal_bq.{BQ_DELTA_TABLE_SEMRUSH_URL_BACKLINKS}', pc=pc)
# Request Data
final_df_phrase_organic = func.get_phrase_data(dimension=DIMENSION_ORGANIC_RESULTS, call_type=CALL_TYPE_ORGANIC_RESULTS,
                                               features=FEATURES_ORGANIC_RESULTS, service_url=SERVICE_URL_ORGANIC_RESULTS,
                                               api_key=API_KEY, date_mid_month=DATE_MID_MONTH, kw_list=KW_LIST, db_list=DB_LIST,
                                               last_update_date=LAST_UPDATE_DATE_SEMRUSH_URL_BACKLINKS)
# Organize Data
for key, value in SERP_FEATURES_MAP_ORGANIC_RESULTS.items():
    symbol = '^' + str(key) + '$'
    func.map_features(df=final_df_phrase_organic, new_column=value, symbol=symbol, old_column='SERP Features')
del final_df_phrase_organic['SERP Features']
# Load to bq
func.load_db(data_frame=final_df_phrase_organic, credentials=BQ_CREDENTIALS, table=BQ_DELTA_TABLE_SEMRUSH_URL_BACKLINKS, method='append')

################################
#### Report - Indexed Pages ####
################################
# Get Parameters
DIMENSION_INDEXED_PAGES, SERVICE_URL_INDEXED_PAGES, CALL_TYPE_INDEXED_PAGES, FEATURES_INDEXED_PAGES, SERP_FEATURES_MAP_INDEXED_PAGES, BQ_DELTA_TABLE_SEMRUSH_KW_SERP_FEATURES_MAP = func.get_report_parameters(parameters_file=GLOBAL_PARAMETERS, report_type='report_indexed_pages')
LAST_UPDATE_DATE_SEMRUSH_KW_SERP_FEATURES_MAP = func.get_last_update_date(table=f'marketing_internal_bq.{BQ_DELTA_TABLE_SEMRUSH_KW_SERP_FEATURES_MAP}', pc=pc)
# Request Data
final_df_backlinks_pages = func.get_url_data(dimension=DIMENSION_INDEXED_PAGES, call_type=CALL_TYPE_INDEXED_PAGES,
                                             features=FEATURES_INDEXED_PAGES, service_url=SERVICE_URL_INDEXED_PAGES,
                                             api_key=API_KEY, url_list=URL_LIST, last_update_date=LAST_UPDATE_DATE_SEMRUSH_KW_SERP_FEATURES_MAP)
# Organize Data
del final_df_backlinks_pages['last_seen']
# Load to bq
func.load_db(data_frame=final_df_backlinks_pages, credentials=BQ_CREDENTIALS, table=BQ_DELTA_TABLE_SEMRUSH_KW_SERP_FEATURES_MAP, method='append')


