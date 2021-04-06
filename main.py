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

###########################
####  Main API Process ####
###########################
for report in GLOBAL_PARAMETERS['reports']:
    # Get Parameters
    DIMENSION, SERVICE_URL, CALL_TYPE, FEATURES, SERP_FEATURES_MAP, BQ_DELTA_TABLE = func.get_report_parameters(parameters_file=GLOBAL_PARAMETERS, report_type=report)
    LAST_UPDATE_DATE = func.get_last_update_date(table=f'marketing_internal_bq.{BQ_DELTA_TABLE}', pc=pc)
    # Get API Data
    if DIMENSION == 'phrase':
        final_df = func.get_phrase_data(dimension=DIMENSION, call_type=CALL_TYPE, features=FEATURES,
                                        service_url=SERVICE_URL, api_key=API_KEY, date_mid_month=DATE_MID_MONTH,
                                        kw_list=KW_LIST, db_list=DB_LIST, last_update_date=LAST_UPDATE_DATE)
    else:
        final_df = func.get_url_data(dimension=DIMENSION, call_type=CALL_TYPE, features=FEATURES,
                                     service_url=SERVICE_URL, api_key=API_KEY, url_list=URL_LIST,
                                     last_update_date=LAST_UPDATE_DATE)
    # Organize Data
    if report == 'report_keyword_overview':
        final_df = final_df.rename(columns={'Search Volume': 'search_volume'})  # bq can't accept 'Search Volume' as column name
    elif report == 'report_organic_results':
        for key, value in SERP_FEATURES_MAP.items():
            symbol = '^' + str(key) + '$'
            func.map_features(df=final_df, new_column=value, symbol=symbol, old_column='SERP Features')
        del final_df['SERP Features']
    else:
        del final_df['last_seen']
    # Load Data to BQ
    func.load_db(data_frame=final_df, credentials=BQ_CREDENTIALS, table=BQ_DELTA_TABLE, method='append')


