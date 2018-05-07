# Import libraries
import os
import sys
import logging
from datetime import datetime

import pyodbc
import pandas as pd
import cartoframes

LOG_LEVEL = logging.INFO

# ODBC Connection details -- these can be pulled out into an odbc.ini file
ODBC_SOURCE_URL = 'vps348928.ovh.net'
ODBC_PORT = '5432'
ODBC_DATABASE = 'mfa'
ODBC_USER = 'mfa'
ODBC_PASSWORD = os.environ.get('mfa_db_password')

CONNECTION_STRING = 'DRIVER={};SERVER={};PORT={};DATABASE={};UID={};PWD={}'
cnxnstr = CONNECTION_STRING.format('{PostgreSQL Unicode}', ODBC_SOURCE_URL, ODBC_PORT, ODBC_DATABASE, ODBC_USER, ODBC_PASSWORD)

# Carto Connection details
CARTO_USER = os.environ.get('CARTO_WRI_RW_USER')
CARTO_PASSWORD = os.environ.get('CARTO_WRI_RW_KEY')

# Flow control
DOWNLOAD = True
# IN CASE RUN INTO TQDM PROBLEMS, refer to: https://github.com/tqdm/tqdm/issues/481

def main():
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)
    logging.info('STARTING')

    ###
    # Initialize pyodbc
    ###

    logging.info('Connection string: {}'.format(cnxnstr))
    cnxn = pyodbc.connect(cnxnstr, autocommit=True)
    cnxn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
    cnxn.setencoding(encoding='utf-8')
    cursor = cnxn.cursor()

    # For debugging purposes - there are sometimes when the tqdm package throws an error
    # This flow control allows for testing the upload process specifically
    if DOWNLOAD:

        ###
        # Fetch data
        ###

        logging.info("DEMO - run query for countries table to prove this works")

        before = datetime.now()
        countries = pd.DataFrame.from_records(cursor.execute('SELECT * FROM Country').fetchall())
        logging.info('Shape of df is: {}'.format(countries.shape))
        after = datetime.now()
        logging.info("Countries query takes {}".format(after-before))
        countries.to_csv('data/countries.csv')

        logging.info("PROCESS THE meat and POTATOES - can take some time depending on internet connection speed")

        before = datetime.now()
        logging.info("Start time for FlowMFA: {}".format(before))
        flowmfa = pd.DataFrame.from_records(cursor.execute('SELECT * FROM FlowMFA').fetchall())
        logging.info('Shape of df is: {}'.format(flowmfa.shape))
        after = datetime.now()
        logging.info("FlowMFA query takes {}".format(after-before))

        flowmfa.columns = ['index', 'isoalpha3', 'flow', 'mfa13', 'mfa4', 'year', 'amount']
        flowmfa.drop('index', inplace=True)
        flowmfa.to_csv('data/flowmfa.csv')

        # before = datetime.now()
        # flowdetailed = pd.DataFrame(cursor.execute('SELECT * FROM FlowDetailed').fetchall())
        # logging.info('Shape of df is: {}'.format(flowdetailed.shape))
        # after = datetime.now()
        # logging.info("FlowDetailed query takes {}".format(after-before))
        # flowdetailed.columns = [???]
        # flowdetailed.to_csv('data/flowdetailed.csv')

    else:

        logging.info('Attempting to load tables from docker volume')

        try:
            flowmfa = pd.read_csv('data/flowmfa.csv')
        except:
            logging.warning('flowmfa table not already available')

        try:
            flowdetailed = pd.read_csv('data/flowdetailed.csv')
        except:
            logging.warning('flowdetaild table not already available')


    ###
    # Authenticate to carto and upload data
    ###

    cc = cartoframes.CartoContext(base_url='https://{}.carto.com/'.format(CARTO_USER),
                               api_key=CARTO_PASSWORD)

    ###
    # Upload data
    ###

    cc.write(flowmfa, 'com_009_flowmfa_autoupdate', overwrite=True)
    # cc.write(flowdetailed, 'com_009_flowdetailed', overwrite=True)

    logging.info('SUCCESS')
