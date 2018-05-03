# Import libraries
import os
import sys
import logging
from datetime import datetime

import pyodbc
import pandas as pd
import cartoframes

# ODBC Connection details -- these can be pulled out into an odbc.ini file
ODBC_SOURCE_URL = 'vps348928.ovh.net'
ODBC_PORT = '5432'
ODBC_DATABASE = 'mfa'
ODBC_USER = 'mfa'
ODBC_PASSWORD = os.environ.get('mfa_db_password')

CONNECTION_STRING = 'DRIVER={};SERVER={};PORT={};DATABASE={};UID={};PWD={}'
cnxnstr = CONNECTION_STRING.format('{PostgreSQL Unicode}', ODBC_SOURCE_URL, ODBC_PORT, ODBC_DATABASE, ODBC_USER, ODBC_PASSWORD)

# Carto Connection details
CARTO_USER = os.environ.get('carto_user')
CARTO_PASSWORD = os.environ.get('carto_password')

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # Initialize pyodbc
    logging.info('Connection string: {}'.format(cnxnstr))
    cnxn = pyodbc.connect(cnxnstr, autocommit=True)
    cnxn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
    cnxn.setencoding(encoding='utf-8')
    cursor = cnxn.cursor()

    # Fetch data
    logging.info("DEMO - run query for countries table to prove this works")
    before = datetime.now()
    countries = pd.DataFrame(cursor.execute('SELECT * FROM Country').fetchall())
    logging.info('Shape of df is: {}'.format(countries.shape))
    after = datetime.now()
    logging.info("Countries query takes {}".format(after-before))

    logging.info("PROCESS THE meat and POTATOES")
    before = datetime.now()
    flowmfa = pd.DataFrame(cursor.execute('SELECT * FROM FlowMFA').fetchall())
    logging.info('Shape of df is: {}'.format(flowmfa.shape))
    after = datetime.now()
    logging.info("FlowMFA query takes {}".format(after-before))

    before = datetime.now()
    flowdetailed = pd.DataFrame(cursor.execute('SELECT * FROM FlowDetailed').fetchall())
    logging.info('Shape of df is: {}'.format(flowdetailed.shape))
    after = datetime.now()
    logging.info("FlowDetailed query takes {}".format(after-before))

    # Authenticate to carto and upload data
    cc = cartoframes.CartoContext(base_url='https://{}.carto.com/'.format(CARTO_USER),
                               api_key=CARTO_PASSWORD)

    cc.write(flowmfa, 'com_009_flowmfa', overwrite=True)
    cc.write(flowdetailed, 'com_009_flowdetailed', overwrite=True)

    logging.info('SUCCESS')
