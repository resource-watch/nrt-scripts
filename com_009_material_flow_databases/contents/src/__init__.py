from __future__ import unicode_literals

import os
import sys
import logging

import pyodbc

import queries as q

# constants for bleaching alerts
SOURCE_URL = 'vps348928.ovh.net'
PORT = '5432'
DATABASE = 'mfa'
USER = 'mfa'
PASSWORD = os.environ.get('mfa_db_password')

CONNECTION_STRING = 'DRIVER={};SERVER={};PORT={};DATABASE={};UID={};PWD={}'
cnxnstr = CONNECTION_STRING.format('{PostgreSQL Unicode}',SOURCE_URL, PORT, DATABASE, USER, PASSWORD)

def main():
    '''Ingest new data into EE and delete old data'''
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # Initialize pyodbc
    logging.info('Connection string: {}'.format(cnxn))
    cnxn = pyodbc.connect(cnxnstr, autocommit=True)
    cnxn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
    cnxn.setencoding(encoding='utf-8')

    cursor = cnxn.cursor()

    print(q.FlowMFA)

    #cursor.execute("select * from tmp").fetchone()

    logging.info('SUCCESS')
