from __future__ import unicode_literals

import os
import sys
import logging

import pyodbc

# constants for bleaching alerts
SOURCE_URL = 'vps348928.ovh.net'
PORT = '5432'
DATABASE = 'mfa'
USER = 'mfa'
PASSWORD = os.environ.get('mfa_db_password')

CONNECTION_STRING = 'DRIVER={};SERVER={};PORT={};DATABASE={};UID={};PWD={}'
cnxn = CONNECTION_STRING.format('{PostgreSQL Unicode}',SOURCE_URL, PORT, DATABASE, USER, PASSWORD)

def main():
    '''Ingest new data into EE and delete old data'''
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # Initialize pyodbc
    logging.info('Connection string: {}'.format(cnxn))
    myconnection = pyodbc.connect(cnxn, autocommit=True)

    logging.info('SUCCESS')
