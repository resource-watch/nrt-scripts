from __future__ import unicode_literals

import os
import sys
import logging

import pyodbc

#from . import queries as q
q = {}
q['FlowMFA'] = '''
SELECT c.Name AS Country, d.Country AS ISOAlpha3, f.Name AS Flow, m2.Name AS MFA13, m.Name AS MFA4, d.Year AS Year, d.Amount AS Amount
    FROM FlowMFA d LEFT JOIN Country c ON d.Country = c.Code
	LEFT JOIN Flow f ON d.Flow = f.Code
	LEFT JOIN MFA13 m2 ON d.MFA13 = m2.Code
	LEFT JOIN MFA4 m ON d.MFA4 = m.Code
	ORDER BY Flow, Year, MFA4, Country, MFA13;
  '''


q['FlowDetailed'] = '''
SELECT d.Year AS Year, c1.Name AS OriginCountry, d.Source AS OriginISOAlpha3, c2.Name AS ConsumerCountry, d.Destination AS ConsumerISOAlpha3, m.Name AS MFA4, p.Name AS ProductGroup, d.Amount AS Amount
	FROM FlowDetailed d LEFT JOIN Country c1 ON d.Source = c1.Code
	LEFT JOIN Country c2 ON d.Destination = c2.Code
	LEFT JOIN MFA4 m ON d.MFA4 = m.Code
	LEFT JOIN Productgroup p ON d.ProductGroup = p.Code
	ORDER BY Year, MFA4, ConsumerCountry, ProductGroup, OriginCountry;
  '''

q['Footprint'] = '''
SELECT d.Year AS Year, c2.Name AS ConsumerCountry, d.Destination AS ConsumerISOAlpha3, m.Name AS MFA4, sum(d.Amount) AS Amount
	FROM FlowDetailed d
	LEFT JOIN Country c2 ON d.Destination = c2.Code
	LEFT JOIN MFA4 m ON d.MFA4 = m.Code
	LEFT JOIN Productgroup p ON d.ProductGroup = p.Code
	GROUP BY Year, MFA4, ConsumerCountry
	ORDER BY Year, MFA4, ConsumerCountry;
  '''

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
    logging.info('Connection string: {}'.format(cnxnstr))
    cnxn = pyodbc.connect(cnxnstr, autocommit=True)
    cnxn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
    cnxn.setencoding(encoding='utf-8')

    cursor = cnxn.cursor()

    for row in cursor.execute('SELECT * FROM Country').fetchall():
        logging.info(row)

    logging.info('SUCCESS')
