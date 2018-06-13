'''
Utility library for interacting with CARTO via the SQL API
from https://github.com/fgassert/cartosql.py
Example:
```
import cartosql
# CARTO_USER and CARTO_KEY read from environment if not specified
r = cartosql.get('select * from mytable', user=CARTO_USER, key=CARTO_KEY)
data = r.json()
```
Read more at:
http://carto.com/docs/carto-engine/sql-api/making-calls/
'''
from __future__ import unicode_literals
from builtins import str
import requests
import os
import logging
import json

CARTO_URL = 'https://{}.carto.com/api/v2/sql'
CARTO_USER = os.environ.get('CARTO_USER')
CARTO_KEY = os.environ.get('CARTO_KEY')
STRICT = True

def sendSql(sql, user=CARTO_USER, key=CARTO_KEY, f='', post=True):
    '''Send arbitrary sql and return response object or False'''
    url = CARTO_URL.format(user)
    payload = {
        'api_key': key,
        'q': sql,
    }
    if len(f):
        payload['format'] = f
    logging.debug((url, payload))
    if post:
        r = requests.post(url, json=payload)
    else:
        r = requests.get(url, params=payload)
    if not r.ok:
        logging.error(r.text)
        if STRICT:
            raise Exception(r.text)
        return False
    return r


def get(sql, user=CARTO_USER, key=CARTO_KEY, f=''):
    '''Send arbitrary sql and return response object or False'''
    return sendSql(sql, user, key, f, False)


def post(sql, user=CARTO_USER, key=CARTO_KEY, f=''):
    '''Send arbitrary sql and return response object or False'''
    return sendSql(sql, user, key, f)


def getFields(fields, table, where='', order='', user=CARTO_USER,
              key=CARTO_KEY, f='', post=False):
    '''Select fields from table'''
    fields = (fields,) if isinstance(fields, str) else fields
    where = ' WHERE {}'.format(where) if where else ''
    order = ' ORDER BY {}'.format(order) if order else ''
    sql = 'SELECT {} FROM "{}" {} {}'.format(
        ','.join(fields), table, where, order)
    return sendSql(sql, user, key, f, post)


def getTables(user=CARTO_USER, key=CARTO_KEY, f='csv'):
    '''Get the list of tables'''
    r = get('SELECT * FROM CDB_UserTables()',user, key, f=f)
    if f == 'csv':
        return r.text.split("\r\n")[1:-1]
    return r


def tableExists(table, user=CARTO_USER, key=CARTO_KEY):
    '''Check if table exists'''
    return table in getTables()


def createTable(table, schema, user=CARTO_USER, key=CARTO_KEY):
    '''
    Create table with schema and CartoDBfy table
    `schema` should be a dict or list of tuple pairs with
     - keys as field names and
     - values as field types
    '''
    items = schema.items() if isinstance(schema, dict) else schema
    defslist = ['{} {}'.format(k, v) for k, v in items]
    sql = 'CREATE TABLE "{}" ({})'.format(table, ','.join(defslist))
    if post(sql, user, key):
        return _cdbfyTable(table, user, key)
    return False


def _cdbfyTable(table, user=CARTO_USER, key=CARTO_KEY):
    '''CartoDBfy table so that it appears in Carto UI'''
    sql = "SELECT cdb_cartodbfytable('{}','\"{}\"')".format(user, table)
    return post(sql, user, key)


def createIndex(table, fields, unique='', using='', user=CARTO_USER,
                key=CARTO_KEY):
    '''Create index on table on field(s)'''
    fields = (fields,) if isinstance(fields, str) else fields
    f_underscore = '_'.join(fields)
    f_comma = ','.join(fields)
    unique = 'UNIQUE' if unique else ''
    using = 'USING {}'.format(using) if using else ''
    sql = 'CREATE {} INDEX idx_{}_{} ON {} {} ({})'.format(
        unique, table, f_underscore, table, using, f_comma)
    return post(sql, user, key)


def _escapeValue(value, dtype):
    '''
    Escape value for SQL based on field type
    TYPE         Escaped
    None      -> NULL
    geometry  -> string as is; obj dumped as GeoJSON
    text      -> single quote escaped
    timestamp -> single quote escaped
    varchar   -> single quote escaped
    else      -> as is
    '''
    if value is None:
        return "NULL"
    if dtype == 'geometry':
        # if not string assume GeoJSON and assert WKID
        if isinstance(value, str):
            return value
        else:
            value = json.dumps(value)
            return "ST_SetSRID(ST_GeomFromGeoJSON('{}'),4326)".format(value)
    elif dtype in ('text', 'timestamp', 'varchar'):
        # quote strings, escape quotes, and drop nbsp
        return "'{}'".format(
            str(value).replace("'", "''"))
    else:
        return str(value)


def _dumpRows(rows, dtypes):
    '''Escapes rows of data to SQL strings'''
    dumpedRows = []
    for row in rows:
        escaped = [
            _escapeValue(row[i], dtypes[i])
            for i in range(len(dtypes))
        ]
        dumpedRows.append('({})'.format(','.join(escaped)))
    return ','.join(dumpedRows)


def _insertRows(table, fields, dtypes, rows, user=CARTO_USER, key=CARTO_KEY):
    values = _dumpRows(rows, tuple(dtypes))
    sql = 'INSERT INTO "{}" ({}) VALUES {}'.format(
        table, ', '.join(fields), values)
    return post(sql, user, key)


def insertRows(table, fields, dtypes, rows, user=CARTO_USER,
               key=CARTO_KEY, blocksize=1000):
    '''
    Insert rows into table
    `rows` must be a list of lists containing the data to be inserted
    `fields` field names for the columns in `rows`
    `dtypes` field types for the columns in `rows`
    Automatically breaks into multiple requests at `blocksize` rows
    '''
    # iterate in blocks
    while len(rows):
        if not _insertRows(table, fields, dtypes, rows[:blocksize], user, key):
            return False
        rows = rows[blocksize:]
    return True

# Alias insertRows
blockInsertRows = insertRows


def deleteRows(table, where, user=CARTO_USER, key=CARTO_KEY):
    '''Delete rows from table'''
    sql = 'DELETE FROM "{}" WHERE {}'.format(table, where)
    return post(sql)


def deleteRowsByIDs(table, ids, id_field='cartodb_id', dtype='',
                    user=CARTO_USER, key=CARTO_KEY):
    '''Delete rows from table by IDs'''
    if dtype:
        ids = [_escapeValue(i, dtype) for i in ids]
    where = '{} in ({})'.format(id_field, ','.join(ids))
    return deleteRows(table, where, user, key)


def dropTable(table, user=CARTO_USER, key=CARTO_KEY):
    '''Delete table'''
    sql = 'DROP TABLE "{}"'.format(table)
    return post(sql)

def truncateTable(table, user=CARTO_USER, key=CARTO_KEY):
    '''Delete table'''
    sql = 'TRUNCATE TABLE "{}"'.format(table)
    return post(sql)

if __name__ == '__main__':
    from . import cli
    cli.main()