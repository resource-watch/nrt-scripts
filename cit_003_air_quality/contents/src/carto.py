import requests
import os
import logging
import json

CARTO_URL = "https://{}.carto.com/api/v2/sql"
CARTO_USER = os.environ.get('CARTO_USER')
CARTO_KEY = os.environ.get('CARTO_KEY')
STRICT = True

def sendSql(sql, user=CARTO_USER, key=CARTO_KEY, f='', post=True):
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
    return sendSql(sql, user, key, f, False)

def post(sql, user=CARTO_USER, key=CARTO_KEY, f=''):
    return sendSql(sql, user, key, f)

def getFields(fields='*', table='', user=CARTO_USER, key=CARTO_KEY, where='', order='', f=''):
    if type(fields) == str:
        fields = (fields,)
    if len(where):
        where = ' WHERE {}'.format(where)
    if len(order):
        order = ' ORDER BY {}'.format(order)
    sql = 'SELECT {} FROM "{}"{}{}'.format(','.join(fields), table, where, order)
    return get(sql, user, key, f=f)

def getTables():
    r = get('SELECT * FROM CDB_UserTables()', f='csv')
    return r.text.split("\r\n")[1:-1]

def tableExists(table):
    return table in getTables()

def createTable(table, schema, user=CARTO_USER, key=CARTO_KEY):
    defslist = ['{} {}'.format(k, v) for k, v in schema.items()]
    sql = 'CREATE TABLE "{}" ({})'.format(table, ','.join(defslist))
    if post(sql, user, key):
        return cdbfyTable(table, user, key)

def cdbfyTable(table, user=CARTO_USER, key=CARTO_KEY):
    sql = "SELECT cdb_cartodbfytable('{}','\"{}\"')".format(user, table)
    return post(sql, user, key)

def createIndex(table, fields, unique=False, user=CARTO_USER, key=CARTO_KEY):
    if type(fields) is str:
        fields = (fields,)
    f_underscore = '_'.join(fields)
    f_comma = ','.join(fields)
    unique = 'UNIQUE' if unique else ''
    sql = 'CREATE {} INDEX idx_{}_{} ON {} ({})'.format(
        unique, table, f_underscore, table, f_comma)
    return post(sql, user, key)

def _escapeValue(value, dtype):
    if value is None:
        return "NULL"
    if dtype == 'geometry':
        # assume GeoJSON and assert WKID
        if type(value) is not str:
            value = json.dumps(value)
        return "ST_SetSRID(ST_GeomFromGeoJSON('{}'),4326)".format(value)
    elif dtype in ('text', 'timestamp', 'varchar'):
        # quote strings, escape quotes, and drop nbsp
        return "'{}'".format(str(value).replace('\xa0', ' ').replace("'", "''"))
    else:
        return str(value)

def _dumpRows(rows, dtypes):
    dumpedRows = []
    for row in rows:
        escaped = [_escapeValue(row[i], dtypes[i]) for i in range(len(dtypes))]
        dumpedRows.append('({})'.format(','.join(escaped)))
    return ','.join(dumpedRows)

def insertRows(table, schema, rows, user=CARTO_USER, key=CARTO_KEY):
    fields = tuple(schema.keys())
    dtypes = tuple(schema.values())
    values = _dumpRows(rows, dtypes)
    sql = 'INSERT INTO "{}" ({}) VALUES {}'.format(table, ', '.join(fields), values)
    return post(sql, user, key)

def blockInsertRows(table, schema, rows, user=CARTO_USER, key=CARTO_KEY, blocksize=1000):
    # iterate in blocks
    while len(rows):
        if not insertRows(table, schema, rows[:blocksize], user, key):
            return False
        rows = rows[blocksize:]
    return True

def deleteRows(table, where, user=CARTO_USER, key=CARTO_KEY):
    sql = 'DELETE FROM "{}" WHERE {}'.format(table, where)
    return post(sql)

def deleteRowsByIDs(table, id_field, ids, user=CARTO_USER, key=CARTO_KEY):
    where = '{} in ({})'.format(id_field, ','.join(ids))
    return deleteRows(table, where, user, key)

def dropTable(table, user=CARTO_USER, key=CARTO_KEY):
    sql = 'DROP TABLE "{}"'.format(table)
    return post(sql)
