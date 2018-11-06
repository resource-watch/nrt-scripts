import os
import logging
import sys
import requests as req
from collections import OrderedDict
from datetime import datetime, timedelta
import cartosql
from functools import reduce

# Constants
LOG_LEVEL = logging.INFO

ALPS_URL = 'http://dataviz.vam.wfp.org/api/GetAlps?ac={country_code}'
MARKETS_URL = 'http://dataviz.vam.wfp.org/api/GetMarkets?ac={country_code}'

CLEAR_TABLE_FIRST = False
TOLERATE_TRIES = 100
DATE_FORMAT = '%Y/%m/%d'
TIME_STEP = {'days':31}

CARTO_ALPS_TABLE = 'foo_053a_alerts_for_price_spikes'
CARTO_ALPS_SCHEMA = OrderedDict([
    ("uid", "text"),
    ("date", "timestamp"),
    ("currency","text"),
    ("mktid","int"),
    ("cmid","int"),
    ("ptid","int"),
    ("umid","int"),
    ("catid","int"),
    ("unit","text"),
    ("cmname","text"),
    ("category","text"),
    ("mktname","text"),
    ("admname","text"),
    ("adm1id","int"),
    ("sn","text"),
    ("forecast","text"),
    ("mp_price","numeric"),
    ("trend","numeric"),
    ("pewi","numeric"),
    ("alps","text")
])

CARTO_MARKET_TABLE = 'foo_053b_monitored_markets'
CARTO_MARKET_SCHEMA = OrderedDict([
    ("uid", "text"),
    ("the_geom", "geometry"),
    ("region_name", "text"),
    ("region_id", "int"),
    ("market_name", "text"),
    ("market_id", "int"),
])

UID_FIELD = 'uid'
TIME_FIELD = 'date'

# Limit 1M rows, drop older than 10yrs
MAXROWS = 1000000
#MAXAGE = datetime.datetime.today() - datetime.timedelta(days=3650)
DATASET_ID = 'acf42a1b-104b-4f81-acd0-549f805873fb'


def lastUpdateDate(dataset, date):
    apiUrl = 'http://api.resourcewatch.org/v1/dataset/{dataset}'.format(dataset =dataset)
    headers = {
    'Content-Type': 'application/json',
    'Authorization': os.getenv('apiToken')
    }
    body = {
        "dataLastUpdated": date
    }
    try:
        r = requests.patch(url = apiUrl, json = body, headers = headers)
        logging.info('[lastUpdated]: SUCCESS, status code'+str(r.status_code))
        return 0
    except Exception as e:
        logging.error('[lastUpdated]: '+str(e))
        logging.error('[lastUpdated]: status code'+str(r.status_code))


def genAlpsUID(sn, date, forecast):
    '''Generate unique id'''
    return '{}_{}_{}'.format(sn, date, forecast)

def genMarketUID(rid, mid, mname):
    '''Generate unique id'''
    return '{}_{}_{}'.format(rid,mid,mname)

## MAP
def parseMarkets(region_scale, existing_markets):
    # Happens w/ 'National Average' entries
    if 'items' not in region_scale:
        logging.debug('Unfamiliar structure, probably National Average entry')
        #logging.debug(region_scale)
        return [None]*len(CARTO_MARKET_SCHEMA)

    new_rows = []
    region_id = region_scale['id']
    region_name = region_scale['text']

    for mkt in region_scale['items']:
        market_id = mkt['id'].replace('mk', '')
        market_name = mkt['text']
        geom = {
            "type": "Point",
            "coordinates": [
                mkt['lon'],
                mkt['lat']
            ]
        }

        uid = genMarketUID(region_id, market_id, market_name)
        if uid not in existing_markets:
            existing_markets.append(uid)

            row = []
            for field in CARTO_MARKET_SCHEMA.keys():
                if field == 'uid':
                    row.append(uid)
                elif field == 'the_geom':
                    row.append(geom)
                elif field == 'region_name':
                    row.append(region_name)
                elif field == 'region_id':
                    row.append(region_id)
                elif field == 'market_name':
                    row.append(market_name)
                elif field == 'market_id':
                    row.append(market_id)
            new_rows.append(row)

    return new_rows


def stepForward(start):
    return (start + timedelta(**TIME_STEP)).replace(day=15)

def assignALPS(pewi):
    if pewi < .25:
        return 'Normal'
    elif pewi < 1:
        return 'Stress'
    elif pewi < 2:
        return 'Alert'
    else:
        return 'Crisis'

## MAP
def parseAlps(market_data, existing_alps):
    # Happens w/ 'National Average' entries
    if 'admname' not in market_data:
        logging.debug('Unfamiliar structure, probably National Average entry')
        #logging.debug(market_data)
        return [None]*len(CARTO_ALPS_SCHEMA)

    new_rows = []
    # These are not always the same length, i.e. 23
    # FLAG FOR WFP
    num_obs = min(len(market_data['mp_price']), len(market_data['trend']), len(market_data['pewi']))

    run_forecast = True
    try:
        num_forecast = min(len(market_data['f_price']), len(market_data['p_trend']), len(market_data['f_pewi']))
    except:
        logging.debug('No forecast')
        #logging.debug(market_data)
        run_forecast = False

    date = datetime.strptime(market_data['startdate'], DATE_FORMAT)
    for i in range(num_obs):
        mp_price = market_data['mp_price'][i]
        trend = market_data['trend'][i]
        pewi = market_data['pewi'][i]

        # This data point will be filtered out later
        if not pewi:
            logging.debug('No alert data for this month')
            #logging.debug(market_data)
            new_rows.append([None]*len(CARTO_ALPS_SCHEMA))
            date = stepForward(date)
            continue

        # If get here, that that pewi is not null
        alps = assignALPS(pewi)

        uid = genAlpsUID(market_data['sn'], date, False)
        if uid not in existing_alps:
            existing_alps.append(uid)

            row = []
            for field in CARTO_ALPS_SCHEMA.keys():
                if field == 'uid':
                    row.append(uid)
                elif field == 'mp_price':
                    row.append(mp_price)
                elif field == 'trend':
                    row.append(trend)
                elif field == 'pewi':
                    row.append(pewi)
                elif field == 'alps':
                    row.append(alps)
                elif field == 'date':
                    row.append(date.strftime(DATE_FORMAT))
                elif field == 'forecast':
                    row.append(False)
                else:
                    row.append(market_data[field])

            new_rows.append(row)
        date = stepForward(date)

    if run_forecast:
        for i in range(num_forecast):
            f_price = market_data['f_price'][i]
            p_trend = market_data['p_trend'][i]
            f_pewi = market_data['f_pewi'][i]

            # This data point will be filtered out later
            if not f_pewi:
                logging.debug('No alert data forecast for this month')
                #logging.debug(market_data)
                new_rows.append([None]*len(CARTO_ALPS_SCHEMA))
                date = stepForward(date)
                continue

            # If get here, that that pewi is not null
            f_alps = assignALPS(f_pewi)

            uid = genAlpsUID(market_data['sn'], date, True)
            if uid not in existing_alps:
                existing_alps.append(uid)

                row = []
                for field in CARTO_ALPS_SCHEMA.keys():
                    if field == 'uid':
                        row.append(uid)
                    elif field == 'mp_price':
                        row.append(f_price)
                    elif field == 'trend':
                        row.append(p_trend)
                    elif field == 'pewi':
                        row.append(f_pewi)
                    elif field == 'alps':
                        row.append(f_alps)
                    elif field == 'date':
                        row.append(date.strftime(DATE_FORMAT))
                    elif field == 'forecast':
                        row.append(True)
                    else:
                        row.append(market_data[field])

                new_rows.append(row)

            date = stepForward(date)

    return new_rows

## REDUCE
def flatten(lst, items):
    lst.extend(items)
    return lst

## FILTER
def clean_null_rows(row):
    return any(row)

def processNewData(existing_markets, existing_alps):
    '''
    Iterively fetch parse and post new data
    '''
    futile_tries = 0
    country_code = 0
    num_new_markets = 0
    num_new_alps = 0
    # get and parse each page; stop when no new results or 200 pages
    while futile_tries < TOLERATE_TRIES:

        # 1. Fetch new data
        logging.info("Fetching country code {}".format(country_code))
        markets = req.get(MARKETS_URL.format(country_code=country_code)).json()
        alps = req.get(ALPS_URL.format(country_code=country_code)).json()

        if len(markets) == len(alps) == 0:
            futile_tries += 1
            country_code += 1
            continue
        else:
            futile_tries = 0
            country_code += 1

        # 2. Parse data excluding existing observations
        new_markets = list(map(lambda mkt: parseMarkets(mkt, existing_markets), markets))
        new_alps = list(map(lambda alp: parseAlps(alp, existing_alps), alps))

        logging.debug('Country {} Data: After map:'.format(country_code))
        logging.debug(new_markets)
        logging.debug(new_alps)

        new_markets = reduce(flatten, new_markets , [])
        new_alps = reduce(flatten, new_alps, [])

        logging.debug('Country {} Data: After reduce:'.format(country_code))
        logging.debug(new_markets)
        logging.debug(new_alps)

        # Ensure new_<rows> is a list of lists, even if only one element
        if len(new_markets):
            if type(new_markets[0]) != list:
                new_markets = [new_markets]
        if len(new_alps):
            if type(new_alps[0]) != list:
                new_alps = [new_alps]

        # Clean any rows that are all None
        new_markets = list(filter(clean_null_rows, new_markets))
        new_alps = list(filter(clean_null_rows, new_alps))

        logging.debug('Country {} Data: After filter:'.format(country_code))
        logging.debug(new_markets)
        logging.debug(new_alps)

        num_new_markets += len(new_markets)
        num_new_alps += len(new_alps)

        # 3. Insert new rows

        if num_new_markets:
            logging.info('Pushing {} new Markets rows'.format(num_new_markets))
            cartosql.insertRows(CARTO_MARKET_TABLE, CARTO_MARKET_SCHEMA.keys(),
                                CARTO_MARKET_SCHEMA.values(), new_markets)
        if num_new_alps:
            logging.info('Pushing {} new ALPS rows'.format(num_new_alps))
            cartosql.insertRows(CARTO_ALPS_TABLE, CARTO_ALPS_SCHEMA.keys(),
                                CARTO_ALPS_SCHEMA.values(), new_alps)


    return num_new_markets, num_new_alps


##############################################################
# General logic for Carto
# should be the same for most tabular datasets
##############################################################

def createTableWithIndex(table, schema, id_field, time_field=''):
    '''Get existing ids or create table'''
    cartosql.createTable(table, schema)
    cartosql.createIndex(table, id_field, unique=True)
    if time_field:
        cartosql.createIndex(table, time_field)


def getIds(table, id_field):
    '''get ids from table'''
    r = cartosql.getFields(id_field, table, f='csv')
    return r.text.split('\r\n')[1:-1]


def deleteExcessRows(table, max_rows, time_field, max_age=''):
    '''Delete excess rows by age or count'''
    num_dropped = 0
    if isinstance(max_age, datetime):
        max_age = max_age.isoformat()

    # 1. delete by age
    if max_age:
        r = cartosql.deleteRows(table, "{} < '{}'".format(time_field, max_age))
        num_dropped = r.json()['total_rows']

    # 2. get sorted ids (old->new)
    r = cartosql.getFields('cartodb_id', table, order='{}'.format(time_field),
                           f='csv')
    ids = r.text.split('\r\n')[1:-1]

    # 3. delete excess
    if len(ids) > max_rows:
        r = cartosql.deleteRowsByIDs(table, ids[:-max_rows])
        num_dropped += r.json()['total_rows']
    if num_dropped:
        logging.info('Dropped {} old rows from {}'.format(num_dropped, table))


def main():
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)
    logging.info('STARTING')

    if CLEAR_TABLE_FIRST:
        if cartosql.tableExists(CARTO_MARKET_TABLE):
            cartosql.dropTable(CARTO_MARKET_TABLE)
        if cartosql.tableExists(CARTO_ALPS_TABLE):
            cartosql.dropTable(CARTO_ALPS_TABLE)

    # 1. Check if table exists and create table
    existing_markets = []
    if cartosql.tableExists(CARTO_MARKET_TABLE):
        logging.info('Fetching existing ids')
        existing_markets = getIds(CARTO_MARKET_TABLE, UID_FIELD)
    else:
        logging.info('Table {} does not exist, creating'.format(CARTO_MARKET_TABLE))
        createTableWithIndex(CARTO_MARKET_TABLE, CARTO_MARKET_SCHEMA, UID_FIELD)

    existing_alps = []
    if cartosql.tableExists(CARTO_ALPS_TABLE):
        logging.info('Fetching existing ids')
        existing_alps = getIds(CARTO_ALPS_TABLE, UID_FIELD)
    else:
        logging.info('Table {} does not exist, creating'.format(CARTO_ALPS_TABLE))
        createTableWithIndex(CARTO_ALPS_TABLE, CARTO_ALPS_SCHEMA, UID_FIELD, TIME_FIELD)

    # 2. Iterively fetch, parse and post new data
    num_new_markets, num_new_alps = processNewData(existing_markets, existing_alps)

    existing_markets = num_new_markets + len(existing_markets)
    logging.info('Total market rows: {}, New: {}, Max: {}'.format(
        existing_markets, num_new_markets, MAXROWS))

    existing_alps = num_new_alps + len(existing_alps)
    logging.info('Total alps rows: {}, New: {}, Max: {}'.format(
        existing_alps, num_new_alps, MAXROWS))

    # 3. Remove old observations
    deleteExcessRows(CARTO_ALPS_TABLE, MAXROWS, TIME_FIELD) # MAXAGE)

    lastUpdateDate(DATASET_ID, datetime.datetime.utcnow())
    
    logging.info('SUCCESS')
