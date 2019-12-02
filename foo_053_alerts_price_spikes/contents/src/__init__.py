import os
import logging
import sys
import requests
from collections import OrderedDict
import datetime
import cartosql
from functools import reduce
from shapely import wkb
import shapely
import numpy as np
import json
import hashlib

# Constants
LOG_LEVEL = logging.INFO

ALPS_URL = 'http://dataviz.vam.wfp.org/api/GetAlps?ac={country_code}'
MARKETS_URL = 'http://dataviz.vam.wfp.org/api/GetMarkets?ac={country_code}'

PROCESS_HISTORY_INTERACTIONS=False
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

CARTO_INTERACTION_TABLE = 'foo_053c_market_interaction'
CARTO_INTERACTION_SCHEMA = OrderedDict([
    ("uid", "text"),
    ("the_geom", "geometry"),
    ("region_name", "text"),
    ("region_id", "int"),
    ("market_name", "text"),
    ("market_id", "int"),
    ("category", "text"),
    ("market_interaction", "text"),
    ("highest_pewi", "numeric"),
    ("highest_alps", "text"),
    ("oldest_interaction_date", "timestamp"),

])
#(name of category in interaction table, sql query from source data)
# these are different because source data has typos
CATEGORIES = OrderedDict([
    ('cereals and tubers', 'cereals and tubers'),
    ('milk and dairy', 'milk and %'),
    ('oil and fats', 'oil and fats'),
    ('pulses and nuts','pulses and nuts'),
    ('vegetables and fruits', 'vegetables and fruits'),
    ('miscellaneous food', 'miscellaneous food')])
UID_FIELD = 'uid'
TIME_FIELD = 'date'
INTERACTION_TIME_FIELD = "oldest_interaction_date"
INTERACTION_STRING_FORMAT = "[{num}] {commodity} markets were at a '{alps}' level as of {date}"

#specify how many months back we want to display alerts for
LOOKBACK = 3

# Limit 1M rows, drop older than 10yrs
MAXROWS = 1000000
#MAXAGE = datetime.datetime.today() - datetime.timedelta(days=3650)
DATASET_ID = 'acf42a1b-104b-4f81-acd0-549f805873fb'


def lastUpdateDate(dataset, date):
    apiUrl = 'http://api.resourcewatch.org/v1/dataset/{0}'.format(dataset)
    headers = {
    'Content-Type': 'application/json',
    'Authorization': os.getenv('apiToken')
    }
    body = {
        "dataLastUpdated": date.isoformat()
    }
    try:
        r = requests.patch(url = apiUrl, json = body, headers = headers)
        logging.info('[lastUpdated]: SUCCESS, '+ date.isoformat() +' status code '+str(r.status_code))
        return 0
    except Exception as e:
        logging.error('[lastUpdated]: '+str(e))


def genAlpsUID(sn, date, forecast):
    '''Generate unique id'''
    return '{}_{}_{}'.format(sn, date, forecast)

def genMarketUID(rid, mid, mname):
    '''Generate unique id'''
    id_str = '{}_{}_{}'.format(rid, mid, mname)
    return hashlib.md5(id_str.encode('utf8')).hexdigest()

def genInteractionUID(rid, mid, mname, food_category):
    '''Generate unique id'''
    id_str = '{}_{}_{}_{}'.format(rid, mid, mname, food_category)
    return hashlib.md5(id_str.encode('utf8')).hexdigest()

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
    return (start + datetime.timedelta(**TIME_STEP)).replace(day=15)

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
# for a particular market, get any dates of data that are new. Each date is a new row.
def parseAlps(market_data, existing_alps):
    # Happens w/ 'National Average' entries
    if 'admname' not in market_data:
        logging.debug('Unfamiliar structure, probably National Average entry')
        #logging.debug(market_data)
        return [[None]*len(CARTO_ALPS_SCHEMA)]

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

    date = datetime.datetime.strptime(market_data['startdate'], DATE_FORMAT)
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
    markets_updated = []
    # get and parse each page; stop when no new results or 200 pages
    while futile_tries < TOLERATE_TRIES:
        # 1. Fetch new data
        logging.info("Fetching country code {}".format(country_code))
        try_num=0
        try:
            markets = requests.get(MARKETS_URL.format(country_code=country_code)).json()
            alps = requests.get(ALPS_URL.format(country_code=country_code)).json()
        except Exception as e:
            if try_num < 2:
                try_num+=1
            else:
                logging.error(e)

        if len(markets) == len(alps) == 0:
            futile_tries += 1
            country_code += 1
            continue
        else:
            futile_tries = 0
            country_code += 1

        # 2. Parse data excluding existing observations
        new_markets = [parseMarkets(mkt, existing_markets) for mkt in markets]

        # returns a 3D list, 1st dimension represents a particular market
        # 2nd dimension represents the time steps that are new
        # 3rd dimention represents the columns of the Carto table for that market and time step
        new_alps = [parseAlps(alp, existing_alps) for alp in alps]

        logging.debug('Country {} Data: After map:'.format(country_code))
        logging.debug(new_markets)
        logging.debug(new_alps)

        new_markets = reduce(flatten, new_markets , [])
        #removes market dimension of array
        #now each element of the array is just a row to be inserted into Carto table
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

        #Check which market ids were updated so that we can update their interactions
        #get only markets that have been updated
        #get market id index
        if len(new_alps)>0:
            for entry in new_alps:
                uid=genMarketUID(entry[list(CARTO_ALPS_SCHEMA.keys()).index("adm1id")],
                                 entry[list(CARTO_ALPS_SCHEMA.keys()).index("mktid")],
                                 entry[list(CARTO_ALPS_SCHEMA.keys()).index("mktname")])
                markets_updated.append(uid)
            markets_updated = np.unique(markets_updated).tolist()
        logging.debug('Country {} Data: After filter:'.format(country_code))
        logging.debug(new_markets)
        logging.debug(new_alps)

        num_new_markets += len(new_markets)
        num_new_alps += len(new_alps)

        # 3. Insert new rows

        if num_new_markets:
            logging.info('Pushing {} new Markets rows'.format(num_new_markets))
            cartosql.insertRows(CARTO_MARKET_TABLE, CARTO_MARKET_SCHEMA.keys(),
                                CARTO_MARKET_SCHEMA.values(), new_markets,
                                user=os.getenv('CARTO_USER'), key =os.getenv('CARTO_KEY'))
        if num_new_alps:
            logging.info('Pushing {} new ALPS rows'.format(num_new_alps))
            cartosql.insertRows(CARTO_ALPS_TABLE, CARTO_ALPS_SCHEMA.keys(),
                                CARTO_ALPS_SCHEMA.values(), new_alps,
                                user=os.getenv('CARTO_USER'), key =os.getenv('CARTO_KEY'))


    return num_new_markets, num_new_alps, markets_updated

def processInteractions(markets_updated):
    num_new_interactions = 0
    #new_rows = []
    if PROCESS_HISTORY_INTERACTIONS==True:
    # get all markets
        logging.info('Processing interactions for all ALPS data')
        markets_to_process = getIds(CARTO_MARKET_TABLE, 'uid')

    else:
        logging.info('Getting IDs of interactions that should be updated')
        r = cartosql.getFields('uid', CARTO_INTERACTION_TABLE, where="{} < current_date - interval '{}' month".format(INTERACTION_TIME_FIELD, LOOKBACK),
                               f='csv', user=os.getenv('CARTO_USER'), key=os.getenv('CARTO_KEY'))
        old_ids = r.text.split('\r\n')[1:-1]

        logging.info('Processing interactions for new ALPS data and re-processing interactions that are out of date')
        markets_to_process = markets_updated + old_ids
    #go through each market that was updated and create the correct rows for them
    num_markets = len(markets_to_process)
    market_num = 1
    for m_uid in markets_to_process:
        new_rows = []
        for food_category, sql_query in CATEGORIES.items():
            try_num=1
            while try_num <=3:
                try:
                    #logging.info('Processing interaction for {} at uid {}, try number {} (market {} of {})'.format(food_category, m_uid, try_num, market_num, num_markets))
                    # get information about market
                    r = cartosql.get("SELECT * FROM {} WHERE uid='{}'".format(CARTO_MARKET_TABLE, m_uid),
                                     user=os.getenv('CARTO_USER'), key=os.getenv('CARTO_KEY'))
                    if r.json()['total_rows']==0:
                        #logging.info('No rows for interaction')
                        break
                    market_entry = r.json()['rows'][0]

                    # get information about food prices at market
                    # SQL gets most recent entry for each commodity at each market that is NOT a forecast
                    request = "SELECT DISTINCT ON (mktid, cmname) * FROM {table} WHERE mktid={market_id} AND mktname='{market_name}' AND adm1id={region_id} AND category LIKE '{cat_name}' AND date > current_date - interval '{x}' month AND forecast = 'False' ORDER  BY mktid, cmname, date desc".format(
                        table=CARTO_ALPS_TABLE, market_id=market_entry['market_id'],
                        market_name=market_entry['market_name'], region_id=market_entry['region_id'],
                        cat_name=sql_query, x=LOOKBACK)
                    r = cartosql.get(request, user=os.getenv('CARTO_USER'), key=os.getenv('CARTO_KEY'))
                    alps_entries = r.json()['rows']
                    break
                except:
                    try_num += 1
            uid = genInteractionUID(market_entry['region_id'], market_entry['market_id'], market_entry['market_name'], food_category)
            commodity_num=1
            for entry in alps_entries:
                if commodity_num==1:
                    interaction_string = INTERACTION_STRING_FORMAT.format(num=commodity_num, commodity=entry['cmname'], alps=entry['alps'].lower(), date=entry['date'][:10])
                else:
                    interaction_string = interaction_string + '; ' + INTERACTION_STRING_FORMAT.format(num=commodity_num, commodity=entry['cmname'], alps=entry['alps'].lower(), date=entry['date'][:10])
                commodity_num+=1
            # create new Carto row
            row = []
            for field in CARTO_INTERACTION_SCHEMA.keys():
                if field == 'uid':
                    row.append(uid)
                elif field == 'market_id':
                    row.append(int(market_entry['market_id']))
                elif field == 'the_geom':
                    shapely_point = wkb.loads(market_entry['the_geom'], hex=True)
                    json_point = json.loads(json.dumps(shapely.geometry.mapping(shapely_point)))
                    row.append(json_point)
                elif field == 'region_name':
                    row.append(market_entry['region_name'])
                elif field == 'region_id':
                    row.append(market_entry['region_id'])
                elif field == 'market_name':
                    row.append(market_entry['market_name'])
                elif field == 'market_interaction':
                    if len(alps_entries) == 0:
                        row.append(None)
                    else:
                        row.append(interaction_string)
                elif field == 'category':
                    row.append(food_category)
                elif field == 'highest_pewi':
                    if len(alps_entries) == 0:
                        row.append(None)
                    else:
                        highest_pewi = max([entry['pewi'] for entry in alps_entries])
                        row.append(highest_pewi)
                elif field == 'highest_alps':
                    if len(alps_entries) == 0:
                        row.append(None)
                    else:
                        highest_alps_category = assignALPS(highest_pewi)
                        row.append(highest_alps_category)
                elif field == INTERACTION_TIME_FIELD:
                    if len(alps_entries) == 0:
                        row.append(None)
                    else:
                        row.append(min(entry['date'] for entry in alps_entries))
            new_rows.append(row)
            num_new_interactions+=1
        #delete old entries for the markets that were updated
        #logging.info('Deleting old interactions from Carto')
        try:
            cartosql.deleteRowsByIDs(CARTO_INTERACTION_TABLE, uid, id_field=UID_FIELD,
                            user=os.getenv('CARTO_USER'), key=os.getenv('CARTO_KEY'))
        except:
            pass
        #cartosql.deleteRowsByIDs(CARTO_INTERACTION_TABLE, markets_to_process, id_field='market_id')
        #send new rows for these markets
        #logging.info('Sending new interactions to Carto')
        cartosql.insertRows(CARTO_INTERACTION_TABLE, CARTO_INTERACTION_SCHEMA.keys(),
                            CARTO_INTERACTION_SCHEMA.values(), new_rows, blocksize=500, user=os.getenv('CARTO_USER'), key=os.getenv('CARTO_KEY'))
        market_num+=1
    return num_new_interactions

##############################################################
# General logic for Carto
# should be the same for most tabular datasets
##############################################################

def createTableWithIndex(table, schema, id_field, time_field=''):
    '''Get existing ids or create table'''
    cartosql.createTable(table, schema, user=os.getenv('CARTO_USER'), key =os.getenv('CARTO_KEY'))
    cartosql.createIndex(table, id_field, unique=True, user=os.getenv('CARTO_USER'), key =os.getenv('CARTO_KEY'))
    if time_field:
        cartosql.createIndex(table, time_field, user=os.getenv('CARTO_USER'), key =os.getenv('CARTO_KEY'))


def getIds(table, id_field):
    '''get ids from table'''
    r = cartosql.getFields(id_field, table, f='csv', user=os.getenv('CARTO_USER'), key =os.getenv('CARTO_KEY'))
    return r.text.split('\r\n')[1:-1]


def deleteExcessRows(table, max_rows, time_field, max_age=''):
    '''Delete excess rows by age or count'''
    num_dropped = 0
    if isinstance(max_age, datetime.datetime):
        max_age = max_age.isoformat()

    # 1. delete by age
    if max_age:
        r = cartosql.deleteRows(table, "{} < '{}'".format(time_field, max_age), user=os.getenv('CARTO_USER'), key =os.getenv('CARTO_KEY'))
        num_dropped = r.json()['total_rows']

    # 2. get sorted ids (old->new)
    r = cartosql.getFields('cartodb_id', table, order='{}'.format(time_field),
                           f='csv', user=os.getenv('CARTO_USER'), key =os.getenv('CARTO_KEY'))
    ids = r.text.split('\r\n')[1:-1]

    # 3. delete excess
    if len(ids) > max_rows:
        r = cartosql.deleteRowsByIDs(table, ids[:-max_rows], user=os.getenv('CARTO_USER'), key =os.getenv('CARTO_KEY'))
        num_dropped += r.json()['total_rows']
    if num_dropped:
        logging.info('Dropped {} old rows from {}'.format(num_dropped, table))

def get_most_recent_date(table):
    r = cartosql.getFields(TIME_FIELD, table, where="forecast = 'False'", f='csv', post=True)
    dates = r.text.split('\r\n')[1:-1]
    dates.sort()
    most_recent_date = datetime.datetime.strptime(dates[-1], '%Y-%m-%d %H:%M:%S')
    return most_recent_date

def main():
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)
    logging.info('STARTING')

    if CLEAR_TABLE_FIRST:
        if cartosql.tableExists(CARTO_MARKET_TABLE):
            cartosql.deleteRows(CARTO_MARKET_TABLE, 'cartodb_id IS NOT NULL', user=os.getenv('CARTO_USER'), key=os.getenv('CARTO_KEY'))
        if cartosql.tableExists(CARTO_ALPS_TABLE):
            cartosql.deleteRows(CARTO_ALPS_TABLE, 'cartodb_id IS NOT NULL', user=os.getenv('CARTO_USER'), key=os.getenv('CARTO_KEY'))
        if cartosql.tableExists(CARTO_INTERACTION_TABLE):
            cartosql.deleteRows(CARTO_INTERACTION_TABLE, 'cartodb_id IS NOT NULL', user=os.getenv('CARTO_USER'), key=os.getenv('CARTO_KEY'))

    # 1. Check if table exists and create table
    existing_markets = []
    if cartosql.tableExists(CARTO_MARKET_TABLE, user=os.getenv('CARTO_USER'), key =os.getenv('CARTO_KEY')):
        logging.info('Fetching existing ids')
        existing_markets = getIds(CARTO_MARKET_TABLE, UID_FIELD)
    else:
        logging.info('Table {} does not exist, creating'.format(CARTO_MARKET_TABLE))
        createTableWithIndex(CARTO_MARKET_TABLE, CARTO_MARKET_SCHEMA, UID_FIELD)

    existing_alps = []
    if cartosql.tableExists(CARTO_ALPS_TABLE, user=os.getenv('CARTO_USER'), key =os.getenv('CARTO_KEY')):
        logging.info('Fetching existing ids')
        existing_alps = getIds(CARTO_ALPS_TABLE, UID_FIELD)
    else:
        logging.info('Table {} does not exist, creating'.format(CARTO_ALPS_TABLE))
        createTableWithIndex(CARTO_ALPS_TABLE, CARTO_ALPS_SCHEMA, UID_FIELD, TIME_FIELD)

    existing_interactions = []
    if cartosql.tableExists(CARTO_INTERACTION_TABLE, user=os.getenv('CARTO_USER'), key =os.getenv('CARTO_KEY')):
        logging.info('Fetching existing interaction ids')
        existing_interactions = getIds(CARTO_INTERACTION_TABLE, UID_FIELD)
    else:
        logging.info('Table {} does not exist, creating'.format(CARTO_INTERACTION_TABLE))
        createTableWithIndex(CARTO_INTERACTION_TABLE, CARTO_INTERACTION_SCHEMA, UID_FIELD, INTERACTION_TIME_FIELD)

    # 2. Iterively fetch, parse and post new data
    num_new_markets, num_new_alps, markets_updated = processNewData(existing_markets, existing_alps)

    # Update Interaction table
    num_new_interactions = processInteractions(markets_updated)

    # Report new data count
    existing_markets = num_new_markets + len(existing_markets)
    logging.info('Total market rows: {}, New: {}, Max: {}'.format(
        existing_markets, num_new_markets, MAXROWS))

    existing_alps = num_new_alps + len(existing_alps)
    logging.info('Total alps rows: {}, New: {}, Max: {}'.format(
        existing_alps, num_new_alps, MAXROWS))

    existing_interactions = num_new_interactions + len(existing_interactions)
    logging.info('Total interaction rows: {}, New: {}, Max: {}'.format(
        existing_interactions, num_new_interactions, MAXROWS))

    # 3. Remove old observations
    deleteExcessRows(CARTO_ALPS_TABLE, MAXROWS, TIME_FIELD) # MAXAGE)

    # Get most recent update date
    most_recent_date = get_most_recent_date(CARTO_ALPS_TABLE)
    lastUpdateDate(DATASET_ID, most_recent_date)

    logging.info('SUCCESS')
