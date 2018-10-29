import os
import logging
import sys
import requests
from collections import OrderedDict
from datetime import datetime, timedelta
import cartosql
from bs4 import BeautifulSoup
import requests


### Constants
SOURCE_URL = "http://www.worldwater.org/conflict/php/table-data-scraping.php?jstr={%22region%22:%22%%22,%22conftype%22:%22%%22,%22epoch%22:%22-5000,2030%22,%22search%22:%22%22}"
CLEAR_TABLE_FIRST = True
#INPUT_DATE_FORMAT = '%B %d, %Y'
#DATE_FORMAT = '%Y-%m-%d'
LOG_LEVEL = logging.INFO

### Table name and structure
CARTO_TABLE = 'soc_029_water_conflict'
CARTO_SCHEMA = OrderedDict([
        ("the_geom", "geometry"),
        ('conflict_type', 'text'),
        ('date', 'text'),
        ('description','text'),
        ('end', 'numeric')
        ('headline', 'text'),
        ('region', 'text'),
        ('sources', 'text'),
        ('latitude' , 'numeric'),
        ('longitude', 'numeric'),
        ('region', 'text'),
        ('sources', 'text'),
        ('start', 'numeric')

])
#Note 'Region' column name was changed on 10/09/18 from 'Region_of_Interest'
UID_FIELD = 'headline'
TIME_FIELD = 'start'

# Table limits
MAX_ROWS = 500000
MAX_YEARS = 10
MAX_AGE = datetime.datetime.today() - datetime.timedelta(days=365*MAX_YEARS)


###
## Accessing remote data
###

#def formatDate(date):
#    """ Parse input date string and write in output date format """
#    return datetime.datetime.strptime(date, INPUT_DATE_FORMAT)\
#                            .strftime(DATE_FORMAT)

def processData(existing_ids):
    """
    """
    num_new = 1
    #year = start
    new_ids = []
    #while year > MAX_AGE.year and num_new:
    logging.info("Fetching data")
    res = requests.get(SOURCE_URL)
    data = res.text
    soup = BeautifulSoup(data, 'html5lib')
    tableconf = soup.find( "table", {"id":"conflict"} )
    rows = tableconf.find_all('tr')[1:]
    data = {
        'date' : [],
        'headline' : [],
        'conflict_type' : [],
        'region' : [],
        'description' : [],
        'sources' : [],
        'latitude' : [],
        'longitude' : [],
        'start' : [],
        'end' : []
        }

    for row in rows:
        cols = row.find_all('td')
        data['date'].append( cols[0].get_text() )
        data['headline'].append( cols[1].get_text() )
        data['conflict_type'].append( cols[2].get_text() )
        data['region'].append( cols[3].get_text() )
        data['description'].append( cols[4].get_text() )
        data['sources'].append( cols[5].get_text() )
        data['latitude'].append( cols[6].get_text() )
        data['longitude'].append( cols[7].get_text() )
        data['start'].append( cols[8].get_text() )
        data['end'].append( cols[9].get_text() )
    table = pd.DataFrame( data )
    table['start'] =  pd.to_numeric(table'start'], errors='coerce')

###
## Carto code
###

def checkCreateTable(table, schema, id_field, time_field):
    '''Get existing ids or create table'''
    if cartosql.tableExists(table):
        logging.info('Fetching existing IDs')
        r = cartosql.getFields(id_field, table, f='csv')
        return r.text.split('\r\n')[1:-1]
    else:
        logging.info('Table {} does not exist, creating'.format(table))
        cartosql.createTable(table, schema)
        cartosql.createIndex(table, id_field, unique=True)
        cartosql.createIndex(table, time_field)
    return []


#def deleteExcessRows(table, max_rows, time_field, max_age=''):
    #'''Delete excess rows by age or count'''
    #num_dropped = 0
    #if isinstance(max_age, datetime.datetime):
    #    max_age = max_age.isoformat()

    # 1. delete by age
    #if max_age:
    #    r = cartosql.deleteRows(table, "{} < '{}'".format(time_field, max_age))
    #    num_dropped = r.json()['total_rows']

    # 2. get sorted ids (old->new)
    #r = cartosql.getFields('cartodb_id', table, order='{}'.format(time_field),
    #                       f='csv')
    #ids = r.text.split('\r\n')[1:-1]

    # 3. delete excess
    #if len(ids) > max_rows:
    #    r = cartosql.deleteRowsByIDs(table, ids[:-max_rows])
    #    num_dropped += r.json()['total_rows']
    #if num_dropped:
    #    logging.info('Dropped {} old rows from {}'.format(num_dropped, table))


def main():
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)
    logging.info('STARTING')

    # 1. Check if table exists and create table
    existing_ids = checkCreateTable(CARTO_TABLE, CARTO_SCHEMA, UID_FIELD,
                                    TIME_FIELD)

    # 2. Iterively fetch, parse and post new data
    new_ids = processData(existing_ids)

    #new_count = len(new_ids)
    #existing_count = new_count + len(existing_ids)
    #logging.info('Total rows: {}, New: {}, Max: {}'.format(
    #    existing_count, new_count, MAX_ROWS))

    # 3. Remove old observations
    #deleteExcessRows(CARTO_TABLE, MAX_ROWS, TIME_FIELD, MAX_AGE)

    logging.info('SUCCESS')
