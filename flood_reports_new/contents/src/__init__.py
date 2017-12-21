import fiona
import os
import logging
import sys
import urllib
from collections import OrderedDict
import src.carto

### Constants
DATA_DIR = 'data'
SOURCE_URLS = {
    'f.dat':'http://floodobservatory.colorado.edu/Version3/FloodArchive.DAT',
    'f.id':'http://floodobservatory.colorado.edu/Version3/FloodArchive.ID',
    'f.map':'http://floodobservatory.colorado.edu/Version3/FloodArchive.MAP',
    'f.ind':'http://floodobservatory.colorado.edu/Version3/FloodArchive.IND',
    'f.tab':'http://floodobservatory.colorado.edu/Version3/FloodArchive.TAB',
}
TABFILE='f.tab'
ENCODING='latin-1'

### asserting table structure rather than reading from input
CARTO_TABLE = 'test_floodreports'
CARTO_SCHEMA = OrderedDict([
    ('the_geom', 'geometry'),
    ('_UID', 'text'),
    ('ID', 'int'),
    ('GlideNumber', 'text'),
    ('Country', 'text'),
    ('OtherCountry', 'text'),
    ('long', 'numeric'),
    ('lat', 'numeric'),
    ('Area', 'numeric'),
    ('Began', 'timestamp'),
    ('Ended', 'timestamp'),
    ('Validation', 'text'),
    ('Dead', 'int'),
    ('Displaced', 'int'),
    ('MainCause', 'text'),
    ('Severity', 'numeric')
])
UID_FIELD = '_UID'
TIME_FIELD = 'Began'

CARTO_USER = os.environ.get('CARTO_USER')
CARTO_KEY = os.environ.get('CARTO_KEY')

MAXROWS = 1000000
OVERWRITE = False

### Generate UID
def genUID(obs):
    return obs['properties']['ID']

### Reads flood shp and returnse list of insertable rows
def parseFloods(filepath, encoding, fields, exclude_ids):
    rows = []
    with fiona.open(filepath, 'r', encoding=encoding) as shp:
        logging.info(shp.schema)
        for obs in shp:
            uid = genUID(obs)
            # Only add new observations unless overwrite
            if str(uid) not in exclude_ids:
                row = []
                for field in fields:
                    if field == 'the_geom':
                        row.append(obs['geometry'])
                    elif field == UID_FIELD:
                        row.append(uid)
                    else:
                        row.append(obs['properties'][field])
                rows.append(row)
    return rows

### Main
def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)

    ### 1. Check if table exists and create table
    dest_ids = []
    if not carto.tableExists(CARTO_TABLE):
        logging.info('Table {} does not exist'.format(CARTO_TABLE))
        carto.createTable(CARTO_TABLE, CARTO_SCHEMA)

    ### 2. Fetch existing IDs from table
    else:
        r = carto.getFields(UID_FIELD, CARTO_TABLE, order=TIME_FIELD, f='csv')
        # quick read 1-column csv to list
        dest_ids = r.split('\r\n')[1:-1]

    ### 3. Fetch data from source
    for dest, url in SOURCE_URLS.items():
        urllib.request.urlretrieve(url, os.path.join(DATA_DIR, dest))

    ### 4. Parse fetched data and generate unique ids
    rows = parseFloods(os.path.join(DATA_DIR, TABFILE), ENCODING, CARTO_SCHEMA.keys(), dest_ids)

    ### 5. Insert new observations
    if len(rows):
        carto.blockInsertRows(CARTO_TABLE, CARTO_SCHEMA, rows)

    ### 6. Remove old observations
    logging.info('Row count: {}, New: {}, Max: {}'.format(len(dest_ids), len(rows), MAXROWS))
    if len(dest_ids) + len(rows) > MAXROWS and MAXROWS > len(rows):
        drop_ids = dest_ids[(MAXROWS - len(rows)):]
        carto.deleteRowsByIDs(CARTO_TABLE, "_UID", drop_ids)
