import urllib.request
import logging
from datetime import datetime, timedelta

def genUID(obs):
    return(obs)

def fetchDataFileName(SOURCE_URL):
    # View file names on the FTP
    with urllib.request.urlopen(SOURCE_URL) as f:
        ftp_contents = f.read().decode('utf-8').splitlines()

    filename = ""
    ALREADY_FOUND=False
    for fileline in ftp_contents:
        fileline = fileline.split()
        potential_filename = fileline[8]
        if (potential_filename.endswith(".txt") and ("V4" in potential_filename)):
            if not ALREADY_FOUND:
                filename = potential_filename
                ALREADY_FOUND=True
            else:
                logging.warning("There are multiple filenames which match criteria, passing most recent")
                filename = potential_filename

    logging.info("Selected filename: " + filename)
    if not ALREADY_FOUND:
        logging.warning("No valid filename found")

    # Return the file name
    return(filename)

# https://stackoverflow.com/questions/20911015/decimal-years-to-datetime-in-python
def dec_to_datetime(dec):
    year = int(dec)
    rem = dec - year
    base = datetime(year, 1, 1)
    dt = base + timedelta(seconds=(base.replace(year=base.year + 1) - base).total_seconds() * rem)
    result = dt.strftime("%Y-%m-%d %H:%M:%S")
    return(result)
