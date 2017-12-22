import pandas as pd
import urllib.request as req
import logging
from datetime import datetime, timedelta

def fetchData():
    ftp = "ftp://podaac.jpl.nasa.gov/allData/merged_alt/L2/TP_J1_OSTM/global_mean_sea_level/"
    df = pd.DataFrame(req.urlopen(ftp).read().splitlines())
    df["files"] = df[0].str.split(expand=True)[8].astype(str)
    logging.info(df["files"])
    df["files"] = df["files"].apply(lambda row: row[2:-1])
    # Select the file that contains the data... i.e. ends with .txt, and has "V4" in the name
    data_file_index = df["files"].apply(lambda row: row.endswith(".txt") & ("V4" in row))
    logging.info(data_file_index)
    # Pull out just the file name
    remote_file_name = df.loc[data_file_index,"files"].values[0]
    logging.info(remote_file_name)

    # Use requests to read in the csv w/out pd
    sea_level = pd.read_csv(ftp+remote_file_name, header = None, sep = '\t')

    # Could do this in a parse step... don't add the row if it has HDR or 999 in it
    df = sea_level
    df = df[~df[0].astype(str).str.contains('HDR')]
    df = df[~df[0].astype(str).str.contains('999')]

    # Split df apart
    df = df[0].str.split(expand=True)

    return(df)

# https://stackoverflow.com/questions/20911015/decimal-years-to-datetime-in-python
def dec_to_datetime(dec):
    year = int(dec)
    rem = dec - year
    base = datetime(year, 1, 1)
    dt = base + timedelta(seconds=(base.replace(year=base.year + 1) - base).total_seconds() * rem)
    result = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return(result)
