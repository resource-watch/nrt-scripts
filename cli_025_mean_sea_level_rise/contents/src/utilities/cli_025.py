import pandas as pd
import urllib.request as req
import logging
from datetime import datetime, timedelta

def genUID(obs):
    return(obs)

def fetchDataFileName(SOURCE_URL):
    # View file names on the FTP
    df = pd.DataFrame(req.urlopen(SOURCE_URL).read().splitlines())
    df["files"] = df[0].str.split(expand=True)[8].astype(str)
    logging.info(df["files"])
    df["files"] = df["files"].apply(lambda row: row[2:-1])

    # Select the file that contains the data... i.e. ends with .txt, and has "V4" in the name
    data_file_index = df["files"].apply(lambda row: row.endswith(".txt") & ("V4" in row))
    logging.info(data_file_index)

    # Pull out just the file name
    remote_file_name = df.loc[data_file_index,"files"].values[0]
    logging.debug(remote_file_name)

    # Return the file name
    return(remote_file_name)

# https://stackoverflow.com/questions/20911015/decimal-years-to-datetime-in-python
def dec_to_datetime(dec):
    year = int(dec)
    rem = dec - year
    base = datetime(year, 1, 1)
    dt = base + timedelta(seconds=(base.replace(year=base.year + 1) - base).total_seconds() * rem)
    result = dt.strftime("%Y-%m-%d %H:%M:%S")
    return(result)
