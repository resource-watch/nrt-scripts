import os
import requests
import pandas as pd
import datetime
import logging
import sys

logging.basicConfig(stream=sys.stderr, level=logging.INFO)

def main():
    # Get ‘Frequency of Updates’ column to determine how frequently we expect each dataset to update
    # get 'Update Strategy' column to determine which datasets are NRT
    sheet = requests.get(os.getenv('METADATA_SHEET'))
    df = pd.read_csv(pd.compat.StringIO(sheet.text), header=0,
                              usecols=['WRI_ID', 'API_ID', 'Public Title', 'Frequency of Updates', 'Update strategy']).dropna()
    # get data sets marked as RT
    full_nrt_df = df[df['Update strategy'].str.startswith('RT')]
    # get rid of Archived data sets
    full_nrt_df = full_nrt_df[~full_nrt_df['WRI_ID'].str.contains('Archived')]
    # get rid of data sets I can't do anything to fix, such as datasets updated by GEE
    nrt_df = full_nrt_df[~full_nrt_df['Update strategy'].str.contains('RT - GEE')]

    # pull in sheet with out-of-date datasets that have been investigated
    known_errors_csv = 'https://raw.githubusercontent.com/resource-watch/nrt-scripts/master/check_if_nrt_datasets_current/outdated_nrt_scripts.csv'
    sheet = requests.get(known_errors_csv)
    error_tracking_df = pd.read_csv(pd.compat.StringIO(sheet.text), header=0)

    # go through each NRT dataset and check if it is up-to-date
    for idx, dataset in nrt_df.iterrows():
        # get data from API for data set
        api_call = requests.get('https://api.resourcewatch.org/v1/dataset/{}'.format(dataset['API_ID']))
        r = api_call.json()
        # check when data set was last updated
        last_updated = datetime.datetime.strptime(r['data']['attributes']['dataLastUpdated'],
                                                  '%Y-%m-%dT%H:%M:%S.%fZ')
        # see how long it has been since its last update
        today = datetime.datetime.utcnow()
        time_since_update = today - last_updated
        # check how frequently we expect this data set to update
        expected_freq = dataset['Frequency of Updates']

        '''set a threshold of how many days we want to allow to pass before we receive an error, based on its 
        # expected frequency of updates'''
        if expected_freq.lower().strip() == 'daily':
            allowed_time = datetime.timedelta(days=4)
        elif expected_freq.lower().strip() == 'weekly':
            allowed_time = datetime.timedelta(days=10)
        elif expected_freq.lower().strip() == 'monthly':
            allowed_time = datetime.timedelta(days=45)
        elif expected_freq.lower().strip() == 'annual':
            allowed_time = datetime.timedelta(days=410)
        elif expected_freq.lower().strip() == 'varies':
            allowed_time = datetime.timedelta(days=10)
        # if frequency on the order of days, we will let the update be 3 days overdue
        elif 'days' in expected_freq.lower():
            x = int(expected_freq.lower()[0:-5])
            allowed_time = datetime.timedelta(days=x + 3)
        # if on the order of weeks, we will let the update be 3 days overdue
        elif 'weeks' in expected_freq.lower():
            x = int(expected_freq.lower()[0:-6])
            allowed_time = datetime.timedelta(days=x * 7 + 3)
        # if on the order of months, we will let the update be 30 days overdue
        elif 'months' in expected_freq.lower():
            x = int(expected_freq.lower()[0:-7])
            allowed_time = datetime.timedelta(days=x * 30 + 30)
        # for any other scenario, we will let the update be 1 day overdue
        else:
            allowed_time = datetime.timedelta(days=1)

        '''allow for exceptions to the rules when certain datasets update on a lag'''
        # biodiversity hotspots
        if 'bio.002' in r['data']['attributes']['name']:
            allowed_time = datetime.timedelta(days=30)
         # chlorophyll updates on the 19th of each month with the previous month's data
        if 'bio.037' in r['data']['attributes']['name']:
            allowed_time = datetime.timedelta(days=53)
        # allow for longer delay for TROPOMI data because it is slow to upload
        if 'cit.035' in r['data']['attributes']['name']:
            allowed_time = datetime.timedelta(days=45)
        # on the 1st of each month, Arctic/Antarctic Sea Ice Extent updates for the 1st of the previous month
        elif 'cli.005a' in r['data']['attributes']['name'] or 'cli.005b' in r['data']['attributes']['name']:
            allowed_time = datetime.timedelta(days=70)
        # within the first few days of each month, Snow Cover updates for the 1st of the previous month
        elif 'cli.021' in r['data']['attributes']['name']:
            allowed_time = datetime.timedelta(days=70)
        # the WACCM forecast often goes offline for a few days
        elif 'cit.038' in r['data']['attributes']['name']:
            allowed_time = datetime.timedelta(days=5)
        # around the 15th of each month, Surface Temperature Change updates for the 15th of the PREVIOUS month
        elif 'cli.035' in r['data']['attributes']['name']:
            allowed_time = datetime.timedelta(days=70)
        # SPEI
        elif 'cli.039' in r['data']['attributes']['name']:
            allowed_time = datetime.timedelta(days=60)
        # sea level rise data set updates at ~3 month delay
        elif 'cli.040' in r['data']['attributes']['name']:
            allowed_time = datetime.timedelta(days=120)
        # antarctic ice mass data set updates at 2-3 month delay
        elif 'cli.041' in r['data']['attributes']['name']:
            allowed_time = datetime.timedelta(days=120)
        # greenland ice mass data set updates at 2-3 month delay
        elif 'cli.042' in r['data']['attributes']['name']:
            allowed_time = datetime.timedelta(days=120)
        # carbon dioxide concentration data set updates at 2-3 month delay
        elif 'cli.045' in r['data']['attributes']['name']:
            allowed_time = datetime.timedelta(days=120)            
        # NDC ratification status probably would only update once a year, after COP
        elif 'cli.047' in r['data']['attributes']['name']:
            allowed_time = datetime.timedelta(days=400)
        # oil spills data set doesn't always have events that occur every 10 days
        elif 'ene.008' in r['data']['attributes']['name']:
            allowed_time = datetime.timedelta(days=20)
        # Vegetation Health Index
        elif 'foo.024' in r['data']['attributes']['name']:
            allowed_time = datetime.timedelta(days=12)
        # Vegetation Condition Index
        elif 'foo.051' in r['data']['attributes']['name']:
            allowed_time = datetime.timedelta(days=12)
        # Fire Risk Index often goes offline for a few days
        elif 'for.012' in r['data']['attributes']['name']:
            allowed_time = datetime.timedelta(days=10)
        # flood data set doesn't always have events that occur every 10 days
        elif 'wat.040' in r['data']['attributes']['name']:
            allowed_time = datetime.timedelta(days=30)

        '''check if the time since last update surpasses the time we allow for this type of data set'''
        # if the dataset is out-of-date
        if allowed_time < time_since_update:
            # if this outdated dataset has already been added to the sheet of outdated datasets:
            if dataset['WRI_ID'] in error_tracking_df['WRI ID'].tolist():
                # if no explanation has been added to the sheet, log an error
                if pd.isna(error_tracking_df[error_tracking_df['WRI ID'] == dataset['WRI_ID']][
                               'Known Reason?'].tolist()[0]):
                    logging.error(
                        '(OUTDATED) {wri_id} {public_title} - expected update frequency: {exp_up}. It has been {days} days since the last update.'.format(
                            wri_id=dataset['WRI_ID'], public_title=dataset['Public Title'],
                            exp_up=dataset['Frequency of Updates'].lower(),
                            days=time_since_update.days))
                # if the explanation is included, log info instead of error
                else:
                    logging.info(
                        '(OUTDATED) {wri_id} {public_title} - expected update frequency: {exp_up}. It has been {days} days since the last update.'.format(
                            wri_id=dataset['WRI_ID'], public_title=dataset['Public Title'],
                            exp_up=dataset['Frequency of Updates'].lower(),
                            days=time_since_update.days))
            # if the dataset has not been added to the sheet of outdated datasets:
            else:
                logging.error(
                    '(OUTDATED) {wri_id} {public_title} - expected update frequency: {exp_up}. It has been {days} days since the last update.'.format(
                        wri_id=dataset['WRI_ID'], public_title=dataset['Public Title'],
                        exp_up=dataset['Frequency of Updates'].lower(),
                        days=time_since_update.days))
        # if the dataset is still up-to-date use logging.info to note that
        else:
            logging.info('(OK) {wri_id} {public_title} up to date.'.format(wri_id=dataset['WRI_ID'],
                                                                           public_title=dataset['Public Title']))
            # if that data set was previously outdated, send an alert to go remove it from the sheet
            if dataset['WRI_ID'] in error_tracking_df['WRI ID'].tolist():
                logging.error('{wri_id} {public_title} is now up to date.'.format(wri_id=dataset['WRI_ID'],
                                                                                  public_title=dataset[
                                                                                      'Public Title']))

    # prepare weekly reminder to check any data sets that have been investigated but are still outdated
    for idx, dataset in error_tracking_df.iterrows():
        # find when the dataset was last checked
        last_check = dataset['Last Check']
        last_check_dt = datetime.datetime.strptime(last_check, '%m/%d/%Y')
        # see how many days it has been since we have checked
        today = datetime.datetime.utcnow()
        time_since_checking = today - last_check_dt
        days = time_since_checking.days
        # if it has been more than a week, log an error
        if days > 7:
            logging.error('The status of {wri_id} has not been checked in {days} days.'.format(wri_id=dataset['WRI ID'],
                                                                                               days=days))
