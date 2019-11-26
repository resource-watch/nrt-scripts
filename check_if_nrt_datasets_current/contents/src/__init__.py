import os
import requests
import pandas as pd
import datetime
import logging
import sys

logging.basicConfig(stream=sys.stderr, level=logging.INFO)

def main():
    # get 'Update Strategy' from master metadata sheet
    sheet = requests.get(os.getenv('MASTER_SHEET'))
    master_df = pd.read_csv(pd.compat.StringIO(sheet.text), header=0, usecols=['WRI_ID', 'Update strategy', 'API_ID'])

    # Get ‘Frequency of Updates’ from Launch metadata
    sheet = requests.get(os.getenv('METADATA_SHEET'))
    metadata_df = pd.read_csv(pd.compat.StringIO(sheet.text), header=0,
                              usecols=['WRI ID', 'Public Title', 'Frequency of Updates'])

    # merge two data frames, based on their WRI ID
    df = metadata_df.merge(master_df, how='inner', left_on='WRI ID', right_on='WRI_ID').drop(['WRI_ID'],
                                                                                             axis=1).dropna()
    # get data sets marked as RT
    full_nrt_df = df[df['Update strategy'].str.startswith('RT')]
    # get rid of Archived data sets
    full_nrt_df = full_nrt_df[~full_nrt_df['WRI ID'].str.contains('Archived')]
    # get rid of data sets I can't do anything to fix
    nrt_df = full_nrt_df[~full_nrt_df['Update strategy'].str.contains('RT - GEE')]
    nrt_df = nrt_df[~(nrt_df['Update strategy'] == 'RT')]

    # pull in sheet with current errors
    sheet = requests.get(os.getenv('ERROR_TRACKING_SHEET'))
    error_tracking_df = pd.read_csv(pd.compat.StringIO(sheet.text), header=0)

    # get rid of
    #
    for idx, dataset in nrt_df.iterrows():
        # get data from API for data set
        api_call = requests.get('https://api.resourcewatch.org/v1/dataset/{}'.format(dataset['API_ID']))
        r = api_call.json()
        if r['data']['attributes']['published'] == True:
            # check when data set was last
            last_updated = datetime.datetime.strptime(r['data']['attributes']['dataLastUpdated'],
                                                      '%Y-%m-%dT%H:%M:%S.%fZ')
            # see how long it has been since its last update
            today = datetime.datetime.utcnow()
            time_since_update = today - last_updated
            # check how frequently we expect this data set to update
            expected_freq = dataset['Frequency of Updates']
            # set a threshold of how many days we want to allow to pass before we receive an error
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
            # if on the order of day, we will let the update be 3 days overdue
            elif 'days' in expected_freq.lower():
                x = int(expected_freq.lower()[0:-5])
                allowed_time = datetime.timedelta(days=x + 3)
            # if on the order of day, we will let the update be 3 days overdue
            elif 'weeks' in expected_freq.lower():
                x = int(expected_freq.lower()[0:-6])
                allowed_time = datetime.timedelta(days=x * 7 + 1)
            elif 'months' in expected_freq.lower():
                x = int(expected_freq.lower()[0:-7])
                allowed_time = datetime.timedelta(days=x * 30 + 30)
            else:
                allowed_time = datetime.timedelta(days=1)
            # allow for longer delay for TROPOMI data because it is slow to upload
            if 'bio.002' in r['data']['attributes']['name']:
                allowed_time = datetime.timedelta(days=30)
             # chlorophyll updates on the 19th of each month with the previous month's data
            if 'bio.037' in r['data']['attributes']['name']:
                allowed_time = datetime.timedelta(days=53)
            if 'cit.035' in r['data']['attributes']['name']:
                allowed_time = datetime.timedelta(days=45)
            #on the 1st of each month, this data set updates for the 1st of the previous month
            elif 'cli.005a' in r['data']['attributes']['name'] or 'cli.005b' in r['data']['attributes']['name']:
                allowed_time = datetime.timedelta(days=70)
            # within the first few days of each month, this data set updates for the 1st of the previous month
            elif 'cli.021' in r['data']['attributes']['name']:
                allowed_time = datetime.timedelta(days=70)
            #this forecast often goes offline for a few days
            elif 'cit.038' in r['data']['attributes']['name']:
                allowed_time = datetime.timedelta(days=5)
            # around the 15th of each month, this data set updates for the 15th of the PREVIOUS month
            elif 'cli.035' in r['data']['attributes']['name']:
                allowed_time = datetime.timedelta(days=70)
            elif 'cli.039' in r['data']['attributes']['name']:
                allowed_time = datetime.timedelta(days=60)
            #sea level rise data set updates at ~3 month delay
            elif 'cli.040' in r['data']['attributes']['name']:
                allowed_time = datetime.timedelta(days=120)
            #NDC ratification status probably would only update once a year, after COP
            elif 'cli.047' in r['data']['attributes']['name']:
                allowed_time = datetime.timedelta(days=400)
            #oil spills data set doesn't always have events that occur every 10 days
            elif 'ene.008' in r['data']['attributes']['name']:
                allowed_time = datetime.timedelta(days=15)
            elif 'foo.024' in r['data']['attributes']['name']:
                allowed_time = datetime.timedelta(days=11)
            elif 'for.012' in r['data']['attributes']['name']:
                allowed_time = datetime.timedelta(days=10)
            #flood data set doesn't always have events that occur every 10 days
            elif 'wat.040' in r['data']['attributes']['name']:
                allowed_time = datetime.timedelta(days=15)
            # check if the time since last update surpasses the time we allow for this type of data set
            if allowed_time < time_since_update:
                time_overdue = time_since_update - allowed_time
                # if this outdated data set has not been checked for why it isn't updating, send an error
                if dataset['WRI ID'] in error_tracking_df['WRI ID'].tolist():
                    if pd.isna(error_tracking_df[error_tracking_df['WRI ID'] == dataset['WRI ID']][
                                   'Known Reason?'].tolist()[0]):
                        logging.error(
                            '(OUTDATED) {wri_id} {public_title} - expected update frequency: {exp_up}. It has been {days} days since the last update.'.format(
                                wri_id=dataset['WRI ID'], public_title=dataset['Public Title'],
                                exp_up=dataset['Frequency of Updates'].lower(),
                                days=time_since_update.days))
                    else:
                        logging.info(
                            '(OUTDATED) {wri_id} {public_title} - expected update frequency: {exp_up}. It has been {days} days since the last update.'.format(
                                wri_id=dataset['WRI ID'], public_title=dataset['Public Title'],
                                exp_up=dataset['Frequency of Updates'].lower(),
                                days=time_since_update.days))
                else:
                    logging.error(
                        '(OUTDATED) {wri_id} {public_title} - expected update frequency: {exp_up}. It has been {days} days since the last update.'.format(
                            wri_id=dataset['WRI ID'], public_title=dataset['Public Title'],
                            exp_up=dataset['Frequency of Updates'].lower(),
                            days=time_since_update.days))
            else:
                logging.info('(OK) {wri_id} {public_title} up to date.'.format(wri_id=dataset['WRI ID'],
                                                                               public_title=dataset['Public Title']))
                # if that data set was previously outdated, send an alert to go remove it from the sheet
                if dataset['WRI ID'] in error_tracking_df['WRI ID'].tolist():
                    logging.error('{wri_id} {public_title} is now up to date.'.format(wri_id=dataset['WRI ID'],
                                                                                      public_title=dataset[
                                                                                          'Public Title']))

    # prepare weekly report to remind me to check any data sets that are still outdated
    for idx, dataset in error_tracking_df.iterrows():
        last_check = dataset['Last Check']
        last_check_dt = datetime.datetime.strptime(last_check, '%m/%d/%Y')
        today = datetime.datetime.utcnow()
        time_since_checking = today - last_check_dt
        days = time_since_checking.days
        if days > 7:
            logging.error('The status of {wri_id} has not been checked in {days} days.'.format(wri_id=dataset['WRI ID'],
                                                                                               days=days))
