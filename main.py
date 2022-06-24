import datetime
from time import sleep
import glob
import os

import pandas as pd

pd.options.mode.chained_assignment = None


def bundle_alerts(data):
    grouped_data = data.groupby(['bundle_id'])
    hour_data = grouped_data.resample('H', on='date').size()
    hour_data = hour_data.loc[hour_data > 10]
    hour_data = hour_data.groupby(['date'], sort=True)
    last_hour = 1
    for key, item in hour_data:
        for val, keys in zip(hour_data.get_group(key), hour_data.get_group(key).index):
            if last_hour != keys[1]:
                minute_alerts(data, keys[1])
                last_hour = keys[1]
            print(
                f"{keys[1] + datetime.timedelta(hours=1)}: received {val} errors during last hour on bundle {keys[0]}")


def minute_alerts(data, date):
    mask = (data['date'] >= date) & (data['date'] < date + datetime.timedelta(hours=1))
    hour_data = data.loc[mask]
    minute_data = hour_data.resample('60S', on='date').size()
    minute_data = minute_data.loc[minute_data > 10]
    global_alert(minute_data)


def global_alert(data):
    for row in data.items():
        print(f"{row[0] + datetime.timedelta(minutes=1)}: received {row[1]} errors during last minute")


raw_data = pd.DataFrame()
while True:
    for f in glob.glob('logs/*.csv'):
        append_data = pd.read_csv(f, delimiter=',',
                           names=['error_code', 'error_message', 'severity', 'log_location', 'mode', 'model',
                                  'graphics',
                                  'session_id', 'sdkv', 'test_mode', 'flow_id', 'flow_type', 'sdk_date', 'publisher_id',
                                  'game_id', 'bundle_id', 'appv', 'language', 'os', 'adv_id', 'gdpr', 'ccpa',
                                  'country_code',
                                  'date'], parse_dates=['date'])
        raw_data = raw_data.append(append_data, ignore_index=True)
        os.remove(f)
    if not raw_data.empty:
        raw_data.to_csv(r'all_logs.csv', mode='a', header=False, index=False)
        data = raw_data.loc[raw_data['severity'] == 'Error']
        data['date'] = pd.to_datetime(data['date'], unit='s', origin='unix')
        bundle_alerts(data)
    sleep(3600)