import glob
import os

import pandas as pd

from classes.hour_class import HourAlerts


class ProcessingCLass:

    def __init__(self):
        raw_data = pd.DataFrame()
        for f in glob.glob('logs/*.csv'):
            append_data = pd.read_csv(f, delimiter=',',
                                      names=['error_code', 'error_message', 'severity', 'log_location', 'mode',
                                             'model',
                                             'graphics',
                                             'session_id', 'sdkv', 'test_mode', 'flow_id', 'flow_type', 'sdk_date',
                                             'publisher_id',
                                             'game_id', 'bundle_id', 'appv', 'language', 'os', 'adv_id', 'gdpr',
                                             'ccpa',
                                             'country_code',
                                             'date'], parse_dates=['date'])
            raw_data = raw_data.append(append_data, ignore_index=True)
            os.remove(f)
        self.raw_data = raw_data

    def start_processing(self):
        if not self.raw_data.empty:
            self.raw_data.to_csv(r'all_logs.csv', mode='a', header=False, index=False)
            data = self.raw_data.loc[self.raw_data['severity'] == 'Error']
            data['date'] = pd.to_datetime(data['date'], unit='s', origin='unix')
            alerts = HourAlerts(data)
            alerts.global_alerts()
