import datetime
import logging

from classes.minute_class import MinuteAlerts


class HourAlerts:

    def __init__(self, data):
        grouped_data = data.groupby(['bundle_id'])
        hour_data = grouped_data.resample('H', on='date').size()
        hour_data = hour_data.loc[hour_data > 10]
        self.data = data
        self.resampled_and_grouped_data = hour_data.groupby(['date'], sort=True)

    def global_alerts(self):
        last_hour = 1
        for key, item in self.resampled_and_grouped_data:
            for val, keys in zip(self.resampled_and_grouped_data.get_group(key),
                                 self.resampled_and_grouped_data.get_group(key).index):
                if last_hour != keys[1]:
                    self.minute_alerts(keys[1])
                    last_hour = keys[1]
                logging.warning(
                    f"{keys[1] + datetime.timedelta(hours=1)}: received {val} errors during last hour on bundle {keys[0]}")

    def minute_alerts(self, date):
        a = MinuteAlerts(self.data, date)
        a.global_alert()
