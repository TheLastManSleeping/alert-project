import datetime
import logging


class MinuteAlerts:

    def __init__(self, data, date):
        """
        Initialising class object

        Here data gets grouped according to time period and stored inside object
        """
        mask = (data['date'] >= date) & (data['date'] < date + datetime.timedelta(hours=1))
        hour_data = data.loc[mask]
        self.resampled_data = hour_data.resample('60S', on='date').size()

    def global_alert(self):
        """Alerts for >10 global errors in a minute"""
        data = self.resampled_data.loc[self.resampled_data > 10]
        for row in data.items():
            logging.warning(f"{row[0] + datetime.timedelta(minutes=1)}: received {row[1]} errors during last minute")