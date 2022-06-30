import glob
import logging
import os

import configparser

import pandas as pd

from classes.hour_class import HourAlerts


class ProcessingCLass:

    def __init__(self):
        """Init class object with config"""
        self.config = configparser.ConfigParser()
        self.config.read('config.ini')

    def read_data(self):
        """
        Reads data from .csv files
        Removes all .csv files
        If there`v been some data in folder - calls start_processing function
        """
        raw_data = pd.DataFrame()
        for f in glob.glob(self.config['Docker']['reading_path']):
            append_data = pd.read_csv(f,
                                      delimiter=',',
                                      names=self.config['Data']['schema'].split(', '),
                                      parse_dates=['date'])
            raw_data = raw_data.append(append_data, ignore_index=True)
            os.remove(f)
        self.raw_data = raw_data
        if not self.raw_data.empty:
            self.start_processing()

    def start_processing(self):
        """
        Data processing function

        Removes non-error data
        Starts processing by creating object with the biggest time period
        and calls processing function of this object
        """
        self.logs_to_file()
        data = self.raw_data.loc[self.raw_data['severity'] == 'Error']
        data['date'] = pd.to_datetime(data['date'], unit='s', origin='unix')
        alerts = HourAlerts(data)
        alerts.global_alerts()

    def logs_to_file(self):
        """Writes logs to log.txt"""
        logger = logging.getLogger('file_logger')
        logger.setLevel(logging.INFO)
        logger.addHandler(logging.FileHandler(filename=self.config['Logs']['logging_file']))
        self.raw_data.apply(
            lambda row: logger.warning('\n' + row.to_string() + '\n') if row['severity'] == 'Error' else logger.info(
                '\n' + row.to_string() + '\n'), axis=1)
