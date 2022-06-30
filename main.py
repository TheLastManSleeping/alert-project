from time import sleep
import pandas as pd
from classes.processing_class import ProcessingCLass

pd.options.mode.chained_assignment = None

while True:
    """Starting alert system"""
    processor = ProcessingCLass()
    processor.read_data()
    sleep(3600)
