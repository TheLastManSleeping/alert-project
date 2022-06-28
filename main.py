from time import sleep
import pandas as pd
from classes.processing_class import ProcessingCLass

pd.options.mode.chained_assignment = None

while True:
    processor = ProcessingCLass()
    processor.start_processing()
    sleep(3600)
