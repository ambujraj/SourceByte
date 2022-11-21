import logging as log
import os
from datetime import datetime
from configuration.extract import get_common_value
from internal import *
from process import *
import pandas as pd
from database import db

if __name__ == '__main__':
    # ----------Start Statement------------
    start_time = datetime.now()
    start_process(start_time)
    # ----------Start Statement------------

    # ----------Read Input------------
    zomato_df = read_input()
    # ----------Read Input------------

    log.info("Processing with the file...")
    process_file(zomato_df)  # The main process (change this as per your need)
    log.info("Processed.")

    # ----------Writing to result------------
    write_to_result(zomato_df, 'result.xlsx')
    move_to_processed()
    # ----------Writing to result------------

    # ----------End Statement------------
    end_time = datetime.now()
    end_process(start_time, end_time)
    # ----------End Statement------------
