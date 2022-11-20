import logging as log
import os
from datetime import datetime
from configuration.extract import get_common_value
from process import *
import pandas as pd

if __name__ == '__main__':
    # ----------Start Statement------------
    start_time = datetime.now()
    start_process(start_time)
    # ----------Start Statement------------
    process_id = get_common_value("ProcessId")
    staged_file_exists = os.path.exists('../data/{}/staged/staged.xlsx'.format(process_id))
    if not staged_file_exists:
        zomato_df = post_read_processing()

    else:
        log.info("Reading from staged...")
        zomato_df = pd.read_excel('../data/{}/staged/staged.xlsx'.format(process_id), sheet_name='Data')
        log.info("Completed reading from staged.")

    log.info("Processing with the file...")
    zomato_df['rate'] = zomato_df['rate'].apply(lambda x: x.split('/')[0])
    zomato_df['phone'] = zomato_df['phone'].apply(lambda x: x.replace('\r\n', ', '))
    zomato_df['phone'] = zomato_df['phone'].apply(lambda x: x.replace('\r\r', ''))
    zomato_df['phone'] = zomato_df['phone'].apply(lambda x: x.replace('\r', ''))

    extract_phone_to_file(zomato_df)
    log.info("Processed.")
    if get_common_value("WriteToS3"):
        output_filename = 'result.xlsx'
        write_to_s3(zomato_df, output_filename)
    else:
        output_filename = '../data/{}/result/result.xlsx'.format(process_id)
        log.info("Writing to Result...")
        excel_writer = pd.ExcelWriter(output_filename, engine='xlsxwriter')
        sheet_name = "Data"
        zomato_df.to_excel(excel_writer, sheet_name=sheet_name, index=False)
        # TODO: add bar graph for the rating
        excel_writer.close()
    log.info("Written to Result: {}".format(output_filename))

    move_to_processed()

    log.info("Highest occurring rating: {}".format(zomato_df['rate'].mode()))
    # ----------End Statement------------
    end_time = datetime.now()
    end_process(start_time, end_time)
    # ----------End Statement------------
