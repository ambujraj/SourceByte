import configparser
import logging as log
import os
from datetime import datetime

import pandas as pd

config = configparser.ConfigParser()


def start_process():
    try:
        os.makedirs('files/staged')
        os.makedirs('files/output')
        os.makedirs('log')
    except OSError:
        pass
    log.basicConfig(level=log.INFO, format='%(name)s - %(levelname)s - %(message)s')
    # log.basicConfig(level=log.INFO, filename='log/{}.log'.format(start_time), filemode='w', format='%(name)s - %(
    # levelname)s - %(message)s')
    start_statement = "Started at: {}".format(start_time)
    log.info(start_statement)


def end_process():
    try:
        os.remove('files/staged/zomato.csv')
    except FileNotFoundError:
        pass
    finish_statement = "Ended at: {}\nTotal execution time: {}".format(end_time, (end_time - start_time))
    log.info(finish_statement)


def post_read_processing():
    df = pd.read_csv('files/input/zomato.csv')
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    df.reset_index(drop=True, inplace=True)

    staged_filename = 'files/staged/zomato.csv'
    df.to_csv(staged_filename)
    log.info("Written to staged: {}".format(staged_filename))

    return df


def extract_phone_to_file():
    phone_number = set()
    rating = {}
    for index, row in zomato_df.iterrows():
        phone_list = row['phone'].split(', ')
        if row['rate'] in rating:
            rating[row['rate']] = rating[row['rate']] + 1
        else:
            rating[row['rate']] = 1
        for phone in phone_list:
            if not phone.startswith('+'):
                phone = '+91 ' + phone
            phone_number.add(phone)
    print(rating)

    with open('files/output/phone.txt', 'w') as f:
        for phone in phone_number:
            f.write("%s\n" % phone)


if __name__ == '__main__':
    # ----------Start Statement------------
    start_time = datetime.now()
    start_process()
    # ----------Start Statement------------

    staged_file_exists = os.path.exists('files/staged/zomato.csv')
    if not staged_file_exists:
        zomato_df = post_read_processing()

    else:
        zomato_df = pd.read_csv('files/staged/zomato.csv')

    zomato_df['rate'] = zomato_df['rate'].apply(lambda x: x.split('/')[0])
    zomato_df['phone'] = zomato_df['phone'].apply(lambda x: x.replace('\r\n', ', '))
    zomato_df['phone'] = zomato_df['phone'].apply(lambda x: x.replace('\r\r', ''))
    zomato_df['phone'] = zomato_df['phone'].apply(lambda x: x.replace('\r', ''))
    print(zomato_df)

    extract_phone_to_file()

    output_filename = 'files/output/zomato.xlsx'
    excel_writer = pd.ExcelWriter(output_filename, engine='xlsxwriter')
    zomato_df.to_excel(excel_writer, sheet_name='Data', index=False)
    # TODO: add bar graph for the rating
    excel_writer.close()
    log.info("Written to output: {}".format(output_filename))

    # ----------End Statement------------
    end_time = datetime.now()
    end_process()
    # ----------End Statement------------
