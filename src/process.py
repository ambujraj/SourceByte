from internal import write_to_result
import logging
from configuration.extract import get_common_value
from logger import CustomFormatter
import pandas as pd

process_id = get_common_value("ProcessId")
log = logging.getLogger("SourceByte")
log.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

ch.setFormatter(CustomFormatter())

log.addHandler(ch)


def process_file(zomato_df):
    zomato_df['rate'] = zomato_df['rate'].apply(lambda x: x.split('/')[0])
    zomato_df['phone'] = zomato_df['phone'].apply(lambda x: x.replace('\r\n', ', '))
    zomato_df['phone'] = zomato_df['phone'].apply(lambda x: x.replace('\r\r', ''))
    zomato_df['phone'] = zomato_df['phone'].apply(lambda x: x.replace('\r', ''))

    extract_phone_to_file(zomato_df)


def extract_phone_to_file(zomato_df):
    phone_number = set()
    for index, row in zomato_df.iterrows():
        phone_list = row['phone'].split(', ')
        for phone in phone_list:
            if not phone.startswith('+'):
                phone = '+91 ' + phone
            phone_number.add(phone)

    phone_df = pd.DataFrame(columns=["PhoneNumber"])
    for phone in phone_number:
        phone_df.loc[len(phone_df), ['PhoneNumber']] = phone

    write_to_result(phone_df, 'phone.xlsx')
