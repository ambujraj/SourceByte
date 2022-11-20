from os import makedirs, remove, listdir, path, rename, rmdir, getenv
import logging
import pandas as pd
from socket import gethostbyname, create_connection
from io import BytesIO, StringIO
from configuration.extract import get_common_value
from shutil import rmtree
from database.db import add_history
from logger import CustomFormatter
import boto3

process_id = get_common_value("ProcessId")
log = logging.getLogger("SourceByte")
log.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

ch.setFormatter(CustomFormatter())

log.addHandler(ch)
if get_common_value("GetFromS3") or get_common_value("WriteToS3"):
    access_key = get_common_value("AWSAccessKey")
    # secret_access_key = getenv(get_common_value("AWSSecretKey"))
    secret_access_key = get_common_value("AWSSecretKey")
    bucket_name = get_common_value("S3BucketName")
    s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)
    s3_resource = boto3.resource('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)


def process_file(zomato_df):
    zomato_df['rate'] = zomato_df['rate'].apply(lambda x: x.split('/')[0])
    zomato_df['phone'] = zomato_df['phone'].apply(lambda x: x.replace('\r\n', ', '))
    zomato_df['phone'] = zomato_df['phone'].apply(lambda x: x.replace('\r\r', ''))
    zomato_df['phone'] = zomato_df['phone'].apply(lambda x: x.replace('\r', ''))

    extract_phone_to_file(zomato_df)


def start_process(start_time):
    start_statement = "Started at: {}".format(start_time)
    log.info(start_statement)
    add_history()
    if get_common_value("WriteToS3"):
        try:
            bucket = s3_resource.Bucket(bucket_name)
            bucket.objects.filter(Prefix="{}/result".format(process_id)).delete()
        except Exception:
            pass
    if get_common_value("GetFromS3"):
        paths_to_be_present = [process_id, "{}/raw".format(process_id)]
        for path_to_exist in paths_to_be_present:
            if not folder_exists_and_not_empty(bucket_name, path_to_exist):
                log.error("Cannot find the folders needed in S3.")
                exit()
        try:
            makedirs("../data/{}".format(process_id))
        except OSError:
            pass
        try:
            makedirs("../data/{}/staged".format(process_id))
        except OSError:
            pass
        try:
            makedirs("../data/{}/result".format(process_id))
        except OSError:
            pass

    else:
        paths_to_be_present = ["../data/{}".format(process_id), "../data/{}/raw".format(process_id)]
        for path_to_exist in paths_to_be_present:
            if not path.exists(path_to_exist):
                log.error("Cannot find the folders needed.")
                exit()
        try:
            makedirs("../data/{}/staged".format(process_id))
        except OSError:
            pass
        try:
            makedirs("../data/{}/processed".format(process_id))
        except OSError:
            pass
        try:
            makedirs("../data/{}/result".format(process_id))
        except OSError:
            pass

    try:
        makedirs("../log")
    except OSError:
        pass

    # log.basicConfig(level=log.INFO, filename='log/{}.log'.format(start_time), filemode='w', format='%(name)s - %(
    # levelname)s - %(message)s')


def end_process(start_time, end_time):
    try:
        log.info("Removing the staged files...")
        if get_common_value("WriteToS3"):
            rmtree("../data/{}".format(process_id), ignore_errors=True)
        else:
            remove('../data/{}/staged/staged.xlsx'.format(process_id))
        log.info("Removed.")
    except FileNotFoundError:
        pass
    finish_statement = "Ended at: {}\nTotal execution time: {}".format(end_time, (end_time - start_time))
    log.info(finish_statement)


def get_from_s3():
    file_name = "{}/raw/{}".format(process_id, get_common_value("S3RawFile"))
    response = s3_client.get_object(Bucket=bucket_name, Key=file_name)

    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        return pd.read_excel(BytesIO(response['Body'].read()))
    else:
        log.error(f"Unsuccessful S3 get_object response for file: {file_name}. Status - {status}")


def write_to_s3(df, file_name):
    log.info("Writing to S3 File: {}...".format(file_name))
    file_name = "{}/result/{}".format(process_id, file_name)
    buffer = BytesIO()
    df.to_excel(buffer)
    s3_resource.Object(bucket_name, file_name).put(Body=buffer.getvalue())
    log.info("Written to S3.")


def post_read_processing():
    if get_common_value("GetFromS3"):
        if not is_connected():
            log.error("Not connected to Internet! Either use offline files or wait for some time.")
            exit()
        log.info("Fetching file from AWS S3...")
        df = get_from_s3()
        log.info("Successfully fetched.")
    else:
        log.info("Fetching file from folder...")
        file_name = get_common_value("S3RawFile")
        # TODO: read all files in folder
        df = pd.read_excel("../data/{}/raw/{}".format(process_id, file_name), sheet_name='Data')
        log.info("Successfully fetched.")
    log.info("Cleaning the data from file...")
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    df.reset_index(drop=True, inplace=True)
    log.info("Cleaned.")
    staged_filename = '../data/{}/staged/staged.xlsx'.format(process_id)
    log.info("Writing to staged..")
    excel_writer_staged = pd.ExcelWriter(staged_filename, engine='xlsxwriter')
    df.to_excel(excel_writer_staged, sheet_name='Data', index=False)
    excel_writer_staged.close()
    log.info("Written to staged: {}".format(staged_filename))

    return df


def extract_phone_to_file(zomato_df):
    phone_number = set()
    for index, row in zomato_df.iterrows():
        phone_list = row['phone'].split(', ')
        for phone in phone_list:
            if not phone.startswith('+'):
                phone = '+91 ' + phone
            phone_number.add(phone)

    if get_common_value("WriteToS3"):
        phone_df = pd.DataFrame(columns=["PhoneNumber"])
        for phone in phone_number:
            phone_df.loc[len(phone_df), ['PhoneNumber']] = phone
        write_to_s3(phone_df, "phone.xlsx")
    else:
        with open('../data/{}/result/phone.txt'.format(process_id), 'w') as f:
            for phone in phone_number:
                f.write("%s\n" % phone)


def is_connected():
    try:
        hostname = "one.one.one.one"
        host = gethostbyname(hostname)
        # connect to the host -- tells us if the host is actually reachable
        s = create_connection((host, 80), 2)
        s.close()
        return True
    except Exception:
        pass
    return False


def move_to_processed():
    if get_common_value("GetFromS3"):
        bucket = s3_resource.Bucket(bucket_name)
        all_files = []
        for object_summary in bucket.objects.filter(Prefix="{}/raw/".format(process_id)):
            all_files.append(object_summary.key)
        all_files = all_files[1:]
        for file in all_files:
            copy_source = {
                'Bucket': bucket_name,
                'Key': file
            }
            destination_key = file.replace("raw", "processed")
            s3_resource.meta.client.copy(copy_source, bucket_name, destination_key)
            s3_resource.Object(bucket_name, file).delete()
    else:
        source = "../data/{}/raw".format(process_id)
        destination = "../data/{}/processed".format(process_id)
        allfiles = listdir(source)
        for f in allfiles:
            src_path = path.join(source, f)
            dst_path = path.join(destination, f)
            rename(src_path, dst_path)


def folder_exists_and_not_empty(bucket: str, path_to_exist: str):
    if not path_to_exist.endswith('/'):
        path_to_exist = path_to_exist + '/'
    resp = s3_client.list_objects(Bucket=bucket, Prefix=path_to_exist, Delimiter='/', MaxKeys=1)
    return 'Contents' in resp
