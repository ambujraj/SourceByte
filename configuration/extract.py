import configparser
import os

config = configparser.ConfigParser()
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
config.read(ROOT_DIR + '/config.ini')


def get_common_value(name):
    value = config['COMMON'][name]
    if value == 'yes':
        return True
    elif value == 'no':
        return False
    else:
        return value
