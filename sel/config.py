import configparser
import os


def read():
    config = configparser.ConfigParser()
    directory = os.path.dirname(__file__)
    config.read(os.path.join(directory, "conf.ini"))

    return config
