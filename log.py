# -*- coding: utf-8 -*-

import logging
import time
import datetime


def create_dynamo_logger(logName):
    LOG_DIR = '/var/log/dynamodb/'
    LOG_FILE = logName + '-' + datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d') + '.log'

    logFile = logging.FileHandler(LOG_DIR + LOG_FILE)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logFile.setFormatter(formatter)
    logger = logging.getLogger('dynamodump')
    logger.setLevel(logging.INFO)
    logger.addHandler(logFile)
    return logger
