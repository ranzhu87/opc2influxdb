#!/usr/bin/env python
# -*- coding: utf8 -*-


# import argparse
# import os
# import sys
import ConfigParser
import time
import datetime
import logging
# import pandas as ps
import threading
import OpenOPC
import re
from influxdb import InfluxDBClient

# define the logging format


def initLogging(logfile_prefix, level_name, log_name):
    LEVELS = {'debug': logging.DEBUG,
              'info': logging.INFO,
              'warning': logging.WARNING,
              'error': logging.ERROR,
              'critical': logging.CRITICAL}

    datefmt = '%Y-%m-%d %H:%M:%S'

    # -- Create Logfile Name if prefix exists

    if logfile_prefix:
        now = datetime.datetime.now()
        LOG_FILENAME = './log/' + logfile_prefix + '_' + now.strftime("%Y-%m-%d") + '.log'
    else:
        LOG_FILENAME = None

    # -- Get the level for logging
    level = LEVELS.get(level_name, logging.NOTSET)

    # -- add filename=LOG_FILENAME below as parameter to write to file
    logging.basicConfig(level=level,
                        format='%(asctime)s - %(message)s',
                        datefmt=datefmt,
                        filename=LOG_FILENAME
                        )
    logger = logging.getLogger(log_name)

    #formatter = logging.Formatter("%("+datefmt+")s - %(name)s - %(levelname)s - %(message)s")
    #ch = logging.StreamHandler()
    # ch.setFormatter(formatter)
    # logger.addHandler(ch)

    return logger
#


def store2db(logger, opcdata, dbname='test', dbaddrs='127.0.0.1:8086', user='admin', password='admin'):
    store_data = opcdata2influxdbjson(opcdata)
    host = dbaddrs.split(':')[0]
    port = int(dbaddrs.split(':')[1])
    client = InfluxDBClient(host, port, user, password, dbname)

    try:
        client.create_database(dbname)
        client.write_points(store_data, time_precision='s')
    except Exception as e:
        logger.info('... FAILED to store data to DB ... ')
        logger.info(e)
        raise e

    # client.create_database(dbname)

# data convert
#('Random.int4',23423,GOOD,'12/23/16 22:12:20')
#   =>{"time":1489653400,"measurement":Random.int4,'fields':{'value':23423},'tags':{'quality':}}


def opcdata2influxdbjson(opcdata):
    # print opcdata
    series = []
    def tupledata2json(data):
        pointValues = {
            "time": string2timestamp(data[3]),
            "measurement": data[0],
            'fields': {
                'value': data[1],
            },
            'tags': {
                "quality": data[2],
            },
        }
        return pointValues
    if isinstance(opcdata, list):
        for idata in opcdata:
            if isinstance(idata, list):
                for _idata in idata:
                    if isinstance(_idata, tuple):
                        series.append(tupledata2json(_idata))
            elif isinstance(idata, tuple):
                series.append(tupledata2json(idata))
    return series

# '12/08/16 16:43:37' --> 1440751417
# 或者 '2015-08-28 16:43:37' --> 1440751417.0


def string2timestamp(strValue):

    if strValue:
        d = datetime.datetime.strptime(strValue, "%m/%d/%y %H:%M:%S")
    else:
        d = datetime.datetime.now()

    t = d.timetuple()
    timeStamp = int(time.mktime(t))
    # timeStamp = float(str(timeStamp) + str("%06d" % d.microsecond))/1000000
    # print timeStamp
    return timeStamp


# Make a logger
# logfile_prefix = 'log'
# level_name = 'info'
# logname = 'opc2influxdb'
# logger = mTools.initLogging(logfile_prefix, level_name, logname)


# logger.info('## ---------------------------------- ##')
# logger.info('## STARTING opc2influxdb ------------ ##')
# logger.info('## ---------------------------------- ##')

# Read the config file
class _read_config(object):
    """docstring for _read_config"""

    def __init__(self, logger, config_name):
        self.logger = logger
        config_filename = config_name  # 'default.conf'
        config = ConfigParser.ConfigParser()
        file_handle = open(config_filename)
        config.readfp(file_handle)
        self.logger.info('Config file: ' + config_filename)
        # Read opc config includes address name and list (tags)
        self.opc_addrs = config.get('opc', 'opcaddrs').strip().replace(' ', '').split(',')  # =>like [192.168.1.1:8888,192.168.1.1:8888]
        self.opc_names = config.get('opc', 'opcnames').strip().replace(' ', '').split(',')  # => like Matrikon.Simulation.1, Matrikon.Simulation.1]\
        _tags_temp = config.get('opc', 'opclists').strip().replace(' ', '').split('],[')  # => like  [[Random.Int4.*,Random.Float8.*],[],[],]
        self.opc_lists = []
        for _tags_ in _tags_temp:
            self.opc_lists.append(_tags_.strip().replace(' ', '').replace('[', '').replace(']', '').split(','))

        # Read influxdb config includes
        # The database's name is auto create by opc_server_lists
        self.db_addrs = config.get('influxdbs', 'influxdbaddrs').strip().replace(' ', '').split(',')
        self.db_names = config.get('influxdbs', 'influxdbnames').strip().replace(' ', '').split(',')
        # Read the read's timer and store's timer(frequecry)
        self.read_timer = config.get('timer', 'readtimer').strip().replace(' ', '').split(',')  # =>like [1,2]
        self.store_timer = config.get('timer', 'storetimer').strip().replace(' ', '').split(',')  # =>like [2,3]


class _opc2influxdb_thread(threading.Thread):

    def __init__(self, logger, opcaddrs='127.0.0.1:7766', opcname='Matrikon.OPC.Simulation.1', tags=[['*.Int4*', ], ], loop_time=10, dbname='test', dbaddrs='127.0.0.1:8086'):
        threading.Thread.__init__(self, name=opcname)

        self.logger = logger

        _opc_addr = opcaddrs.split(':')
        self.ip = _opc_addr[0]
        self.port = int(_opc_addr[1])
        self.opcname = opcname
        self.tags = tags
        self.opc = None
        self.opc_connected = False
        self.read_data = []
        self.loop_time = loop_time
        self.dbname = dbname
        self.dbaddrs = dbaddrs

    def _initConnnect(self):

        try:
            self.opc = OpenOPC.open_client(self.ip, self.port)
            self.opc.connect(self.opcname)
            # print self.opc_connected
        except IOError as e:
            self.logger.info('... FAILED to connect to OPC Server:{0}'.format(self.opcname))
            self.opc_connected = False
            raise e

        else:
            self.logger.info('... Success to connect to OPC Server:{0}'.format(self.opcname))
            self.opc_connected = True

        # return the  opc connect status
        # if not connect reconnectxx

        if self.opc_connected:
            return True
        else:
            time.sleep(5)
            # self._initConnnect

    def _iread(self):

        self.read_data = []

        for _tags in self.tags:

            # _tag_name = _tags.replace('*', '').replace('.', '')
            _list_en = True
            _flat = True
            # opc.list flat always True but when the opcserver is Matirkon we need attention
            # if opcserver name is matrikon , the tags need start with *
            # Regx find * if includes * then _list_en = True  that is need use the list function to read tags

            if re.findall('\\*', _tags):
                _list_en = True
            else:
                _list_en = False

            # print self.opc.read(self.opc.list(_tags, flat=True))
            if _tags:

                try:
                    if _list_en:
                        _data = self.opc.read(self.opc.list(_tags, flat=_flat))
                    else:
                        _data_name = (_tags,)
                        _data = _data_name + self.opc.read(_tags)
                except Exception as e:
                    self.logger.info('... FAILED to read' + ' ' + _tags)
                    self.logger.info(e)
                    raise e
                else:
                    self.read_data.append(_data)
    # def _dataparse(self):

    # def _read(self):
    #     # if self.opc_connected:

    #     for one_tags in self.tags:
    #         print one_tags
            # if one_tags:
            #     self._iread(one_tags)
            # else:
            #     self.logger.info()
    def _close(self):
        self.opc.close()

    # store the data
    def _store(self):
        if self.read_data:
            try:
                store2db(self.logger, self.read_data, self.dbname, self.dbaddrs)
            except Exception as e:
                raise e
                self.logger.info('----store to {0} error  ----'.format(self.dbname))
        else:
            self.logger.info('----the data: {0} is wrong  ----'.format(self.read_data))

    def run(self):
        first_read = True
        while True:
            if first_read:
                try:
                    self._initConnnect()
                except Exception as e:
                    self.logger.error(e)
                else:
                    first_read = False
            try:
                self._iread()
            except Exception as read_err:
                self.logger.info('... The opcsever {0} is connceting....'.format(self.opcname))
                self.logger.info(read_err)
                try:
                    self._initConnnect()
                except Exception as connect_err:
                    self.logger.error('connect {0} opcserver error'.format(self.opcname))
                    self.logger.error(connect_err)
               
            else:
                self.logger.info('..Read  {0} of {1} is DONE ..'.format(str(self.tags), self.opcname))
                print('..Read  {0} of {1} is DONE ..'.format(str(self.tags), self.opcname))

                try:
                    self._store()
                except Exception as store_err:
                    # self.logger.info('... The influxdb {0} can not conncet....'.format(self.dbname))
                    self.logger.error(store_err)
                else:
                    self.logger.info('..Store  {0} to {1} is DONE ..'.format(str(self.tags), self.dbname))
                    print('..Store The {0} to {1} is DONE ..'.format(str(self.tags), self.dbname))


            # self.logger.info('..Read and store The {0} of {1} is ok ..'.format(str(self.tags), self.opcname))
            # print('..Read and store The {0} of {1} is ok ..'.format(str(self.tags), self.opcname))
            
            time.sleep(float(self.loop_time))

        # print self.read_data

        # def __init__(self, logger, )


def main():
    logfile_prefix = 'log'
    level_name = 'info'
    logname = 'opc2influxdb'
    logger = initLogging(logfile_prefix, level_name, logname)
    logger.info('## ---------------------------------- ##')
    logger.info('## ------STARTING opc2influxdb ------ ##')
    logger.info('## ---------------------------------- ##')
    print('## ---------------------------------- ##')
    print('## ------STARTING opc2influxdb ------ ##')
    print('## ---------------------------------- ##')

    conf = _read_config(logger, 'default.conf')
    # print conf.db_addrs
    # print len(conf.db_addrs)-1
    data_stores = []
    for i in range(0, len(conf.opc_addrs)):
        time.sleep(1)
        # print conf.db_addrs[i], conf.db_names[i], conf.opc_names[i], conf.opc_addrs[i], conf.read_timer[i], conf.opc_lists[i]
        opcaddrs = conf.opc_addrs[i]
        opcname = conf.opc_names[i]
        tags = conf.opc_lists[i]
        dbaddrs = conf.db_addrs[i]
        dbname = conf.db_names[i]
        loop_time = conf.read_timer[i]
        # _opc2influxdb_thread(logger, opcaddrs='127.0.0.1:7766', opcname='Matrikon.OPC.Simulation.1', loop_time=10, dbname='test', dbaddrs='127.0.0.1:8086')
        data_store = _opc2influxdb_thread(logger, opcaddrs, opcname, tags, loop_time, dbname, dbaddrs)
        data_store.start()
        data_stores.append(data_store)

    for trace_store in data_stores:
        trace_store.join()
    raw_input('Enter some keys to exits')

    # data_stores[i] = _opc2influxdb_thread(logger, conf.opc_addrs[i], conf.opc_names[i], conf.opc_lists[i],conf.read_timer[i], conf.db_names[i], conf.db_addrs[i])
    # data_stores[i].start()


if __name__ == '__main__':
    main()


# class opc2influxdb(object):
    """docstring for opc2influxdb"""
