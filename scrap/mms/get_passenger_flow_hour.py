# coding=utf-8
import os
import sys
import re
import time
import getopt
import requests
import pandas as pd
from functools import partial
from sqlalchemy import create_engine
from datetime import datetime, timedelta, date
# customer utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from module import PrestoUtils


HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36 OPR/52.0.2871.40 (Edition Baidu)',
    'Connection': 'keep-alive'
}
SQL_DROP_TABLE = """
    drop table if exists ods_mms.passenger_flow_hour
"""
SQL_CREATE_TABLE = """
    create table if not exists ods_mms.passenger_flow_hour (
        brand       varchar,
        store_code  varchar,
        store_name  varchar,
        device_mac  varchar,
        inCount     integer,
        outCount    integer,
        date        varchar,
        time        varchar,
        datetime    varchar
    )
"""

pu = PrestoUtils(host='10.10.22.8', port='10300', user='prod', catalog='prod_hive')

def get_token(headers):
    base_url = 'http://www.sr-ts.cn/service/mobileLogin.action'
    email = 'FIVEPLUS'
    password = '888888'
    suffix = '?email={email}&password={password}'.format(email=email, password=password)

    request_url = base_url + suffix

    session = requests.session()
    response_json = session.get(url=request_url, headers=headers).json()

    return response_json['data']['data']['token']


def get_devices_config_list(headers):
    base_url = 'http://www.sr-ts.cn/service/getAllPassengerShops.action'
    token = get_token(headers)
    suffix = '?token={token}'.format(token=token)
    
    devices_config_list = []

    request_url = base_url + suffix

    session = requests.session()
    response_json = session.get(url=request_url, headers=headers).json()
    # print(response_json['data']['data'][0]['devices'])
    data_list = response_json['data']['data']
    for data in data_list:
        devices_config = {}
        mac_list= []
        # print(data)
        for device in data['devices']:
            # print(device)
            mac_list.append(device['mac'])
        
        devices_config['id'] = data['id']
        devices_config['name'] = data['name']
        devices_config['mac_list'] = mac_list
    
        devices_config_list.append(devices_config)

    # print(devices_config_list)
    return devices_config_list


def get_values(response_json, **kwargs):
    values = []
    if response_json['result'] == 'ok':
        data_list = response_json['data']['data']
        for data in data_list:
            result = (
                kwargs['brand'],
                kwargs['store_code'],
                kwargs['store_name'],
                data['mac'],
                data['inCount'],
                data['outCount'],
                data['time'].split(' ')[0],
                data['time'].split(' ')[1],
                data['time'],
            )
            values.append(result)
            # print(values)
    else:
        values = None

    return values


def insert_into_table(conn, values_list):
    if len(values_list) != 0:
        values_batch = '{}'.format(values_list).strip('[').strip(']')
        sql = """
            insert into ods_mms.passenger_flow_hour
            values {}
        """.format(values_batch)
        # print(sql)
        cur = conn.cursor()
        cur.execute(sql)
        print(cur.fetchall())
    else:
        pass


def get_result(conn, headers, devices_config_list, ptype=None):
    if ptype is None or ptype not in ('-i', "--init", '-u', "--update"):
        raise Exception("ptype error! use -h for help")
    
    else:
        base_url = 'http://www.sr-ts.cn/service/getPassengers.action'
        token = get_token(headers)
        session = requests.session()
        presto_engin = create_engine('presto://prod@10.10.22.8:10300/prod_hive/ods_mms')
        con = presto_engin.connect()
        values_list = []

        if ptype in ('-i', "--init"):
            pu.drop_table(SQL_DROP_TABLE)
            pu.create_table(SQL_CREATE_TABLE)

            start_time = pd.Timestamp(datetime.today().date() - timedelta(days=60))
            end_time = pd.Timestamp(datetime.today().date())

        if ptype in ('-u', "--update"):
            sql_last_time = """
                select distinct max(datetime) time from ods_mms.passenger_flow_hour
            """

            start_time = datetime.strptime(pd.read_sql_query(sql_last_time, con)['time'].values[0], '%Y-%m-%d %H:%M:%S')
            end_time = datetime.now()

        for devices_config in devices_config_list:
            store_id = devices_config['id']
            name = devices_config['name']
            brand = name.split('_')[0]
            store_code = name.split('_')[1]
            store_name = name.split('_')[2]

            for mac in devices_config['mac_list']:
                stime = start_time.strftime('%Y-%m-%d %H:%M:%S')
                etime = end_time.strftime('%Y-%m-%d %H:%M:%S')

                suffix = """?token={token}&id={id}&mac={mac}&stime={stime}&etime={etime}
                """.format(token=token, id=store_id, mac=mac, stime=stime, etime=etime)
                request_url = base_url + suffix

                response_json = session.get(url=request_url, headers=headers).json()

                values = get_values(response_json, brand=brand, store_code=store_code, store_name=store_name)

                if values is not None:
                    values_list.extend(values)
                else:
                    pass

        insert_into_table(conn, values_list)
        

def execute(argv):
    message = """
    Usage: python36 get_city_weather_history.py <command>
    Commands:
    [-h | --help]    Show help message.
    [-i | --init]    Initialize table. use in the first time. Do not use it after the first init was completed!!!
    [-u | --update]  Update table. Use this command after first init.
    """
    try:
        opts, _ = getopt.getopt(argv, "hiu", ["help", "init", "update"])
    except getopt.GetoptError:
        print("Command not found. Use -h|--help for help")
        sys.exit(2)
    else:
        if len(opts) < 1:
            raise Exception("Command not found. Use -h for help.")
        for opt, _ in opts:
            if opt in ('-h', "--help"):
                print(message)
            elif opt in ('-i', "--init", '-u', "--update"):
                devices_config_list = get_devices_config_list(HEADERS)
                get_result(pu.presto_conn, HEADERS, devices_config_list, ptype=opt)


if __name__ == '__main__':
    execute(sys.argv[1:])
