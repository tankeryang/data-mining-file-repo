# coding=utf-8
import os
import sys
import re
import time
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta, date
# customer module
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from module import PrestoUtils


HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36 OPR/52.0.2871.40 (Edition Baidu)',
    'Connection': 'keep-alive'
}
SQL_DROP_TABLE = """
    drop table if exists ods_mms.city_weather_future
"""
SQL_CREATE_TABLE = """
    create table if not exists ods_mms.city_weather_future (
        area_id           varchar,
        dates             varchar,
        weather_day       varchar,
        weather_day_id    varchar,
        weather_night     varchar,
        weather_night_id  varchar,
        tempreture_day    integer,
        tempreture_night  integer,
        wind              varchar,
        wind_id           varchar,
        wind_comp         integer,
        sunrise           varchar,
        sunset            varchar
    )
"""


def get_param_dict():
    presto_engin = create_engine('presto://prod@10.10.22.8:10300/prod_hive/ods_mms')
    con = presto_engin.connect()
    sql_get_areaid = """
        select distinct cwsa.areaid from ods_mms.weather_support_area cwsa
        right join ods_mms.city_weather_history cwh
        on cwsa.area = cwh.city_name
    """
    
    param_dict = {}
    areaid_list = pd.read_sql_query(sql=sql_get_areaid, con=con)['areaid'].tolist()
    start_date = datetime.today().date()
    end_date = start_date + timedelta(days=14)

    param_dict['areaid_list'] = areaid_list
    param_dict['start_date'] = start_date.strftime(format='%Y%m%d')
    param_dict['end_date'] = end_date.strftime(format='%Y%m%d')

    return param_dict


def get_values(json, **kw):
    return (
        kw['areaid'],
        (datetime.strptime(kw['start_date'], '%Y%m%d') + timedelta(days=kw['days'])).strftime('%Y-%m-%d'),
        json['w_am'],
        json['cw_am'],
        json['w_pm'],
        json['cw_pm'],
        json['tmp_max'],
        json['tmp_min'],
        json['wd'],
        json['cwd'],
        eval(re.sub(r'\D', '', json['wind'])[0] if len(json['wind']) != 0 else '0'),
        json['sunrise'],
        json['sunset']
    )


def insert_into_table(conn, param_dict, headers):
    base_url = 'http://v.juhe.cn/xiangji_weather/15_area.php'
    appkey = '68e1e80009c0aa3e81a2ff146007d19d'

    session = requests.session()
    values_list = []

    for areaid in param_dict['areaid_list']:
        suffix = """&areaid={areaid}&startTime={startTime}&endTime={endTime}
        """.format(areaid=areaid, startTime=param_dict['start_date'], endTime=param_dict['end_date'])
        request_url = base_url + '?key=' + appkey + suffix
        # print(request_url)
        response_list = session.get(url=request_url, headers=headers).json()
        # print(response_list)
        if response_list['reason'] == 'success':
            for days, response_json in enumerate(response_list['result']['series']):
                values = get_values(response_json, areaid=areaid, start_date=param_dict['start_date'], days=days)
                values_list.append(values)
                # print(len(values))
        else:
            raise Exception("param error! error code: {}".format(response_list['error_code']))

    if values_list is not None:
        sql = """
            insert into ods_mms.city_weather_future
            values {}
        """.format('{}'.format(values_list).strip('[').strip(']'))
        cur = conn.cursor()
        cur.execute(sql)
        time.sleep(5)
        # print(cur.fetchall())
        # print('insert success!')
    else:
        raise Exception("values_list None!")


def execute():
    param_dict = get_param_dict()
    pu = PrestoUtils(host='10.10.22.8', port='10300', user='prod', catalog='prod_hive')

    pu.drop_table(SQL_DROP_TABLE)
    pu.create_table(SQL_CREATE_TABLE)
    insert_into_table(pu.presto_conn, param_dict, HEADERS)


if __name__ == '__main__':
    execute()
