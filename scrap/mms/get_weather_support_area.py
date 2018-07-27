# coding=utf-8
import os
import sys
import requests

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'. '..'))
)
from module.utils import get_prestodb_conn, create_table, drop_table


HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36 OPR/52.0.2871.40 (Edition Baidu)',
    'Connection': 'keep-alive'
}
SQL_DROP_TABLE = """
    drop table if exists ods_mms.weather_support_area
"""
SQL_CREATE_TABLE = """
    create table if not exists ods_mms.weather_support_area (
        province  varchar,
        city      varchar,
        area      varchar,
        areaid   varchar,
        lon       varchar,
        lat       varchar
    )
"""


def get_values(json):
    return (
        json['province'],
        json['city'],
        json['area'],
        json['areaid'],
        json['lon'],
        json['lat']
    )


def insert_into_table(conn, headers):
    base_url = 'http://v.juhe.cn/xiangji_weather/areaList.php'
    appkey = '68e1e80009c0aa3e81a2ff146007d19d'
    request_url = base_url + '?key=' + appkey

    session = requests.session()
    response_list = session.get(url=request_url, headers=headers).json()

    if response_list['error_code'] == 0:
        values_list = []
        for response_json in response_list['result']:
            values_list.append(get_values(response_json))
        
        sql = """
            insert into ods_mms.weather_support_area
            values {}
        """.format('{}'.format(values_list).strip('[').strip(']'))
        cur = conn.cursor()
        cur.execute(sql)
    else:
        raise Exception("params error! error code: {}".format(response_list['error_code']))


def execute():
    with get_prestodb_conn() as conn:
        drop_table(conn, SQL_DROP_TABLE)
        create_table(conn, SQL_CREATE_TABLE)
        insert_into_table(conn, HEADERS)


if __name__ == '__main__':
    execute()
