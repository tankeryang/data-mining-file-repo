import os
import sys
import time
import requests
from tqdm import tqdm

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
)
from module.utils import get_prestodb_conn, create_table, drop_table


HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36 OPR/52.0.2871.40 (Edition Baidu)',
    'Connection': 'keep-alive'
}
SQL_DROP_TABLE = """
    drop table if exists ods_mms.weather_support_city
"""
SQL_CREATE_TABLE = """
    create table if not exists ods_mms.weather_support_city (
        province_id  varchar,
        city_id      varchar,
        city_name    varchar
    )
"""


def insert_into_table(conn, headers):
    base_url = 'http://v.juhe.cn/historyWeather/citys'
    appkey = '7bf37125aec50d51a066f972e3618d21'
    params_list = list(map(str, range(1, 35)))
    headers = headers
    session = requests.session()

    for params in tqdm(params_list):
        request_url = base_url + '?province_id={params}&key={appkey}'.format(params=params, appkey=appkey)
        response_json = session.get(url=request_url, headers=headers).json()

        if response_json['reason'] == '查询成功':
            result_list = response_json['result']
            for result in tqdm(result_list):
                values = (result['province_id'], result['id'], result['city_name'])
                sql = """
                    insert into ods_mms.weather_support_city
                    values {}
                """.format(values)
                # print(sql)
                cur = conn.cursor()
                cur.execute(sql)
                time.sleep(0.2)
        else:
            print(response_json['reason'])
        
    print('insert success!')


if __name__ == '__main__':
    with get_prestodb_conn() as conn:
        drop_table(conn, SQL_DROP_TABLE)
        create_table(conn, SQL_CREATE_TABLE)
        insert_into_table(conn, HEADERS)
