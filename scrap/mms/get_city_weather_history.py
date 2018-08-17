# coding=utf-8
import os
import sys
import re
import getopt
import time
import requests
import pandas as pd
from functools import partial
from datetime import datetime, timedelta, date
from sqlalchemy import create_engine
# customer module
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from module import PrestoUtils


HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36 OPR/52.0.2871.40 (Edition Baidu)',
    'Connection': 'keep-alive'
}
SQL_DROP_TABLE = """
    drop table if exists ods_mms.city_weathe_history
"""
SQL_CREATE_TABLE = """
    create table if not exists ods_mms.city_weather_history (
        city_id           varchar,
        city_name         varchar,
        dates             varchar,
        weather_day       varchar,
        weather_day_id    varchar,
        weather_night     varchar,
        weather_night_id  varchar,
        tempreture_day    bigint,
        tempreture_night  bigint,
        wind_day          varchar,
        wind_comp_day     bigint,
        wind_night        varchar,
        wind_comp_night   bigint
    )
"""

pu = PrestoUtils(host='10.10.22.8', port='10300', user='prod', catalog='prod_hive')

def get_params_dict(ptype=None):
    presto_engin = create_engine('presto://prod@10.10.22.8:10300/prod_hive/ods_mms')
    con = presto_engin.connect()
    sql_city_init = """
        SELECT DISTINCT  wsc.city_id, si.city_name
        FROM cdm_base.store_info si
        LEFT JOIN ods_mms.weather_support_city wsc
        ON si.city_name = concat(wsc.city_name, '市')
        ORDER BY wsc.city_id
    """
    sql_city_remian = """
        SELECT DISTINCT city_id
        FROM ods_mms.city_weather_history
    """
    sql_city_continue = """
        SELECT city_id, max(dates) weather_date FROM ods_mms.city_weather_history
        GROUP BY city_id
        HAVING max(dates) < '{yesterday}'
    """.format(yesterday=(datetime.today().date() - timedelta(days=1)))
    sql_city_last = """
        SELECT max(dates) weather_date FROM ods_mms.city_weather_history
    """
    sql_check = """
        SELECT * FROM ods_mms.city_weather_history
        LIMIT 10
    """

    params_dict = {}
    partial_strftime = partial(datetime.strftime, format='%Y-%m-%d')

    flag = None

    if ptype in ('-c', "--continue"):
        flag = ptype
        ptype = "--update"

    if ptype in ('-i', "init"):
        if pd.read_sql_query(sql_check, con) is not None:
            raise Exception("table is not none! can't not initialize. use -h for help.")
            sys.exit(0)
        else:
            start_date = datetime(2016, 1, 1)
            end_date = datetime.today().date() - timedelta(days=1)

            city_id_list = pd.read_sql_query(sql_city_init, con)['city_id'].tolist()
            weather_date_list = list(map(partial_strftime, pd.date_range(start=start_date, end=end_date)))
            # print(city_id_list, weather_date_list)

            params_dict['ptype'] = ptype
            params_dict['city_id_list'] = city_id_list
            params_dict['weather_date_list'] = weather_date_list

    if ptype in ('-u', "update"):
        start_date = datetime(2016, 1, 1)
        end_date = datetime.today().date() - timedelta(days=1)
        last_date = datetime.strptime(pd.read_sql_query(sql_city_last, con)['weather_date'].values[0], '%Y-%m-%d') + timedelta(days=1)

        city_id_now_list = pd.read_sql_query(sql_city_init, con)['city_id'].tolist()
        city_id_remain_list = pd.read_sql_query(sql_city_remian, con)['city_id'].tolist()
        city_id_new_list = list(set(city_id_now_list).difference(set(city_id_remain_list)))
        weather_date_remain_list = list(map(partial_strftime, pd.date_range(start=last_date, end=end_date)))
        weather_date_new_list = list(map(partial_strftime, pd.date_range(start=start_date, end=end_date)))

        params_dict['ptype'] = ptype
        params_dict['city_id_remain_list'] = city_id_remain_list
        params_dict['city_id_new_list'] = city_id_new_list
        params_dict['weather_date_remain_list'] = weather_date_remain_list
        params_dict['weather_date_new_list'] = weather_date_new_list

        if flag in ('-c', "continue"):
            df_continue = pd.read_sql_query(sql_city_continue, con)

            start_date_continue = datetime.strptime(df_continue['weather_date'].values[0], '%Y-%m-%d') + timedelta(days=1)
            weather_date_continue_list = list(map(partial_strftime, pd.date_range(start=start_date_continue, end=end_date)))

            params_dict['ptype'] = flag
            params_dict['city_id_continue_list'] = df_continue['city_id'].tolist()
            params_dict['weather_date_continue_list'] = weather_date_continue_list

    else:
        raise Exception("ptype error! use -h for help")
    
    return params_dict


def get_values(response_json):
    if response_json['error_code'] == 0:
        result = response_json['result']
        values = (
            result['city_id'],
            result['city_name'],
            result['weather_date'],
            result['day_weather'],
            result['day_weather_id'],
            result['night_weather'],
            result['night_weather_id'],
            eval(re.sub(r'\D', '', result['day_temp']) if len(result['day_temp']) != 0 else re.sub(r'\D', '', result['night_temp'])),
            eval(re.sub(r'\D', '', result['night_temp']) if len(result['night_temp']) != 0 else re.sub(r'\D', '', result['day_temp'])),
            result['day_wind'],
            eval(re.sub(r'\D', '', result['day_wind_comp'])[0] if len(result['day_wind_comp']) != 0 else '0'),
            result['night_wind'],
            eval(re.sub(r'\D', '', result['night_wind_comp'])[0] if len(result['night_wind_comp']) != 0 else '0'),
        )

    else:
        values = None

    return values


def insert_into_table(conn, values_list):
    if len(values_list) != 0:
        values_batch = '{}'.format(values_list).strip('[').strip(']')
        sql = """
            insert into ods_mms.city_weather_history
            values {}
        """.format(values_batch)
        # print(sql)
        cur = conn.cursor()
        cur.execute(sql)
        time.sleep(5)
        # print(cur.fetchall())
    else:
        pass


def get_result(conn, params_dict):
    base_url = 'http://v.juhe.cn/historyWeather/weather'
    appkey = '7bf37125aec50d51a066f972e3618d21'
    params_dict = params_dict
    session = requests.session()
    values_list = []

    if params_dict['ptype'] is None or params_dict['ptype'] not in ('-i', "--init", '-u', "--update", '-c', "--continue"):
        raise Exception("ptype error! use -h for help")

    else:
        if params_dict['ptype'] in ('-i', "--init"):

            pu.drop_table(SQL_DROP_TABLE)
            pu.create_table(SQL_CREATE_TABLE)

            for city_id in params_dict['city_id_list']:

                for weather_date in params_dict['weather_date_list']:
                    suffix = """?city_id={city_id}&weather_date={weather_date}&key={appkey}
                    """.format(city_id=city_id, weather_date=weather_date, appkey=appkey)
                    request_url = base_url + suffix
                    response_json = session.get(url=request_url, headers=HEADERS).json()

                    values = get_values(response_json)

                    if values is not None:
                        values_list.append(values)
                    else:
                        pass

        elif params_dict['ptype'] in ('-u', "--update"):
            
            for city_id_remain in params_dict['city_id_remain_list']:

                for weather_date_remain in params_dict['weather_date_remain_list']:
                    suffix = """?city_id={city_id}&weather_date={weather_date}&key={appkey}
                    """.format(city_id=city_id_remain, weather_date=weather_date_remain, appkey=appkey)
                    request_url = base_url + suffix
                    response_json = session.get(url=request_url, headers=HEADERS).json()

                    values = get_values(response_json)

                    if values is not None:
                        values_list.append(values)
                    else:
                        pass

            for city_id_new in params_dict['city_id_new_list']:

                for weather_date_new in params_dict['weather_date_new_list']:
                    suffix = """?city_id={city_id}&weather_date={weather_date}&key={appkey}
                    """.format(city_id=city_id_new, weather_date=weather_date_new, appkey=appkey)
                    request_url = base_url + suffix
                    response_json = session.get(url=request_url, headers=HEADERS).json()

                    values = get_values(response_json)

                    if values is not None:
                        values_list.append(values)
                    else:
                        pass
        
        elif params_dict['ptype'] in ('-c', "--continue"):

            for city_id_continue in params_dict['city_id_continue_list']:

                for weather_date_continue in params_dict['weather_date_continue_list']:
                    suffix = """?city_id={city_id}&weather_date={weather_date}&key={appkey}
                    """.format(city_id=city_id_continue, weather_date=weather_date_continue, appkey=appkey)
                    request_url = base_url + suffix
                    response_json = session.get(url=request_url, headers=HEADERS).json()

                    values = get_values(response_json)

                    if values is not None:
                        values_list.append(values)
                    else:
                        pass

            for city_id_new in params_dict['city_id_new_list']:

                for weather_date_new in params_dict['weather_date_new_list']:
                    suffix = """?city_id={city_id}&weather_date={weather_date}&key={appkey}
                    """.format(city_id=city_id_new, weather_date=weather_date_new, appkey=appkey)
                    request_url = base_url + suffix
                    response_json = session.get(url=request_url, headers=HEADERS).json()

                    values = get_values(response_json)

                    if values is not None:
                        values_list.append(values)
                    else:
                        pass

        else:
            raise Exception("ptype error! use -h for help")

        insert_into_table(conn, values_list)


def execute(argv):
    message = """
    Usage: python36 get_city_weather_history.py <command>
    Commands:
    [-h | --help]      Show help message.
    [-i | --init]      Initialize table. use in the first time. Do not use it after the first init was completed!!!
    [-c | --continue]  While initialize table with [-i | --init] was intrupted, use this command to continue.
    [-u | --update]    Update table. Use this command after first init.
    """
    try:
        opts, _ = getopt.getopt(argv, "hiuc", ["help", "init", "update", "continue"])
    except getopt.GetoptError:
        print("Command not found. Use -h for help.")
        sys.exit(2)
    else:
        if len(opts) < 1:
            raise Exception("Command not found. Use -h for help.")
        for opt, _ in opts:
            if opt in ('-h', "--help"):
                print(message)
            elif opt in ('-i', "--init", '-c', "--continue", '-u', "--update"):
                params_dict = get_params_dict(ptype=opt)
                get_result(pu.presto_conn, params_dict)
            else:
                raise Exception("Command not found. Use -h for help.")


if __name__ == '__main__':
    execute(sys.argv[1:])
