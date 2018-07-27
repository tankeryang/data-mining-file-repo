"""
Notice: 此脚本已弃用，替代脚本为get_city_weather_history.py
author: yang
date: 2018-05-12
"""


# import sys
# import os
# import time
# import pandas as pd
# import requests
# from pypinyin import lazy_pinyin
# from bs4 import BeautifulSoup

# sys.path.append(
#     os.path.abspath(
#         os.path.join(os.getcwd(), '..', '..')
#     )
# )
# from module.utils import get_prestodb_conn


# headers = {
#     'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 \
#     (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36 OPR/52.0.2871.40 (Edition Baidu)',
#     'Connection': 'keep-alive'
# }
# year_list = ['2017', '2018']
# month_list = ['4', '5']

# host = 'fp-bd8'
# port = '10300'
# user = 'prod'
# catalog = 'prod_hive'
# schema = 'test_'


# def get_city_name_list(conn):
#     city_name_list = []
#     sql = """
#         SELECT 
#             DISTINCT city_name
#         FROM
#             test_.city_weather
#     """
#     cur = conn.cursor()
#     cur.execute(sql)
#     for city_name in cur.fetchall():
#         city_name_list.append(city_name[0])
#     return city_name_list


# def get_remain_city_name_list(conn):
#     remain_city_name_list = []
#     sql = """
#         SELECT
#             DISTINCT city_name
#         FROM
#             test_.city_weather
#     """
#     cur = conn.cursor()
#     cur.execute(sql)
#     for city_name in cur.fetchall():
#         remain_city_name_list.append(city_name[0].split('市')[0])

#     print(len(remain_city_name_list))
#     return remain_city_name_list


# def get_not_in_city_name_list(city_name_list, remain_city_name_list):
#     not_in_city_name_list = list(set(city_name_list).difference(set(remain_city_name_list)))
#     print(len(not_in_city_name_list))
#     return not_in_city_name_list


# def get_city_weather_url_dict(city_name_list, year_list, month_list=None):
#     home_url = 'http://www.tianqihoubao.com/lishi/'
#     city_weather_url_dict = {}

#     for city_name in city_name_list:
#         city_name_pinyin = ''
#         city_weather_url_list = []

#         for fen_ci in lazy_pinyin(city_name):
#             city_name_pinyin += fen_ci
#             if city_name_pinyin == 'dongguan':
#                 city_name_pinyin = 'dongguang'

#         if month_list == None:
#             for year_id in year_list:
#                 for month_id in range(1, 10):
#                     city_weather_url_list.append(
#                         home_url + '{}/month/{}0{}.html'.format(city_name_pinyin, year_id, month_id)
#                     )
#                 for month_id in range(10, 13):
#                     city_weather_url_list.append(
#                         home_url + '{}/month/{}{}.html'.format(city_name_pinyin, year_id, month_id)
#                     )
#         else:
#             for year_id in year_list:
#                 for month_id in month_list:
#                     city_weather_url_list.append(
#                         home_url + '{}/month/{}0{}.html'.format(city_name_pinyin, year_id, month_id)
#                     )
#         city_weather_url_dict[city_name] = city_weather_url_list

#     return city_weather_url_dict


# def drop_table_if_exist(conn):
#     sql = """
#         DROP TABLE IF EXISTS ods_test_.city_weather
#     """
#     print(sql)
#     cur = conn.cursor()
#     cur.execute(sql)
#     time.sleep(5)
#     print('drop success!\n', 50*'-')


# def create_table(conn):
#     sql = """
#         CREATE TABLE IF NOT EXISTS ods_test_.city_weather (
#             city_name        VARCHAR,
#             dates            VARCHAR,
#             weather_day      VARCHAR,
#             weather_night    VARCHAR,
#             tempreture_day   INTEGER,
#             tempreture_night INTEGER
#         )
#     """
#     print(sql)
#     cur = conn.cursor()
#     cur.execute(sql)
#     time.sleep(5)
#     print('create success!\n', 50*'-')


# def insert_into_table(conn, city_weather_url_dict, headers=headers):
#     sess = requests.session()

#     for city_name in city_weather_url_dict:
#         city_weather_dict = {}
#         city_weather_dict['city_name'] = city_name + '市'

#         for city_weather_url in city_weather_url_dict[city_name]:
#             html = sess.get(url=city_weather_url, headers=headers)

#             if html.status_code == 302:
#                 break
#             else:
#                 soup = BeautifulSoup(html.text, 'lxml')

#                 for tag_tr in soup.select('table.b > tr')[1:]:

#                     for num, tag_td in enumerate(tag_tr.find_all(name='td')[:-1]):
#                         if num == 0:
#                             dates = tag_td.find(name='a').text
#                             year = dates.split('年')[0].strip()
#                             month = dates.split('年')[1].split('月')[0].strip()
#                             day = dates.split('月')[1].strip().strip('日')
#                             city_weather_dict['dates'] = year + '-' + month + '-' + day

#                         if num == 1:
#                             if tag_td.text.split('/')[0].strip() != '' and tag_td.text.split('/')[0].strip() != '-':
#                                 city_weather_dict['weather_day'] = tag_td.text.split('/')[0].strip()
#                             else:
#                                 city_weather_dict['weather_day'] = tag_td.text.split('/')[1].strip()

#                             if tag_td.text.split('/')[1].strip() != '' and tag_td.text.split('/')[1].strip() != '-':
#                                 city_weather_dict['weather_night'] = tag_td.text.split('/')[1].strip()
#                             else:
#                                 city_weather_dict['weather_night'] = tag_td.text.split('/')[0].strip()

#                         if num == 2:
#                             if tag_td.text.split('/')[0].strip() != '':
#                                 city_weather_dict['tempreture_day'] = int(
#                                     tag_td.text.split('/')[0].strip().replace('℃', '')
#                                 )
#                             else:
#                                 city_weather_dict['tempreture_day'] = int(
#                                     tag_td.text.split('/')[1].strip().replace('℃', '')
#                                 )

#                             if tag_td.text.split('/')[1].strip() != '':
#                                 city_weather_dict['tempreture_night'] = int(
#                                     tag_td.text.split('/')[1].strip().replace('℃', '')
#                                 )
#                             else:
#                                 city_weather_dict['tempreture_night'] = int(
#                                     tag_td.text.split('/')[0].strip().replace('℃', '')
#                                 )

#                     sql = """
#                         INSERT INTO ods_test_.city_weather
#                         VALUES {}
#                     """.format(
#                         (
#                             city_weather_dict['city_name'],
#                             city_weather_dict['dates'],
#                             city_weather_dict['weather_day'],
#                             city_weather_dict['weather_night'],
#                             city_weather_dict['tempreture_day'],
#                             city_weather_dict['tempreture_night']
#                         )
#                     )
#                     cur = conn.cursor()
#                     cur.execute(sql)
#                     print('insert {}'.format(
#                             (
#                                 city_weather_dict['city_name'],
#                                 city_weather_dict['dates'],
#                                 city_weather_dict['weather_day'],
#                                 city_weather_dict['weather_night'],
#                                 city_weather_dict['tempreture_day'],
#                                 city_weather_dict['tempreture_night']
#                             )
#                         )
#                     )
#                     time.sleep(0.3)
#                 time.sleep(5)


# def execute_start():
#     conn = get_prestodb_conn()
#     city_name_list = get_city_name_list(conn)
#     city_weather_url_dict = get_city_weather_url_dict(city_name_list, year_list)

#     drop_table_if_exist(conn)
#     create_table(conn)
#     insert_into_table(conn, city_weather_url_dict)


# def execute_continue():
#     conn = get_prestodb_conn()

#     city_name_list = get_city_name_list(conn)
#     remain_city_name_list = get_remain_city_name_list(conn)
#     not_in_city_name_list = get_not_in_city_name_list(city_name_list, remain_city_name_list)
#     not_in_city_weather_url_dict = get_city_weather_url_dict(not_in_city_name_list, year_list)
    
#     insert_into_table(conn, not_in_city_weather_url_dict)


# def execute_add():
#     conn = get_prestodb_conn()
#     # city_name_list = get_remain_city_name_list(conn)
#     city_name_list = ['东营']
#     city_weather_url_dict = get_city_weather_url_dict(city_name_list, year_list)
#     insert_into_table(conn, city_weather_url_dict)    


# if __name__ == '__main__':
#     # execute_start()
#     execute_add()
#     # execute_continue()
