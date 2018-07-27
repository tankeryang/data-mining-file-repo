import pandas as pd
from datetime import date, datetime
from sqlalchemy import create_engine

def select_sp_day(week):
    if eval(week) >= 4 and eval(week) <= 8:
        return 'NEW_YEAR'
    elif eval(week) >= 16 and eval(week) <= 18:
        return '5_1'
    elif eval(week) >= 37 and eval(week) <= 42:
        return '10_1'
    elif eval(week) >= 43 and eval(week) <= 47:
        return '11_11'
    elif eval(week) >= 48 and eval(week) <= 52:
        return '12_12'
    else:
        return 'NORMAL'


def execute():
    start_date = date(2016, 1, 1)
    end_date = date(2019, 12, 31)
    presto_engine = create_engine('presto://prod@10.10.22.8:10300/prod_hive/ods_mms')
    con = presto_engine.connect()

    date_list = [d.strftime('%Y-%m-%d') for d in pd.date_range(start=start_date, end=end_date).tolist()]
    year_list = [str(d.year) for d in pd.date_range(start=start_date, end=end_date).tolist()]
    month_list = [str(d.month) for d in pd.date_range(start=start_date, end=end_date).tolist()] 
    week_list = [str(d.week) for d in pd.date_range(start=start_date, end=end_date).tolist()]
    special_day_list = list(map(select_sp_day, week_list))
    # print(special_day_list)
    data = {
        'dates': date_list,
        'year_code': year_list,
        'month_code': month_list,
        'week_code': week_list,
        'special_day_type': special_day_list
    }
    df_sp_day = pd.DataFrame(data=data)

    df_sp_day.to_sql(name='special_day_info', con=con, schema='ods_mms', if_exists='append', index=False)

if __name__ == '__main__':
    execute()
