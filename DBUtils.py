"""
Created on 2017年12月9日

@author: Colin
"""


import time

import pandas

import logging.config

from colin.chen.shares import DataSource
from colin.chen.shares import Utils
from sqlalchemy import create_engine


'''logger配置'''
logging.config.fileConfig('logging.conf')
logger = logging.getLogger(__name__)

'''
   数据库连接
'''


def db_connection():
    engine = create_engine('mysql+pymysql://root:aaa@127.0.0.1/shares?charset=utf8mb4')
    return engine


def change_to(str_date):
    time_array = time.strptime(str_date, "%Y-%m-%d")
    # 转换成新的时间格式(20160505-20:28:54)
    dt_new = time.strftime("%Y%m%d", time_array)
    return int(dt_new)


'''
  各股K綫数据库保存
'''


def db_save(engine, code, start_date, end_date, table):
    items_rs = DataSource.get_data_from_tu_share(code, start_date, end_date)
    if items_rs is None:
        logger.warning(code + " result is null")
        return
#     print(itemsRs)
#     rsNew=itemsRs.copy()
#     itemsRs[u'date']=rsNew[u'date'].map(changeTo)
    items_rs.to_sql(table, engine, if_exists='append')
    

'''
  指數K綫數據庫保存
'''


def index_save(engine, code, start_date, end_date):
    item_rs = DataSource.get_index_from_tu_share(code, start_date, end_date)
    if item_rs is None:
        return
    item_rs.to_sql("index_k_his_data", engine, if_exists='append')


'''
  行业分类数据保存，如果存在就替换
'''


def industry_classify_save(engine):
    item_rs = DataSource.get_industry_classify()
#     print(item_rs)
    item_rs.to_sql("industry_classify_data", engine, if_exists='replace')


'''
    保存财报数据基本信息
'''


def save_share_report(engine, year, quarter):
    item_rs = DataSource.get_report_data_quarterly(year, quarter)
    if item_rs is None:
        logger.error("report_data api no data returned!!! year: " + str(year) + "quarter: " + quarter)
        return
    item_rs.to_sql("share_report_basic", engine, if_exists='append')


'''
    获取财务报表利润信息
'''


def save_share_report_profit(engine, year, quarter):
    item_rs = DataSource.get_report_profit_quarterly(year, quarter)
    if item_rs is None:
        logger.error("profit api no data returned!!! year: " + str(year) + "quarter: " + quarter)
        return
    item_rs.to_sql("share_report_profit", engine, if_exists='append')


'''
    获取财务报表运营能力信息
'''


def save_share_report_operation(engine, year, quarter):
    item_rs = DataSource.get_report_operation_quarterly(year, quarter)
    if item_rs is None:
        logger.error("operation api no data returned!!! year: " + str(year) + "quarter: " + quarter)
        return
    item_rs.to_sql("share_report_operation", engine, if_exists='append')


'''
    获取财务报表成长能力信息
'''


def save_share_report_growth(engine, year, quarter):
    item_rs = DataSource.get_report_growth_quarterly(year, quarter)
    if item_rs is None:
        logger.error("growth api no data returned!!! year: " + str(year) + "quarter: " + quarter)
        return
    item_rs.to_sql("share_report_growth", engine, if_exists='append')


'''
    获取财务报表偿债信息
'''


def save_share_report_debt(engine, year, quarter):
    item_rs = DataSource.get_report_debt_quarterly(year, quarter)
    if item_rs is None:
        logger.error("debt api no data returned!!! year: " + str(year) + "quarter: " + quarter)
        return
    item_rs.to_sql("share_report_debt", engine, if_exists='append')


'''
    获取财务报表现金流信息
'''


def save_share_report_cash_flow(engine, year, quarter):
    item_rs = DataSource.get_report_cash_flow_quarterly(year, quarter)
    if item_rs is None:
        return
    item_rs.to_sql('share_report_cash_flow', engine, if_exists='append')


'''
  读取k_his_data表数据，从startDate开始到当前时间
'''


def db_read(code, start_date, engine):
    rs = db_read_k_history(code, engine, start_date, Utils.get_current_date(), 'asc')
    return rs


'''
  按照时间升序读取k_his_data表数据
'''


def db_read_k_history_inc(code, engine, start_date, end_date):
    return db_read_k_history(code, engine, start_date, end_date, "asc")


'''
  按照时间降序读取k_his_data表数据
'''


def db_read_k_history_des(code, engine, start_date, end_date):
    return db_read_k_history(code, engine, start_date, end_date, "desc")


'''
   读取k_his_data表数据，从startDate开始到endDate时间结束
   
   2018-01-02 修改获取日期范围，从>startDate修改为>=startDate
'''


def db_read_k_history(code, engine, start_date, end_date, is_inc):
    sql = "select * from shares.k_his_data where k_his_data.shareCode='" + code + "'" + " and date >= '" + \
          start_date + "' and date <= '" + end_date + "' order by date " + is_inc
    # print(sql)
    rs = pandas.read_sql_query(sql, engine)
    return rs


def db_read_k_index(code, engine):
    sql = "select * from shares.index_k_his_data where index_k_his_data.shareCode='" + code + "' order by date asc"
    rs = pandas.read_sql_query(sql, engine)
    return rs


'''
获取时间短内大盘指数波动情况
'''


def db_read_index_history(code, engine, start_date, end_date):
    sql = "select * from shares.index_k_his_data where index_k_his_data.shareCode='" + code + "'" + " and date >= '" + \
          start_date + "' and date <= '" + end_date + "' order by date asc"
    rs = pandas.read_sql_query(sql, engine)
    return rs


'''
    读取基本面偿债能力数据
'''


def db_read_debt_info(code, engine, start_year, end_year, quarter):
    if quarter == 1:
        str_quarter = 'quarter < 4 '
    elif quarter == 2:
        str_quarter = 'quarter in (2,4) '
    else:
        str_quarter = 'quarter = 4 '

    sql = "select * from shares.share_report_debt as debt where debt.code = '" + str(code) + "' and " + str_quarter + \
          " and year >= " + str(start_year) + " and year <= " + str(end_year) + " order by year asc"
    logger.debug(sql)
    rs = pandas.read_sql_query(sql, engine)
    return rs


'''
保存A股全部日K线数据，从startDate日期开始，到endDate结束
'''


def save_data(begin, end, step, start_date, end_date):
    counter = 0
    cur_total = end - begin
    print("start db connection")
    engine = db_connection()
    print("end db connection")
    while begin <= end:
        if begin <= Utils.MAXSZ:
            sz_special = Utils.change_sz_code(begin)
            start = sz_special
        else:
            start = str(begin)
        db_save(engine, start, start_date, end_date, 'k_his_data')
        begin = begin + step
        counter = counter + 1
        if counter % 100 == 0:
            Utils.print_progess(counter, cur_total)


'''
保存A股全部日K线数据，从startDate日期开始，到当前时间结束
'''


def save_data_from_now_on(begin, end, step, start_date):
    save_data(begin, end, step, start_date, Utils.get_current_date())


'''
保存A股全部日K先数据，获取当天的数据
'''


def save_data_current_day(begin, end, step):
    save_data_from_now_on(begin, end, step, Utils.get_current_date())


'''
  保存指數K綫數據，從begin開始到當前數據
'''


def save_index_data(begin_date):
    save_index_data_end_date(begin_date, Utils.get_current_date())


'''
保存指數K綫數據，從begin開始到end結束
'''


def save_index_data_end_date(begin_date, end_date):
    indexs = ['sh', 'sz', 'hs300', 'sz50', 'zxb', 'cyb']
    engine = db_connection()
    for currentIndex in indexs:
        index_save(engine, currentIndex, begin_date, end_date)


'''
   保存行业分类
'''


def save_industry_index():
    engine = db_connection()
    industry_classify_save(engine)


'''
   保存股票基本信息
   需要周期更新，建议每季度更新一次。
   最后一次更新时间：2018-01-07
'''


def save_stock_basics(engine):
    item_rs = DataSource.get_stock_basics()
    item_rs.to_sql("stock_basics_info", engine, if_exists='append')
    # itemRs.to_csv('/Users/chenke/Documents/market/basic.csv')


'''
    获取股票基本信息
'''


def get_stock_basics(engine):
    sql = "select * from shares.stock_basics_info where timeToMarket !=0 order by timeToMarket asc "
    rs = pandas.read_sql_query(sql, engine)
    return rs


'''
    保存股票上市到现在到复权数据
'''


def save_stock_h_data(engine, code, start_date, end_date):
    item_rs = DataSource.get_h_data_qfq(code, start_date, end_date)
    # print("save_stock_h_data:::::::::")
    # print(item_rs)
    if item_rs is not None:
        item_rs.to_sql('stock_h_data_qfq', engine, if_exists='append')
    else:
        logger.info('get_h_data_qfq result item_rs is null!!!!!!!!')


'''
    获取交易日历
'''


def save_trad_cal(engine):
    item_rs = DataSource.get_trade_cal()
    if item_rs is not None:
        item_rs.to_sql('trad_cal', engine, if_exists='replace')
    else:
        logger.info('save_trad_cal result item_rs is null!!!!!!!!')


'''
    读取交易日历
'''


def read_trad_cal(engine):
    sql = "select * from shares.trad_cal"
    # print(sql)
    rs = pandas.read_sql_query(sql, engine)
    return rs
