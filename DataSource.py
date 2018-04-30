"""
Created on 2017年12月9日

@author: Colin
"""

import logging.config

from colin.chen.shares import Utils


import tushare as ts

import pandas as pd

'''logger配置'''
logging.config.fileConfig('logging.conf')
logger = logging.getLogger(__name__)

'''
getDataFromTuShare(currentCode,startDate, endDate)
获取数据接口，获取currentCode的起始日期到结束日期的日K线数据
        open    high   close     low     volume    p_change  ma5 \
date
2012-01-11   6.880   7.380   7.060   6.880   14129.96     2.62   7.060
2012-01-12   7.050   7.100   6.980   6.900    7895.19    -1.13   7.020
2012-01-13   6.950   7.000   6.700   6.690    6611.87    -4.01   6.913
2012-01-16   6.680   6.750   6.510   6.480    2941.63    -2.84   6.813
2012-01-17   6.660   6.880   6.860   6.460    8642.57     5.38   6.822
2012-01-18   7.000   7.300   6.890   6.880   13075.40     0.44   6.788
2012-01-19   6.690   6.950   6.890   6.680    6117.32     0.00   6.770
2012-01-20   6.870   7.080   7.010   6.870    6813.09     1.74   6.832

             ma10    ma20      v_ma5     v_ma10     v_ma20     turnover
date
2012-01-11   7.060   7.060   14129.96   14129.96   14129.96     0.48
2012-01-12   7.020   7.020   11012.58   11012.58   11012.58     0.27
2012-01-13   6.913   6.913    9545.67    9545.67    9545.67     0.23
2012-01-16   6.813   6.813    7894.66    7894.66    7894.66     0.10
2012-01-17   6.822   6.822    8044.24    8044.24    8044.24     0.30
2012-01-18   6.833   6.833    7833.33    8882.77    8882.77     0.45
2012-01-19   6.841   6.841    7477.76    8487.71    8487.71     0.21
2012-01-20   6.863   6.863    7518.00    8278.38    8278.38     0.23
'''


def get_data_from_tu_share(current_code, start_date, end_date):
    # items_rs=None
    # try:
    items_rs = ts.get_hist_data(current_code, start=start_date, end=end_date)
    # except IOError:
    #     print(currentCode + " time out!!!!")
    # except :
    #     print(currentCode + " else time out!!!!")
    '''api返回值没有股票代码，加上'''
    if items_rs is not None:
        add_column_share_code(items_rs, current_code)
    #     print(type(itemsRs))
    return items_rs


'''
   獲取指數
'''


def get_index_from_tu_share(code, start_date, end_date):
    items_rs = ts.get_hist_data(code, start_date, end_date)
    new_df = items_rs
    if items_rs is not None:
        new_df = add_column_share_code(items_rs, code)
    return new_df


'''
   获取行业分类
'''


def get_industry_classify():
    item_rs = ts.get_industry_classified()
    return item_rs


'''
  #api返回值没有股票代码，在这里增加上
'''


def add_column_share_code(items_rs, current_code):
    items_rs['shareCode'] = str(current_code)
    #     print(itemsRs)
    return items_rs


'''
    #给api返回值增加数据时间戳
'''


def add_column_date(items_rs, year, quarter):
    items_rs['year'] = str(year)
    items_rs['quarter'] = str(quarter)
    return items_rs


'''
    获取股票基本信息
'''


def get_stock_basics():
    basics = pd.DataFrame(ts.get_stock_basics())
    '''更改timeToMarket格式为2018-01-01'''
    time_to_market_list = list(basics[u'timeToMarket'])
    begin = 0
    end = len(time_to_market_list)
    to_replace_list = []
    while begin < end:
        orign_date = str(time_to_market_list[begin])
        # old_date=time_to_market_list[begin]
        logger.debug("old_date:::::::" + str(orign_date))
        begin = begin + 1
        if orign_date == '0':
            to_replace_list.append('0')
            continue
        convert_date = Utils.change_date_format(orign_date)
        to_replace_list.append(convert_date)
    basics['timeToMarketNew'] = to_replace_list

    return basics


'''
    获取股票复权数据
    默认前复权
    autype='hfq' 是后复权
    autype='None' 不复权
    start 开始时间
    end 结束时间
'''


def get_h_data_qfq(code, start_date, end_date):
    if start_date is None and end_date is None:
        logger.warning("There is no start and end time")
        return None
    logger.info('get_h_data start=' + start_date + 'end=' + end_date)
    result = None
    try:
        result = ts.get_h_data(code, start=start_date, end=end_date, retry_count=3)
        '''增加股票代码'''
        add_column_share_code(result, code)
        # print('get_h_data_qfq')
        # print(result)
    except Exception:
        logger.error('exception happened!!!!!!!!!!code=' + str(code) + 'start_date=' +
                     str(start_date) + 'end_date=' + str(end_date))
    # ts.get_h_data('002337', start='2015-01-01', end='2015-03-16')  # 两个日期之间的前复权数据
    # logger.debug('result:::::::::::'+str(result))
    return result


'''
   获取交易日历
'''


def get_trade_cal():
    result = ts.trade_cal()
    # print(result)
    return result


'''
按季度获取报表基础信息
quarter = 1,2,3,4
1 一季度
2 半年报
3 三季度
4 年报

code,代码
name,名称
esp,每股收益
eps_yoy,每股收益同比(%)
bvps,每股净资产
roe,净资产收益率(%)
epcf,每股现金流量(元)
net_profits,净利润(万元)
profits_yoy,净利润同比(%)
distrib,分配方案
report_date,发布日期
'''


def get_report_data_quarterly(year, quarter):
    result = ts.get_report_data(year, quarter)
    if result is not None:
        add_column_date(result, year, quarter)
    return result


'''
按季度获取报表盈利信息
quarter = 1,2,3,4
1 一季度
2 半年报
3 三季度
4 年报


code,代码
name,名称
roe,净资产收益率(%)
net_profit_ratio,净利率(%)
gross_profit_rate,毛利率(%)
net_profits,净利润(万元)
esp,每股收益
business_income,营业收入(百万元)
bips,每股主营业务收入(元)
'''


def get_report_profit_quarterly(year, quarter):
    result = ts.get_profit_data(year, quarter)
    if result is not None:
        add_column_date(result, year, quarter)
    return result


'''
按季度获取报表运营能力信息
quarter = 1,2,3,4
1 一季度
2 半年报
3 三季度
4 年报

code,代码
name,名称
arturnover,应收账款周转率(次)
arturndays,应收账款周转天数(天)
inventory_turnover,存货周转率(次)
inventory_days,存货周转天数(天)
currentasset_turnover,流动资产周转率(次)
currentasset_days,流动资产周转天数(天)
'''


def get_report_operation_quarterly(year, quarter):
    result = ts.get_operation_data(year, quarter)
    if result is not None:
        add_column_date(result, year, quarter)
    return result


'''
按季度获取报表成长能力信息
quarter = 1,2,3,4
1 一季度
2 半年报
3 三季度
4 年报

code,代码
name,名称
mbrg,主营业务收入增长率(%)
nprg,净利润增长率(%)
nav,净资产增长率
targ,总资产增长率
epsg,每股收益增长率
seg,股东权益增长率
'''


def get_report_growth_quarterly(year, quarter):
    result = ts.get_growth_data(year, quarter)
    if result is not None:
        add_column_date(result, year, quarter)
    return result



'''
按季度获取报表偿债信息
quarter = 1,2,3,4
1 一季度
2 半年报
3 三季度
4 年报

code,代码
name,名称
currentratio,流动比率
quickratio,速动比率
cashratio,现金比率
icratio,利息支付倍数
sheqratio,股东权益比率
adratio,股东权益增长率
'''


def get_report_debt_quarterly(year, quarter):
    result = ts.get_debtpaying_data(year, quarter)
    if result is not None:
        add_column_date(result, year, quarter)
    return result


'''
按季度获取报表现金流信息
quarter = 1,2,3,4
1 一季度
2 半年报
3 三季度
4 年报

code,代码
name,名称
cf_sales,经营现金净流量对销售收入比率
rateofreturn,资产的经营现金流量回报率
cf_nm,经营现金净流量与净利润的比率
cf_liabilities,经营现金净流量对负债比率
cashflowratio,现金流量比率
'''


def get_report_cash_flow_quarterly(year, quarter):
    result = ts.get_cashflow_data(year, quarter)
    if result is not None:
        add_column_date(result, year, quarter)
    return result

# code='000012'
# start='2017-01-08'
# end='2018-01-07'
# get_h_data_qfq(code,start,end)

# get_trade_cal()
