"""
Created on 2017年12月9日

@author: Colin
"""

import datetime
import math
import logging
import logging.config
import re


# china stock shanghai
startSH = 600001
MAXSH = 603999
startSZ = 1
MAXSZ = 2909
startCY = 300001
MAXCY = 300710
STEP = 1
LASTDAYS = 10
INCR_VOL = 5
PERCENTBOUND = 9.97
VOLATILITY = 0.15

HIGHERTHANUP = 0
BETWEENUPMID = 1
LITTLEHIGERTHANMID = 2
LITTLELESSTHANMID = 3
BETWEENMIDDOWN = 4
LITTLEHIGERTHANDOWN = 5
LITTLELESSTHANDOWN = 6

SELL = 0
BUY = 1

BOLL_DAY = 13

TREND_UP = 1
TREND_DOWN = -1

OPEN_SALE = 1
CLOSE_SALE = 0


logging.config.fileConfig('logging.conf')
logger = logging.getLogger(__name__)


def print_progess(cur_counter, total):
    cur_percent = round(cur_counter / total * 100, 4)
    print('%d%%' % cur_percent, end="", flush=True)
    print('...', end="", flush=True)


'''
公共方法，进行深圳代码转制。
e.g: 1 -->  000001
'''


def change_sz_code(share_code):
    sz_code = 0
    # if(share_code <= MAXSZ):
    if share_code < 10:
        sz_code = str("00000") + str(share_code)
    elif share_code < 100:
        sz_code = str("0000") + str(share_code)
    elif share_code < 1000:
        sz_code = str("000") + str(share_code)
    elif share_code < 10000:
        sz_code = str("00") + str(share_code)
    return sz_code


'''
  获取当前日期
'''


def get_current_date():
    current_date = datetime.datetime.now().strftime('%Y-%m-%d')
    return current_date


'''
  返回下一天日期
'''


def get_next_day(cur_date):
    d1 = datetime.datetime.strptime(cur_date, '%Y-%m-%d')
    delta_d = datetime.timedelta(days=1)
    ndays = d1 + delta_d
    return ndays.strftime('%Y-%m-%d')


"""
   返回指定日期前n天的日期
"""


def get_front_day(cur_date, front_days):
    d1 = datetime.datetime.strptime(cur_date, '%Y-%m-%d')
    delta_d = datetime.timedelta(days=front_days)
    ndays = d1 - delta_d
    return ndays.strftime('%Y-%m-%d')


"""
   返回当前日期前n天的日期
"""


def get_current_front_day(front_days):
    d1 = get_current_date()
    return get_front_day(d1, front_days)


"""
    返回当前日期下一年的日期
"""


def get_next_year(cur_date):
    d1 = datetime.datetime.strptime(cur_date, '%Y-%m-%d')
    delta_d = datetime.timedelta(days=365)
    ndays = d1 + delta_d
    logging.debug("ndays:::::::::" + str(ndays.strftime('%Y-%m-%d')))
    return ndays.strftime('%Y-%m-%d')


'''
    返回当前日期前一年的日期
'''


def get_pre_year(cur_date):
    d1 = datetime.datetime.strptime(cur_date, '%Y-%m-%d')
    delta_d = datetime.timedelta(days=365)
    ndays = d1 - delta_d
    logging.debug("ndays:::::::::" + str(ndays.strftime('%Y-%m-%d')))
    return ndays.strftime('%Y-%m-%d')


'''
    返回当前日期后三年的日期
'''


def get_next_3years(cur_date):
    d1 = datetime.datetime.strptime(cur_date, '%Y-%m-%d')
    delta_d = datetime.timedelta(days=365*3)
    ndays = d1 + delta_d
    logging.debug("ndays:::::::::" + str(ndays.strftime('%Y-%m-%d')))
    return ndays.strftime('%Y-%m-%d')


'''
    刷新日期格式
    19990101 --> 1999-01-01
'''


def change_date_format(cur_date):
    year = str(cur_date)[0:4]
    month = str(cur_date)[4:6]
    day = str(cur_date)[6:8]
    logger.debug('year=' + year + 'month=' + month + 'day=' + day)
    result = datetime.date(int(year), int(month), int(day))
    # result = tmp_date
    # result=datetime.datetime.strptime(str(datetime.date(int(year),int(month),int(day))),'%Y-%m-%d')
    # logger.debug('result='+str(result))
    return result


'''
    计算标准差
'''


def cal_std_dev(list_to_cal):
    cur_size = len(list_to_cal)
    counter = 0
    result = 0
    while counter < cur_size:
        tmp = list_to_cal[counter]
        result += math.pow(tmp, 2)
        counter += 1
    return math.sqrt(result / cur_size)

# get_next_year('2008-09-01')

# change_date_format('19980101')
# change_date_format('20111028')


'''
判断股票代码是哪个市场的。沪市，深市，创业板
'''


def check_market(share_code):
    reg_sh = '^600'
    reg_sz = '^00'
    reg_cy = '^300'
    result = None
    if re.match(reg_sh, share_code) is not None:
        logger.info("share_code: "+ share_code + " is shanghai")
        result = 'sh'
    elif re.match(reg_sz, share_code) is not None:
        logger.info("share_code: "+ share_code + " is shenzhen")
        result = 'sz'
    elif re.match(reg_cy, share_code) is not None:
        logger.info("share_code: "+ share_code + " is chuangye")
        result = 'cyb'
    return result