"""
Created on 2017年10月7日

@author: Colin
"""

import threading
import datetime
import os
import pandas
import matplotlib.pyplot as plt
import logging.config
import math
from colin.chen.shares import Utils
from colin.chen.shares import DBUtils
from colin.chen.shares import DataAnalysis


'''logger配置'''
logging.config.fileConfig('logging.conf')
logger = logging.getLogger(__name__)

'''
   调用主函数 
'''

SAVE_FILE_PATH='/Users/chenke/Documents/market/'


def checkStockVolumn(begin, end, step, startDate, endDate):
    counter = 0
    result = []
    start = 0
    curTotal = end - begin
    engine = DBUtils.db_connection()
    while begin <= end:
        if (begin <= Utils.MAXSZ):
            szSpecial = Utils.change_sz_code(begin)
            start = szSpecial
        else:
            start = str(begin)
        currentRs = DBUtils.db_read_k_history_inc(start, engine, startDate, endDate)
        tmpResult = DataAnalysis.check_by_vols(start, currentRs)
        if tmpResult is not None:
            result.append(tmpResult)
        begin = begin + step
        counter = counter + 1
        if (counter % 100 == 0):
            Utils.print_progess(counter, curTotal)
    print(result)
    result_frame = pandas.DataFrame(result)

    filename=SAVE_FILE_PATH + 'from ' + startDate + 'to ' + endDate
    if os.path.exists(filename):
        result_frame.to_csv(filename,mode='a',header='None')
    else:
        result_frame.to_csv(filename)
    return result


'''
 执行过程
'''


def do_save(begin_date, finish_date):
    threads = []
    current_date = begin_date
    end_date = finish_date
    t1 = threading.Thread(target=DBUtils.save_data, args=(Utils.startSH, Utils.MAXSH, Utils.STEP, current_date, end_date))
    threads.append(t1)
    t2 = threading.Thread(target=DBUtils.save_data, args=(Utils.startSZ, Utils.MAXSZ, Utils.STEP, current_date, end_date))
    threads.append(t2)
    t3 = threading.Thread(target=DBUtils.save_data, args=(Utils.startCY, Utils.MAXCY, Utils.STEP, current_date, end_date))
    threads.append(t3)

    for t in threads:
        t.start()
        # t.join()


'''
    保存股票前复权数据，批量执行
    逻辑：
        STEP1：获取全部基础数据
        STEP2：循环分10个线程获取单个股票数据，保存
'''


def do_save_basics_batch():
    engine = DBUtils.db_connection()
    basic_info_rs = DBUtils.get_stock_basics(engine)
    logger.info('get basic info total::::' + str(basic_info_rs.count()))
    share_code_list = list(basic_info_rs[u'code'])
    # time_to_market_list=list(basic_info_rs[u'timeToMarketNew'])
    counter = 0
    end = len(share_code_list)
    while counter <= end:
        threads = []
        '''第一个线程'''
        share_code = share_code_list[counter]
        time_to_market = Utils.get_pre_year(Utils.get_current_date())
        # time_to_market=time_to_market_list[counter]
        logger.debug("prepare to run:::::share_code="+str(share_code)+"time_to_market="+str(time_to_market))
        # t1=threading.Thread(target=do_save_basics_pre, args=(share_code,time_to_market))
        do_save_basics_pre(share_code,time_to_market)
        # threads.append(t1)

        # counter=counter+1
        # if counter > end:
        #     logger.info("out of end!!! end="+end+'counter='+counter)
        # else:
        #     '''第二个线程'''
        #     share_code = share_code_list[counter]
        #     time_to_market = time_to_market_list[counter]
        #     t2=threading.Thread(target=do_save_basics_pre, args=(share_code,time_to_market))
        #     threads.append(t2)
        #
        # counter = counter + 1
        # if counter > end:
        #     logger.info("out of end!!! end=" + end + 'counter=' + counter)
        #     break
        # else:
        #     '''第三个线程'''
        #     share_code = share_code_list[counter]
        #     time_to_market = time_to_market_list[counter]
        #     t3 = threading.Thread(target=do_save_basics_pre, args=(share_code, time_to_market))
        #     threads.append(t3)
        #
        # counter = counter + 1
        # if counter > end:
        #     logger.info("out of end!!! end=" + end + 'counter=' + counter)
        #     break
        # else:
        #     '''第5个线程'''
        #     share_code = share_code_list[counter]
        #     time_to_market = time_to_market_list[counter]
        #     t5 = threading.Thread(target=do_save_basics_pre, args=(share_code, time_to_market))
        #     threads.append(t5)
        #
        #
        # counter = counter + 1
        # if counter > end:
        #     logger.info("out of end!!! end=" + end + 'counter=' + counter)
        #     break
        # else:
        #     '''第6个线程'''
        #     share_code = share_code_list[counter]
        #     time_to_market = time_to_market_list[counter]
        #     t6 = threading.Thread(target=do_save_basics_pre, args=(share_code, time_to_market))
        #     threads.append(t6)
        #
        counter=counter+1
        # for t in threads:
        #     t.start()
        #     t.join()


'''
    保存股票前复权的数据
'''


def do_save_basics_pre(share_code, time_to_market):
    engine = DBUtils.db_connection()
    cur_date = Utils.get_current_date()
    change_time = time_to_market
    while change_time < cur_date:
        next_year = Utils.get_next_year(change_time)
        logger.debug('prepare to save next time range data....change_time='+str(change_time)+'next_year='+str(next_year))
        DBUtils.save_stock_h_data(engine,share_code,change_time,next_year)
        change_time = Utils.get_next_day(Utils.get_next_year(change_time))


def save_index(start_date, finish_date):
    begin_date = start_date
    end_date = finish_date
    DBUtils.save_index_data_end_date(begin_date, end_date)


def do_check(start_date, finish_date):
    threads = []
    current_date = start_date
    end_date = finish_date

    t1 = threading.Thread(target=checkStockVolumn, args=(Utils.startSH, Utils.MAXSH, Utils.STEP, current_date, end_date))
    threads.append(t1)
    t2 = threading.Thread(target=checkStockVolumn, args=(Utils.startSZ, Utils.MAXSZ, Utils.STEP, current_date, end_date))
    threads.append(t2)
    t3 = threading.Thread(target=checkStockVolumn, args=(Utils.startCY, Utils.MAXCY, Utils.STEP, current_date, end_date))
    threads.append(t3)
    for t in threads:
        t.start()
        t.join()


def static_change(share_code, start_date, finish_date):
    stock_change = DataAnalysis.static_single_wave(share_code, start_date, end_date)
    market_pro = Utils.check_market(share_code)
    market_change = DataAnalysis.static_a_wave(market_pro, start_date, finish_date)

def copy_table():
    engine = DBUtils.db_connection()
    sql = "select * from shares.k_his_data"
    rs = pandas.read_sql_query(sql, engine)
    #     rsNew=rs.copy()
    #     rs[u'date']=rsNew[u'date'].map(changeTo)
    #     print(rs)
    rs.to_sql('k_his_data_new', engine, if_exists='append')


def draw_pic(cur_list):
    # engine = DBUtils.dbConnection()
    # rs = DBUtils.dbReadKIndex("sh", engine)
    # close = rs[u'close']
    plt.figure("test")
    plt.plot(cur_list)
    plt.show()
    # os.system("pause")


def test(year):
    while year < 2005:
        quarter = 4
        # while quarter <= 4:
        logger.info("\n")
        logger.info("year:::"+ str(year) + " quarter:::" + str(quarter))
        save_share_report(year, quarter)
        # quarter = quarter + 1
        year = year +1


def save_basics():
    engine=DBUtils.db_connection()
    DBUtils.save_stock_basics(engine)


'''
保存报表
'''


def save_share_report(year, quarter):
    engine = DBUtils.db_connection()
    DBUtils.save_share_report(engine, year, quarter)
    DBUtils.save_share_report_profit(engine, year, quarter)
    DBUtils.save_share_report_operation(engine, year, quarter)
    DBUtils.save_share_report_growth(engine, year, quarter)
    DBUtils.save_share_report_debt(engine, year, quarter)
    DBUtils.save_share_report_cash_flow(engine, year, quarter)


'''
基本面分析展示
'''
def get_basic_info_draw(share_code, start_year, end_year, quarter):
    rs = DataAnalysis.share_basic_info_analysis(share_code, start_year, end_year, quarter)
    year_list = list(rs[u'year'])
    cur_ration_list = list(rs[u'currentratio'])
    '''将字符串转换成数字'''
    cur_ration_list = list(map(lambda x:float(x), cur_ration_list))
    quick_ratio_list = list(rs[u'quickratio'])
    quick_ratio_list = list(map(lambda x:float(x), quick_ratio_list))

    # x_start = int(year_list[0])
    # x_end = int(year_list[-1])
    plt.figure("test")
    plt.plot(year_list, cur_ration_list, label='current ratio', color='black')
    plt.plot(year_list, quick_ratio_list, label='quick ratio', color='red')
    # plt.axis([x_start, x_end, 0, 1])
    # plt.plot(year_list, cur_ration_list, year_list, quick_ratio_list, linewidth=2)
    plt.legend()
    plt.show()



print("start working...")
begin_date='2018-02-01'
end_date='2018-04-27'
check_date = '2018-02-01'
# temp_code=Utils.change_sz_code(2016)
# temp_code='002813'
# result=DataAnalysis.market_simulate(50000,temp_code,'2017-08-01','2017-11-01')
# draw_pic(result)
# do_save(begin_date, end_date)
# save_index(begin_date, end_date)
# do_check(check_date, end_date)
# static_change('300365',check_date, end_date)
# save_share_report(2017, 4)
# copyTable()
# saveIndustryIndex()
# drawPic()
# save_basics()
# test(1990)
get_basic_info_draw('300365', 2010,2017,4)
# do_save_basics_batch()

print("finish working...")

#
# code = str('000001')
# saveDataFromNowOn(1, 1, 1, '2017-01-01')           
# currentRs = dbRead(code, "2017-09-01")
# currentCode = checkByVolumn(code, currentRs)
# print(currentCode)
# dbRead(str(300134),"2017-10-8")
# dbSave(str(300134),"2017-1-1")
# saveData(300134,300135,1,'2017-1-1')
