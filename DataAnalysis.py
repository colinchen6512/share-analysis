"""
Created on 2017年12月9日

@author: Colin
"""

import logging.config

import pandas as pd

import numpy as np

from colin.chen.shares import DBUtils
from colin.chen.shares import MoneyMaker
from colin.chen.shares import Utils

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('main')

'''
判断是否持续增大，连续放量大于最低量INCR_VOL倍，持续时间LASTDAYS（放量大于INCR_VOL倍后，累计时间）

2018-01-06 TODO:需要增加条件：剔除一字涨停板+跌停板产生的交易量
'''


def check_by_vols(share_code, current_rs):
    if len(current_rs):
        item_vols = list(current_rs[u'volume'])
        item_close = list(current_rs[u'close'])
        item_start = list(current_rs[u'open'])
        item_high = list(current_rs[u'high'])
        item_low = list(current_rs[u'low'])
        item_date = list(current_rs[u'date'])
    else:
        return
    '''总计数器'''
    i_counter = 0

    item_result = item_vols[i_counter]

    '''持续放量时间计数器'''
    last_day_counter = 0

    '''开关：表示是否已经用非一字板数据进行比较'''
    '''TRUE表示没有比较过，FALSE表示已经比较过'''
    is_compare = True

    '''记录统计区间第一个收盘值，为剔除上涨过多股票做记录'''
    first_close = item_close[i_counter]

    for curVolume in item_vols:
        item_close_result = item_close[i_counter]
        item_start_result = item_start[i_counter]
        item_high_result = item_high[i_counter]
        item_low_result = item_low[i_counter]
        item_date_result = item_date[i_counter]
        i_counter = i_counter + 1
        '''如果是一字板，跳出进行下一条数据检查'''
        if check_is_one_reach(item_start_result, item_high_result, item_low_result, item_close_result):
            logger.info("There is a 10% reach to continue...share_code=" + str(share_code) + 'date=' +
                        str(item_date_result))
            '''如果刚开始就是一字板，则取iTemResult为iTemVols集合中下一个值'''
            if is_compare is True and i_counter < pd.DataFrame(item_vols).size:
                item_result = item_vols[i_counter]
            continue
        '''如果当前成交量小于上一条成交量，替换最小成交量为当前数值'''
        if curVolume <= item_result:
            item_result = curVolume
            '''一旦已经比较，将开关设置为false'''
            is_compare = False
            # item_close_result = itemClose[iCounter]
            last_day_counter = 0
        else:
            if round(curVolume / item_result, 2) > Utils.INCR_VOL:
                last_day_counter = last_day_counter + 1
    #                 print(itemDate[iCounter],curVolume,iTemResult,lastDayCounter)
            if last_day_counter >= Utils.LASTDAYS:
                '''獲取list最後一個元素'''
                check_close_result = item_close[-1]
                # print(shareCode)

                is_exists = check_price_change(first_close, check_close_result)
                # item_vols_df = pd.DataFrame(item_vols)
                is_avg_increase = check_avg_vol_increase(item_vols, item_result, share_code)
                if is_exists:
                    logger.info("The share has increase more than 15%!!!share_code=" + str(share_code)
                                + "first_close=" + str(first_close) + "last_close=" + str(check_close_result))
                    break
                if is_avg_increase:
                    logger.info("The share avg increase 5 times!!!!!share_code=" + str(share_code))
                else:
                    return share_code

'''
检查最小成交量以前成交量平均值，同以后成交量平均值比较。是否超出3倍以上
'''


def check_avg_vol_increase(vols_data, item_result, share_code):
    '''检查最低量时间点以前的平均量，以后的平均量，查看是否平均量增长很多 add at 2018-4-1'''
    # item_vols_df = pd.DataFrame(item_vols)
    vols_dataframe = pd.DataFrame(vols_data)
    # vol_lowest = int(item_result)
    low_point = int(vols_data.index(item_result))
    before_mean = vols_dataframe.iloc[0:low_point + 1].mean()
    after_mean = vols_dataframe.iloc[low_point + 1:].mean()
    if int(before_mean) * 3 < int(after_mean):
        logger.info("attention!!!!!!!!!!!" + str(share_code))
        return False
    else:
        return True
    '''2018-4-1 end'''

'''
检查是否是一字板
'''


def check_is_one_reach(open_price, high, low, close):
    # logger.debug('open=' + str(open_price) + 'high=' + str(high) + 'low=' + str(low) + 'close=' + str(close))
    if open_price == close and open_price == high and open_price == low:
        return True
    else:
        return False


'''
检查变动幅度
  超过+-15%的剔除掉
'''


def check_price_change(start_close, end_close):
    change_range = round(abs(start_close - end_close) / start_close, 4)
    # if startClose > endClose:
    #     changeRange = round((startClose - endClose) / startClose, 4)
    # if startClose < endClose:
    #     changeRange = round((endClose - startClose) / startClose, 4)
    # print("startClose=" + str(startClose) + " endClose=" + str(endClose) + " changeRange=" + str(changeRange))
    if change_range > Utils.VOLATILITY:
        return True


'''
日BOLL指标的计算公式
　　中轨线=N日的移动平均线
　　上轨线=中轨线+两倍的标准差
　　下轨线=中轨线-两倍的标准差
日BOLL指标的计算过程
计算MA：
　　MA=N日内的收盘价之和除以N
计算标准差MD：
　　MD=平方根N日的（C - MA）的两次方之和除以N
计算MB、UP、DN线：
　　MB=（N-1）日的MA
　　UP=MB + 2 × MD
　　DN=MB - 2 × MD

2018-01-03 TODO：boll up和dn两个值不正确，std函数标准差计算跟numbers中公式计算不匹配
'''


def get_boll_result(rs):
    day = Utils.BOLL_DAY
    k = 2
    # print("rs=============")
    # print(rs)
    '''计算前day天的平均值'''
    tmp_ma = rs[u'close'].rolling(window=day).mean().tail(1).iloc[0]
    ma = round(tmp_ma, 2)
    # print('ma========'+str(ma))
    '''标准差'''
    # print("tmp_md============")
    # print(np.array(rs[u'close'].rolling(window=day).std()))
    tmp_md = rs[u'close'].rolling(window=day).std().tail(1).iloc[0]
    md = round(tmp_md, 2)
    # print('md========'+str(md))
    '''计算前day-1天的平均值'''
    # tmp_mb = np.array(rs[u'close'].rolling(day - 1).mean().tail(2).head(1)).tolist()
    # print('tmp_mb=============')
    # print(rs[u'close'].rolling(window=day - 1).mean())
    to_cal_mb = np.array(rs[u'close']).tolist()
    # tmp_mb = rs[u'close'].rolling(window=day - 1).mean().tail(1).iloc[0]
    tmp_mb = Utils.cal_std_dev(to_cal_mb)
    mb = round(tmp_mb, 2)
    # print('mb========='+str(mb))
    # print("mb,k,md======="+str(mb)+' '+str(k)+' '+str(md))
    up = round(mb + k * md, 2)
    # print(up)
    dn = round(mb - k * md, 2)
    # print(dn)
    print("up,ma,dn======" + str(up) + ' ' + str(ma) + ' ' + str(dn))
    return up, ma, dn


'''
检查boll线，指定时间
'''


def get_boll(share_code, start_date, end_date):
    engine = DBUtils.db_connection()
    rs = DBUtils.db_read_k_history_des(share_code, engine, start_date, end_date)
    return get_boll_result(rs)


'''
检查boll线从开始时间到当前时间
'''


def get_current_boll(share_code, start_date):
    return get_boll(share_code, start_date, Utils.get_current_date())


'''
只根据两天收市价格检查k线走势
TREND_UP  上升
TREND_DOWN 下降

2018-01-06 TODO: 检查光头K线，光头K线不买卖
'''


def check_k_trend(pri_close, cur_close):
    if cur_close < pri_close:
        return Utils.TREND_DOWN
    if cur_close > pri_close:
        return Utils.TREND_UP


'''
计算KDJ指标
@2018-03-02
'''


def kdj_func(share_code, current_rs, step):
    if len(current_rs):
        item_close = current_rs[u'close']
        item_high = current_rs[u'high']
        item_low = current_rs[u'low']
        item_date = current_rs[u'date']
    else:
        return
    '''
    从第一条数据开始遍历
    如果不是交易日，继续遍历下一条
    如果是交易日，获取当前交易日收盘价C，N日内最低价LN，N日内最高价HN，计算rsv，公式=（CN-LN)/(HN-LN)*100
    
    '''
    engine = DBUtils.db_connection()
    trade_cal = DBUtils.read_trad_cal(engine)
    counter = 0
    '''
    定义计算好的值保存数组变量
    '''
    k_list = ['0', '0', '0', '0', '0', '0', '0', '0']
    d_list = ['0', '0', '0', '0', '0', '0', '0', '0']
    j_list = ['0', '0', '0', '0', '0', '0', '0', '0']
    '''
    初始值定位50
    '''
    pre_k = 50
    pre_d = 50

    cur_size = item_close.count()
    for cur_date in item_date:
        is_today_open = trade_cal.loc[trade_cal[u'calendarDate'] == cur_date].iloc[0, 2]
        '''如果是休息日，就继续下一天'''
        if is_today_open == Utils.CLOSE_SALE:
            logger.info("today market close...." + str(cur_date))
            continue
        '''计算公式'''
        cur_end = counter + step - 1
        if cur_end >= cur_size:
            logger.info("out of bounds..." + str(cur_end))
            break
        ln = item_low.iloc[counter:cur_end].min()
        hn = item_high.iloc[counter:cur_end].max()
        cur_close = item_close.iloc[cur_end]
        rsvn = round((cur_close - ln) / (hn - ln) * 100, 2)
        cur_k = 2 / 3 * pre_k + 1 / 3 * rsvn
        cur_d = 2 / 3 * pre_d + 1 / 3 * cur_k
        cur_j = 3 * cur_k - 2 * cur_d
        k_result = round(cur_k, 2)
        d_result = round(cur_d, 2)
        j_result = round(cur_j, 2)
        k_list.append(str(k_result))
        d_list.append(str(d_result))
        j_list.append(str(j_result))
        pre_k = k_result
        pre_d = d_result

        counter = counter + 1
    logger.debug("k value:::::" + str(k_list))
    logger.debug("d value:::::" + str(d_list))
    logger.debug("j value:::::" + str(j_list))
    return k_list, d_list, j_list


def test():
    engine = DBUtils.db_connection()
    rs = DBUtils.db_read_k_history_des('300623', engine, '2017-12-13', '2017-12-29')
    # rs=DBUtils.dbReadKHistoryDes('603160', engine, '2017-12-13','2017-12-29')
    get_boll_result(rs)


'''
初始化持仓金额和持仓股票代码
到达boll线中轨时，购买持仓金额的七分之一，到达下轨时再购买持仓金额的七分之二
从下轨到中轨时，卖出持仓金额的七分之二
从中轨到上轨时，清仓
持有周期一个月
ware_house 仓位资金
share_code 股票代码
start_date 开始时间
end_date   结束时间

2018-01-02 已经实施，根据boll进行仓位操作要增加趋势判断，不能简单根据三根线进行买卖操作。
2018-01-08 bug已经修改。L288-L302要根据星期几来进行数据获取
2018-01-09 TODO:买入和卖出条件需要刷新，如果第一天已经买入/卖出，第二天如果达到触发条件，不能继续买入/卖出
            通过MoneyMaker中的成员变量交易动作和时间来判断
'''


def market_simulate(ware_house, share_code, start_date, end_date):
    result = []
    mm = MoneyMaker.MoneyMaker(ware_house, share_code)
    engine = DBUtils.db_connection()
    fronter_date = Utils.get_front_day(start_date, 60)
    print('fronter_date=' + fronter_date)
    rs = DBUtils.db_read_k_history_des(share_code, engine, fronter_date, end_date)
    trade_cal = DBUtils.read_trad_cal(engine)
    # print("rs:::::::::::::::::::::::::")
    # print(rs)
    cur_date = start_date
    '''买入日期，如果前一天买入，第二天触发买入条件则不买入'''
    # sell_day = Utils.get_current_date()
    '''卖出日期，如果前一天卖出，第二天触发卖出条件则不卖出'''
    # buy_day = Utils.get_current_date()
    while cur_date <= end_date:
        is_today_open = trade_cal.loc[trade_cal[u'calendarDate'] == cur_date].iloc[0, 2]
        # is_yesterday_open=np.array(trade_cal.loc[trade_cal[u'calendarDate']==Utils.get_front_day(cur_date,1)][u'isOpen']).tolist()[0]
        logger.debug('begin:::::::::::::' + str(cur_date))
        '''获取当前日期前的集合'''
        temp_rs = rs.loc[(rs[u'date'] < cur_date)]
        # print("temp_rs====================")
        # print(temp_rs)
        '''从当前日期前的集合获取排序前20个的集合'''
        days = Utils.BOLL_DAY
        filter_rs = temp_rs.iloc[0:days]
        # print("filter_rs==================")
        # print(filter_rs)
        (up, mi, dn) = get_boll_result(filter_rs)

        '''看当天日期，如果为休市状态，则跳到下一日'''
        if is_today_open == Utils.CLOSE_SALE:
            cur_date = Utils.get_next_day(cur_date)
            continue

        tmp_today = rs.loc[(rs[u'date'] == cur_date)]
        yest_tmp = Utils.get_front_day(cur_date, 1)
        # is_yesterday_open = \
        #     np.array(trade_cal.loc[trade_cal[u'calendarDate'] == yest_tmp][u'isOpen']).tolist()[
        #         0]
        is_yesterday_open = trade_cal.loc[trade_cal[u'calendarDate'] == yest_tmp].iloc[0, 2]

        '''如果前一个交易日不开市，一直往前找到最近一个交易日'''
        while is_yesterday_open != Utils.OPEN_SALE:
            yest_tmp = Utils.get_front_day(yest_tmp, 1)
            is_yesterday_open = trade_cal.loc[trade_cal[u'calendarDate'] == yest_tmp].iloc[0, 2]

        tmp_yesterday = rs.loc[(rs[u'date'] == yest_tmp)]
        # today_info = np.array(tmp_today[u'close']).tolist()[0]
        # today_info = tmp_today.iloc[0,3]
        close_price = round(tmp_today.iloc[0, 3], 4)
        # yesterday_info = np.array(tmp_yesterday[u'close']).tolist()[0]
        close_yesterday = round(tmp_yesterday.iloc[0, 3], 4)
        # close_yesterday = round(yesterday_info, 4)

        logger.debug('close_yesterday::::::::::::' + str(close_yesterday))
        logger.debug('close_price::::::::::::' + str(close_price))
        logger.debug("MainProcess:::::up,mi,dn======" + str(up) + ' ' + str(mi) + ' ' + str(dn))
        logger.debug(close_price)
        # print(temp_date)
        is_up_or_down = check_k_trend(close_yesterday, close_price)
        if is_up_or_down == Utils.TREND_DOWN:
            '''现根据当天close价格刷新市值'''
            mm.refresh_market_price(close_price)
            # cur_today=Utils.get_current_frontday(1)
            if 0 <= close_price - mi < 0.1:
                ware_house = mm.mm_buy_policy()
                buy_result = mm.buy_share(ware_house, close_price)
                # buy_day = Utils.get_current_date()
                if buy_result:
                    logger.info("buy=========date:" + cur_date + "price:" + str(close_price) + "buy stock:" +
                                str(ware_house))
                    logger.info("mm value:" + mm.print_moneymaker())
            if 0 <= close_price - dn < 0.1:
                ware_house = mm.mm_buy_policy()
                buy_result = mm.buy_share(ware_house, close_price)
                if buy_result:
                    logger.info("buy=========date:" + cur_date + "price:" + str(close_price) + "buy stock:" +
                                str(ware_house))
                    logger.info("mm value:" + mm.print_moneymaker())
        elif is_up_or_down == Utils.TREND_UP:
            """清仓时删除趋势判断"""
            if up - close_price <= 0.1 or close_price > up:
                sell_numbers = mm.mm_sell_policy()
                '''现根据当前价位刷新市值'''
                mm.refresh_market_price(close_price)
                mm.sell_share(sell_numbers, close_price)
                logger.info("sell=========date:" + cur_date + "price:" + str(close_price) + "sell stock:" + str(
                    sell_numbers))
                logger.info("mm value:" + mm.print_moneymaker())
            if mi - close_price <= 0.1 or close_price > mi:
                sell_numbers = mm.mm_sell_policy()
                mm.refresh_market_price(close_price)
                mm.sell_share(sell_numbers, close_price)
                logger.info("sell=========date:" + cur_date + "price:" + str(close_price) + "sell stock:" + str(
                    sell_numbers))
                logger.info("mm value:" + mm.print_moneymaker())

        mm.refresh_market_price(close_price)
        result.append(mm.share_value + mm.ware_house)
        cur_date = Utils.get_next_day(cur_date)
        logger.debug('end:::::::' + str(cur_date))
    logger.debug("total result+" + str(result))
    return result


'''
统计单只股票周期内的波动
'''


def static_single_wave(share_code, begin_date, end_date):
    engine = DBUtils.db_connection()
    rs = DBUtils.db_read_k_history_inc(share_code, engine, begin_date, end_date)
    if len(rs):
        item_p_change = rs['p_change']
    else:
        return

    total = item_p_change.sum()
    logger.info("stock_change sum is: " + str(total) + " from " + begin_date + " to " + end_date)
    return total


'''
统计大盘的波动
'''


def static_a_wave(share_code, begin_date, end_date):
    engine = DBUtils.db_connection()
    rs = DBUtils.db_read_index_history(share_code, engine, begin_date, end_date)
    if len(rs):
        item_p_change = rs['p_change']
    else:
        return

    total = item_p_change.sum()
    logger.info("market_change sum is: " + str(total) + " from " + begin_date + " to " + end_date)
    return total


'''
    基本面数据提取分析
    quarter: 1 季度为周期
             2 半年度为周期
             4 年度为周期
'''


def share_basic_info_analysis(share_code, start_year, end_year, quarter):
    engine = DBUtils.db_connection()
    rs = DBUtils.db_read_debt_info(share_code, engine, start_year, end_year, quarter)
    return rs