"""
Created on 2017年12月10日

@author: Colin
"""


from colin.chen.shares import Utils
import logging.config


class MoneyMaker(object):

    """
    初始化函数
    ware_house 持仓货币
    share_value 持仓金额
    share_price 股票价格
    share_numbers 持仓股票数量
    share_code 购买股票代码
    
    """

    def __init__(self, ware_house, share_code):
        self.ware_house = ware_house
        self.share_code = share_code
        self.share_value = 0
        self.share_price = 0
        self.share_numbers = 0
        self.counter = 0
        self.last_kick = 'buy/sell'
        self.last_kick_day = Utils.get_current_date()
        '''logger配置'''
        logging.config.fileConfig('logging.conf')
        self.logger = logging.getLogger(__name__)

    '''
        检查是否有足够的持仓货币完成交易
    '''
    def is_enough_money(self, buy_value):
        if self.ware_house < buy_value:
            return False
        else:
            return True

    '''
    购买函数
    buy_value 购买价格
    share_price 股票价格
    '''
    def buy_share(self, buy_value, share_price):
        is_have = self.is_enough_money(buy_value)
        '''检查持仓货币是否足够，如果不足够提示退出'''
        if is_have is not True:
            print("Error!!!, There is not enough money to buy shares")
            return False
        if self.last_kick == 'buy':
            trigger = ( self.share_price - share_price ) / self.share_price
            if trigger < 0.05:
                self.logger.info('Not to trigger to buy')
                return False
        # price_trigger=round(self.share_price*0.91,2)
        # if share_price > price_trigger and self.share_value!=0:
        #     print("Error!!! Not have to trigger the 0.91 line!")
        #     return False
        '''购买股票，向下取整'''
        cur_share_numbers = (buy_value // (share_price * 100)) * 100
        self.counter = self.counter + 1
        self.ware_house -= cur_share_numbers * share_price
        self.last_kick = 'buy'
        self.last_kick_day = Utils.get_current_date()

        '''
           计算持仓成本
           STEP1:计算持仓股票当前市值 当前价格*持有数量
           STEP2:本次购买市值  当前价格*本次购买数量
           STEP3:计算平均成本  （持仓市值+本次购买市值）/持有数量
        '''
        # tmp_price=(self.share_value + cur_share_numbers * share_price) / (self.share_numbers + cur_share_numbers)

        self.share_value += cur_share_numbers * share_price
        pre_buy_value = self.share_price * self.share_numbers
        self.share_numbers += cur_share_numbers
        self.share_price = round((pre_buy_value + cur_share_numbers * share_price) / self.share_numbers, 2)
        # self.share_price = round(self.share_value / self.share_numbers, 2)

        return cur_share_numbers

    '''
    检查是否有足够的股票卖出
    '''
    def is_enough_shares(self):
        if self.counter == 0:
            print("Error!!!There is no shares to sell")
            return True

    '''
    卖出函数
    share_numbers 卖出数量
    share_price 股票价格
    '''
    def sell_share(self, share_numbers, share_price):
        if self.is_enough_shares() is True:
            return

        self.ware_house += share_numbers * share_price
        self.share_value -= share_numbers * share_price
        self.share_numbers -= share_numbers
        self.counter = self.counter - 1
        self.last_kick = 'sell'
        self.last_kick_day = Utils.get_current_date()

    '''
    根据每日收盘价格刷新市值数据
    share_price 股票收盘价格
    '''
    def refresh_market_price(self, share_price):
        self.share_value = self.share_numbers*share_price
        self.print_moneymaker()

    '''
    卖出4，2，1策略
    '''
    def mm_sell_policy(self):
        result = 0
        if self.counter == 3:
            result = self.share_numbers / 7 * 4
        if self.counter == 2:
            result = self.share_numbers / 3 * 2
        if self.counter == 1:
            result = self.share_numbers
        return result

    '''
    购买1，2，4仓位策略
    '''
    def mm_buy_policy(self):
        money_buy = 0
        if self.counter == 0:
            money_buy = self.ware_house / 7
        if self.counter == 1:
            money_buy = self.ware_house / 7 * 2
        if self.counter == 2:
            money_buy = self.ware_house / 7 * 4
        return money_buy

    '''
    打印moneymaker对象
    ware_house=0
    share_value=0
    share_price=0
    share_numbers=0
    share_code=0
    counter=0
    '''
    def print_moneymaker(self):
        # print('ware_house=' + str(self.ware_house) + 'share_value=' + str(self.share_value) + 'share_price=' +
        #       str(self.share_price) + 'share_numbers=' + str(self.share_numbers) + 'share_code=' +
        #       str(self.share_code) + 'counter=' + str(self.counter))
        return str('ware_house=' + str(self.ware_house) + 'share_value=' + str(self.share_value) + 'share_price=' +
                   str(self.share_price) + 'share_numbers=' + str(self.share_numbers) + 'share_code=' +
                   str(self.share_code) + 'counter=' + str(self.counter))
#     def __init__(self,name,score):
#         self.name=name
#         self.score=score
#
#
#     def print(self):
#         print("%s:%s"%(self.name,self.score))
#
#     def get_grade(self):
#         if self.score>=90:
#             return "A"
#         if self.score>=60:
#             return "B"
#         return "C"
#
# test=MoneyMaker("test",92)
# test.print()
# print(test.get_grade())
