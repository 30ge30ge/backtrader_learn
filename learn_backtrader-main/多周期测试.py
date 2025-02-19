# 多周期
# 买入条件：日MACD金叉、周RSI小于50
# 卖出条件：价格较最高收盘价回撤5%卖出
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import backtrader as bt
import backtrader.feeds as btfeeds
import backtrader.indicators as btind
from datetime import datetime
import pandas as pd
from collections import defaultdict
import akshare as ak










#拓展数据
class Dailydataextend(bt.feeds.PandasData):
    # 增加线
    lines = ('momentum_5','bBreak','bEnter', )
    params = (('momentum_5', -1),('bBreak', -1),('bEnter', -1),
              ('dtformat', '%Y-%m-%d'),)


class Getdata():
    def __init__(self):
        self.code = None


    def mindata(self):
        data=pd.read_csv('/Users/mac1234/PycharmProjects/trader/可转债策略/可转债分时.txt')
        data.index = pd.to_datetime(data['时间'])
        data['openinterest'] = 0
        data = data[['开盘', '最高', '最低', '收盘', '成交量','openinterest','symbol']]
        columns = ['open', 'high', 'low', 'close', 'volume', 'openinterest','symbol']
        data.columns = columns

        return data

    def dailydata(self):
        data=pd.read_csv('/Users/mac1234/PycharmProjects/trader/可转债策略/可转债日线行情.txt')
        data.index = pd.to_datetime(data['date'])
        data['openinterest'] = 0
        data = data[['open', 'high', 'low', 'close', 'volume', 'openinterest', 'symbol','momentum_5', 'pivot', 'bBreak', 'bEnter']]
        return data








class RSIMACDMultiTF(bt.Strategy):
    params = (
        ('trailamount', 0.0),
        ('trailpercent', 0.05),
    )

    def __init__(self):
        # 存储不同数据的技术指标
        self.inds = dict()
        # 存储特定股票的订单，key为股票的代码
        self.orders = dict()
        # 遍历所有数据
        for i, d in enumerate(self.datas):

            self.orders[d._name] = None
            # 为每个数据定义字典，存储技术指标
            self.inds[d] = dict()
            # 判断d是否为日线数据
            if 0 == i % 2:
                self.inds[d]['crossup'] = btind.CrossUp(btind.MACD(d).macd, btind.MACD(d).signal)
            # d为周线数据
            else:
                self.inds[d]['rsi'] = btind.RSI_Safe(d)

    def next(self):
        for i, d in enumerate(self.datas):
            print(self.datas[d]['crossup'])

            # 如果处理周线数据则跳过买卖条件，因为已在日线数据判断处理过
            if 1 == i % 2:
                continue
            pos = self.getposition(d)
            # 不在场内，则可以买入
            if not len(pos):
                # 达到买入条件
                if self.inds[d]['crossup'][0] and self.inds[self.datas[i + 1]]['rsi'][0] < 50:

                    # 买入手数，如果是多只股票回测，这里需要修改
                    stake = int(self.broker.cash // (d.close[0] * 100)) * 100
                    # 买买买
                    self.buy(data = d, size = stake)
            elif not self.orders[d._name]:
                # 下保护点卖单
                self.orders[d._name] = self.close(data = d, exectype= bt.Order.StopTrail,
                            trailamount=self.p.trailamount,
                            trailpercent=self.p.trailpercent)

    def notify_order(self, order):

        if order.status in [order.Completed]:
            if order.isbuy():
                print('{} BUY {} EXECUTED, Price: {:.2f}'.format(self.datetime.date(), order.data._name, order.executed.price))
            else:  # Sell
                self.orders[order.data._name] = None
                print('{} SELL {} EXECUTED, Price: {:.2f}'.format(self.datetime.date(), order.data._name, order.executed.price))

def runstrat():
    from_idx = datetime(2023, 4, 1)  # 记录行情数据的开始时间和结束时间
    to_idx = datetime(2023, 4, 15)
    print(from_idx, to_idx)


    cerebro = bt.Cerebro()
    cerebro.broker.setcash(1000000.0)
    cerebro.addstrategy(RSIMACDMultiTF)
    data=Getdata()
    data_0=data.mindata()
    data_1 = data.dailydata()
    banks=data_1['symbol'].unique().tolist()
    for stk_code in banks[:5]:
        data_m=data_0[data_0['symbol']==stk_code]
        data_m=bt.feeds.PandasData(dataname=data_m,fromdate=from_idx,todate=to_idx,timeframe=bt.TimeFrame.Minutes,name='15m')
        cerebro.adddata(data_m)
        cerebro.resampledata(data_m, name='1d', timeframe=bt.TimeFrame.Days)

    # cerebro.addwriter(bt.WriterFile, out = 'log.csv', csv = True)
    cerebro.run()
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
    # Plot the result绘制结果
    cerebro.plot(volume = False, style = 'candle',barup = 'red', bardown = 'green')

if __name__ == '__main__':
    runstrat()
