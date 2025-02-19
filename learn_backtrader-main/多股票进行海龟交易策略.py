import time
import math
import datetime
import numpy as np
import pandas as pd

import backtrader as bt
from sklearn.svm import SVR

today=(datetime.datetime.now()+datetime.timedelta(days=-3)).strftime('%Y%m%d')

df_all = pd.read_csv('D:/北京交接20201116/tushare数据/每日指标与每日行情数据/'+today+'每日指标.txt',encoding='utf-8',parse_dates=['trade_date'])
stocklist_allA = df_all['ts_code'].unique().tolist()

df_all['openinterest']=1
df_all['volume']=df_all['vol']



def get_stock_data(code):
    df_stock = df_all[df_all['ts_code']==code]
    df_stock = df_stock[['trade_date','open','high','low','close','volume',
                         'openinterest','pe','pb','turnover_rate','circ_mv','total_mv']]
    df_stock['trade_date']=pd.to_datetime(df_stock['trade_date'])
    df_stock.index=df_stock.trade_date
    df_stock = df_stock.sort_index()
    return df_stock




#增加数据
class Addmoredata(bt.feeds.PandasData):
    lines = ('pe','pb','turnover_rate','circ_mv','total_mv',)
    params = (('pe',7),('pb',8),('turnover_rate',9),('circ_mv',10),('total_mv',11),('dtformat', '%Y-%m-%d'),)


class TurtleStrategy(bt.Strategy):
#默认参数
    params = (('long_period',20),
              ('short_period',10),
              ('printlog', False), )

    def __init__(self):
        self.order = None
        self.buyprice = 0
        self.buycomm = 0
        self.buy_size = 0
        self.buy_count = 0
        # 海龟交易法则中的唐奇安通道和平均波幅ATR
        self.inds = dict()
        for i,d in enumerate(self.datas):
            print(i,d,d._name)
            self.inds[d]['sma1'] = bt.ind.SMA(d.close, period=self.p.short_period)  # 短期均线
            self.inds[d]['sma2'] = bt.ind.SMA(d.close, period=self.p.long_period)  # 长期均线
            self.inds[d]['cross'] = bt.ind.CrossOver(self.inds[d]['sma1'], self.inds[d]['sma2'], plot=False)
            # self.inds[d]['DonchianHi'] = bt.ind.Highest(d.high[-1], period=self.p.long_period)  # 短期均线


        #     self.inds[d].Donchianlow = bt.ind.Lowest(d.low(-1), period=self.p.short_period)
            self.inds[d].TR = bt.indicators.Max((d.high[0]- d.low[0]),abs(d.close[-1]-d.high[-1]),abs(d.close(-1)-d.low[0]))
        #     self.inds[d].ATR = bt.indicators.SimpleMovingAverage(self.inds[d].TR, period=14)
        # # 价格与上下轨线的交叉
        #     self.buy_signal = bt.ind.CrossOver(d.close[0], self.inds[d].DonchianHi )
        #     self.sell_signal = bt.ind.CrossOver(d.close[0], self.inds[d].Donchianlow )

    def next(self):
        for i, d in enumerate(self.datas):
            pass
            # if self.order:
            #     return
            # #入场：价格突破上轨线且空仓时
            # if self.inds[d].buy_signal > 0 and d.buy_count == 0:
            #     d.buy_size = d.broker.getvalue() * 0.01 / self.inds[d].ATR
            #     d.buy_size  = int(d.buy_size  / 100) * 100
            #     d.sizer.p.stake = d.buy_size
            #     d.buy_count = 1
            #     self.order = self.buy()
            # #加仓：价格上涨了买入价的0.5的ATR且加仓次数少于3次（含）
            # elif d.close >d.buyprice+0.5*self.inds[d].ATR[0] and d.buy_count > 0 and d.buy_count <=4:
            #     d.buy_size  = d.broker.getvalue() * 0.01 / self.inds[d].ATR
            #     d.buy_size  = int(d.buy_size  / 100) * 100
            #     d.sizer.p.stake = d.buy_size
            #     d.order = self.buy()
            #     d.buy_count += 1
            # #离场：价格跌破下轨线且持仓时
            # elif self.sell_signal < 0 and self.d.buy_count > 0:
            #     self.order = self.sell()
            #     self.d.buy_count = 0
            # #止损：价格跌破买入价的2个ATR且持仓时
            # elif self.d.close < (self.d.buyprice - 2*self.inds[d].ATR[0]) and self.d.buy_count > 0:
            #     self.order = self.sell()
            #     self.d.buy_count = 0




class TradeSizer(bt.Sizer):
    pass
    # params = (('stake', 1),)
    # def _getsizing(self, comminfo, cash, data, isbuy):
    #     if isbuy:
    #         return self.p.stake
    #     position = self.broker.getposition(data)
    #     if not position.size:
    #         return 0
    #     else:
    #         return position.size
    #     return self.p.stake





if __name__ == '__main__':
    cerebro = bt.Cerebro()
    cerebro.broker.setcash(10000000000.0)
    cerebro.broker.setcommission(commission=0.001)
    cerebro.addstrategy(TurtleStrategy)
    for code in stocklist_allA[1:5]:
        feed = Addmoredata(dataname = get_stock_data(code),name=code)
        cerebro.adddata(feed)
    # cerebro.addsizer(TradeSizer)
    cerebro.run()
    cerebro.plot(volume=False)








