import pandas as pd
import numpy as np
import backtrader as bt
import tushare as ts
from datetime import datetime
from datetime import timedelta


'''获取股票代码日线数据'''
def get_data(code,start,end):
    df=ts.get_k_data(code,autype='qfq',start=start,end=end)
    df.index=pd.to_datetime(df.date)
    df['openinterest']=0
    df=df[['open','high','low','close','volume','openinterest']]
    return df

class Boll_strategy(bt.Strategy):

    # 自定义参数，每次买入100手
    params=(('size',1500000),)

    def log(self, txt, dt=None, doprint=False):
        ''' 日志函数，用于统一输出日志格式 '''
        if doprint:
            dt = dt or self.datas[0].datetime.date(0)
            print('%s, %s' % (dt.isoformat(), txt))


    def __init__(self):
        self.dataclose = self.datas[0].close

        self.order=None
        self.buyprice=None
        self.buycomm=None
        ##使用自带的indicators中自带的函数计算出支撑线和压力线，period设置周期，默认是20
        self.lines.mid=bt.indicators.BollingerBands(self.datas[0],period=20,devfactor=0.5).mid
        self.lines.bot=bt.indicators.BollingerBands(self.datas[0],period=20,devfactor=0.5).bot
    def next(self):
        if not self.position:
            if self.dataclose<=self.lines.bot[0]:
                #执行买入
                self.order=self.buy(size=self.params.size)
        else:
            if self.dataclose>=self.lines.mid[0]:
                #执行卖出
                self.order=self.sell(size=self.params.size)
class ChinaBuySell(bt.observers.BuySell):
    plotlines = dict(
        buy=dict(marker='^', markersize=9.0, color='red'),
        sell=dict(marker='v', markersize=9.0, color='lime')
    )





if __name__ == '__main__':

    start = datetime(2021, 6, 1).strftime("%Y-%m-%d")
    end = datetime(2021, 12, 26).strftime("%Y-%m-%d")
    dataframe = get_data('510300', start=start, end=end)
    # 初始化模型
    cerebro = bt.Cerebro()

    # 构建策略
    strats = cerebro.addstrategy(Boll_strategy)
    # 每次买100股
    # cerebro.addsizer(bt.sizers.FixedSize, stake=100)

    # 加载数据到模型中
    data = bt.feeds.PandasData(dataname=dataframe)

    cerebro.adddata(data)

    # 设定初始资金和佣金
    cerebro.broker.setcash(10000000.0)
    cerebro.broker.setcommission(0.005)

    # 策略执行



    start_portfolio_value = cerebro.broker.getvalue()
    print('启动资金: %.2f' % start_portfolio_value)
    cerebro.run()
    end_portfolio_value = cerebro.broker.getvalue()
    print('最终资金: %.2f' % end_portfolio_value)
    pnl = end_portfolio_value - start_portfolio_value
    print(f'净值PnL: {pnl:.2f}')
    cerebro.plot(style='candle')











