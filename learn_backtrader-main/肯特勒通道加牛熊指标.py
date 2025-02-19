import pandas as pd
import backtrader as bt
import quantstats
import warnings
warnings.filterwarnings("ignore")
import pysnowball as ball
import datetime
import tushare as ts
import backtrader.analyzers as btanalyzers


#操作逻辑,利用肯特那指标回测时发现回撤较之前40有很好的调整，想进场时候用牛熊做多，离场时候用肯特那通道离场



#读数据
'''token'''
pro = ts.pro_api('170765a888b06db9964fe52a90fee167216e52ba3452ed8e7b19fc8d')
ball.set_token('xq_a_token=16534528effa8d3606211a60a5e154a489ff3112;')


#利用tushare把历史数据读取

start_date='20010101'
middle_date='20101231'
next_date='20110101'
end_date=datetime.datetime.now().strftime('%Y%m%d')
#这里改指数代码
code='000300.SH'
df_start= pro.index_dailybasic(ts_code=code,start_date=start_date,end_date=middle_date, fields='ts_code,trade_date,turnover_rate')
df_end= pro.index_dailybasic(ts_code=code,start_date=next_date,end_date=end_date, fields='ts_code,trade_date,turnover_rate')
index_dailybasic=pd.concat([df_start,df_end])

df_dailydata_start = pro.index_daily(ts_code=code, start_date=start_date, end_date=middle_date)
df_dailydata_end= pro.index_daily(ts_code=code, start_date=next_date, end_date=end_date)
index_daily=pd.concat([df_dailydata_start,df_dailydata_end])


df=pd.merge(index_dailybasic,index_daily,on='trade_date')
df=df[['ts_code_x','open','high','low','close','pct_chg','turnover_rate','vol','trade_date']]
df.columns=['ts_code','open','high','low','close','pct_chg','turnover_rate','volume','trade_date']
df=df.sort_values('trade_date')


#利用雪球把今日得数据读取
#这里改指数代码
bs='SH000300'
index_now=ball.quotec(bs)
index_today=pd.DataFrame(index_now['data'][0],index=[0])
#把雪球代码变成tushare代码
index_today['ts_code']=index_today['symbol'].str.slice(2,8)+'.SH'
# index_today['ts_code']=index_today['symbol'].apply(lambda x:x[2:])+'.SH'
index_today=index_today[['ts_code','open','high','low','current','percent','turnover_rate','volume']]
index_today['trade_date']=end_date
index_today.columns=['ts_code','open','high','low','close','pct_chg','turnover_rate','volume','trade_date']


#把2数据合并
data=pd.concat([df,index_today])
#若晚上或者节假日读数据，删除掉多余日期
data=data.drop_duplicates(subset='trade_date',keep='first')
data['trade_date']=pd.to_datetime(data['trade_date'])
data=data.set_index('trade_date')
data=data[['open','high','low','close','volume','turnover_rate']]



#加载换手率数据
class GenericCSVData_turnover(bt.feeds.PandasData):
    lines = ('turnover',)
    params = (
        ('dtformat', '%Y%m%d'),
        ('turnover', 5),
    )

#计算肯特勒指标
class Ketler(bt.Indicator):
    params = dict(ema=20, atr=17)
    lines=('expo', 'atr', 'upper', 'lower')
    plotinfo = dict(subplot=False)
    plotlines = dict(upper=dict(ls='--'),lower=dict(_samecolor=True))
#这里上下轨应该是ema加减2倍ATR，我跑后发现1倍更有效
    def __init__(self):
        self.l.expo = bt.talib.EMA(self.datas[0].close, timeperiod=self.params.ema)
        self.l.atr = bt.talib.ATR(self.data.high, self.data.low, self.data.close, timeperiod=self.params.atr)
        self.l.upper = self.l.expo + self.l.atr
        self.l.lower = self.l.expo - self.l.atr








class Strategy(bt.Strategy):
    params = (
        ('turnover_period', 120),
        ('turnoverband_period', 20),
        ('pct_period', 120),
    )


    def log(self, txt, dt=None):
        ''' Logging function fot this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))


    def __init__(self):
        self.ketler = Ketler()
        self.close = self.datas[0].close
        self.dataturnover = self.datas[0].turnover
        pct = bt.ind.PctChange(self.close, period=1, subplot=False) * 100
        pct_std = bt.ind.StandardDeviation(pct, period=self.p.pct_period)
        turnover_ma = bt.ind.SimpleMovingAverage(self.dataturnover, period=self.p.turnover_period)
        self.kernel_index = pct_std / turnover_ma
        self.bband = bt.ind.BollingerBands(self.kernel_index, period=20)
        self.buy_sig = bt.ind.CrossDown(self.kernel_index, self.bband.bot)
        self.sell_sig = bt.ind.CrossUp(self.kernel_index, self.bband.top)


    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, Price: {:.2f}, Cost: {:.2f}, Comm {:.2f}'.format(
                        order.executed.price,
                        order.executed.value,
                        order.executed.comm))

                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:  # Sell
                self.log('SELL EXECUTED, Price: {:.2f}, Cost: {:.2f}, Comm {:.2f}'.format(
                    order.executed.price,
                    order.executed.value,
                    order.executed.comm))

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
                 (trade.pnl, trade.pnlcomm))


    def next(self):
        if not self.position:
            if self.buy_sig:
                self.order = self.buy()
        else:
            if self.close[0] < self.ketler.lower[0]:
                self.order = self.sell()





if __name__ == '__main__':
    cerebro = bt.Cerebro()
    data = GenericCSVData_turnover(dataname=data,
                                   fromdate=datetime.datetime(2005, 1, 4),
                                   todate=datetime.datetime(2021, 6, 4))

    cerebro.adddata(data)

    cerebro.addstrategy(Strategy)
    cerebro.broker.setcash(100000000)
    cerebro.broker.setcommission(commission=0)
    cerebro.addsizer(bt.sizers.PercentSizer, percents=98)
    cerebro.addanalyzer(btanalyzers.SharpeRatio, _name = 'sharpe')
    cerebro.addanalyzer(btanalyzers.DrawDown, _name = 'drawdown')
    cerebro.addanalyzer(btanalyzers.Returns, _name='returns')
    cerebro.addanalyzer(bt.analyzers.PyFolio, _name='PyFolio')

    print('Starte Portfolio Value {}'.format(cerebro.broker.getvalue()))
    back = cerebro.run()
    print('end portfolio value {}'.format(cerebro.broker.getvalue()))

    par_list = [[x.analyzers.returns.get_analysis()['rtot'],
                 x.analyzers.returns.get_analysis()['rnorm100'],
                 x.analyzers.drawdown.get_analysis()['max']['drawdown'],
                 x.analyzers.sharpe.get_analysis()['sharperatio']
                 ] for x in back]
    par_df = pd.DataFrame(par_list, columns=['Total Return','APR', 'Drawdown', 'SharpRatio'])
    print(par_df)


    # cerebro.plot(style='candle')
    #运行回测，保存结果
    strat = back[0]
    portfolio_stats = strat.analyzers.getbyname('PyFolio')
    returns, positions, transactions, gross_lev = portfolio_stats.get_pf_items()
    returns.index = returns.index.tz_convert(None)
    quantstats.reports.html(returns, output='Ketler_and_Niuxiong'+end_date+'.html', title='Ketler_牛熊策略')