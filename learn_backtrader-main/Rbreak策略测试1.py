#经典rbreak策略复现
import pandas as pd
import backtrader as bt
# import quantstats
import akshare as ak



class Databasic():
    def __init__(self,code,start,end):
        self.code = code
        self.start = start
        self.end = end

    '''获取股票代码日线数据'''
    def get_data(self):
        df=ak.stock_zh_a_hist(self.code,period='daily',start_date=self.start,end_date=self.end)
        df.index = pd.to_datetime(df.日期)
        df = df[['开盘', '最高', '最低', '收盘', '成交量']]
        df['openinterest'] = 0
        columns = ['open', 'high', 'low', 'close', 'volume', 'openinterest']
        df.columns = columns
        return df

    '''获取股票代码分钟数据'''
    def get_data_m(self):
        df = ak.stock_zh_a_hist_min_em(self.code, period='5', start_date=self.start, end_date=self.end)
        df.index = pd.to_datetime(df.时间)
        df = df[['开盘', '最高', '最低', '收盘', '成交量']]
        df['openinterest'] = 0
        columns = ['open', 'high', 'low', 'close', 'volume', 'openinterest']
        df.columns = columns
        return df

    def Rbreak_data(self):
        df = self.get_data()
        df['pivot'] =(df['high'].shift()+df['low'].shift()+df['close'].shift())/3  #'中枢点'
        df['bBreak'] = df['high'].shift() + 2 * (df['pivot'] - df['low'].shift())  # 突破买入价
        df['sSetup'] = df['pivot'] + (df['high'].shift() - df['low'].shift())  # 观察卖出价
        df['sEnter'] = 2 * df['pivot'] - df['low'].shift()  # 反转卖出价
        df['bEnter'] = 2 * df['pivot'] - df['high'].shift()  # 反转买入价
        df['bSetup'] = df['pivot'] - (df['high'].shift() - df['low'].shift())  # 观察买入价
        df['sBreak'] = df['low'].shift() - 2 * (df['high'].shift() - df['pivot'])  # 突破卖出价
        return df



class PandasDataExtend(bt.feeds.PandasData):
    # 增加线
    lines = ('pivot','bBreak','sSetup','sEnter','bEnter','bSetup','sBreak',)
    params = (('pivot', -1), ('bBreak',-1),('sSetup',-1),('sEnter',-1),('bEnter',-1),('bSetup',-1),('sBreak',-1),
              ('dtformat', '%Y-%m-%d'),)

class R_BreakStrategy(bt.Strategy):
    params = (
        ('lowestperiod', 5),
        ('trailamount', 0.0),
        ('trailpercent', 0.05),
    )
    # 日志函数
    def log(self, txt, dt=None):
        # 以第一个数据data0，即指数作为时间基准
        dt = dt or self.data0.datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        self.order = None

        self.dataclose = self.datas[1].close #5分钟收盘价
        self.datahigh = self.datas[0].high
        self.datalow = self.datas[0].low
        #日线收盘rbreak指标
        self.bBreak = self.datas[0].bBreak
        self.sSetup = self.datas[0].sSetup
        self.sEnter = self.datas[0].sEnter
        self.bEnter = self.datas[0].bEnter
        self.bSetup = self.datas[0].bSetup
        self.sBreak = self.datas[0].sBreak




    def next(self):
        # print(self.dataclose[0],self.bBreak[0])
        if self.order:
            return
        #趋势追踪
        if not self.position:
            if self.dataclose[0]>self.bBreak[0]:#突破买入价
                self.log('突破买入成功, %.2f' % self.dataclose[0])
                self.order = self.buy(size=100000)
            elif self.dataclose[0]<self.sBreak[0]:#突破卖出价
                self.log('反转卖出成功, %.2f' % self.dataclose[0])
                self.order = self.sell(size=100000)
        # 反转策略
            # 多头持仓,当日内最高价超过观察卖出价后，
            # 盘中价格出现回落，且进一步跌破反转卖出价构成的支撑线时，
            # 采取反转策略，即在该点位反手做空
        if self.getposition().size !=0 :
           if self.datahigh[0]> self.sSetup[0] and self.dataclose[0]<self.sEnter[0]:
               self.order = self.close()
               self.log('反转卖出成功, %.2f' % self.dataclose[0])
               self.order = self.sell(size=1000000)
           # 空头持仓，当日内最低价低于观察买入价后，
           # 盘中价格出现反弹，且进一步超过反转买入价构成的阻力线时，
           # 采取反转策略，即在该点位反手做多
           elif self.datalow[0]> self.bSetup[0] and self.dataclose[0]>self.bEnter[0]:
               self.order = self.close()
               self.log('反转买入成功, %.2f' % self.dataclose[0])
               self.order = self.buy(size=100000)

        #5%止损点
        self.order = self.close(data=self.datas[0], exectype=bt.Order.StopTrail,
                                          trailamount=self.p.trailamount,
                                          trailpercent=self.p.trailpercent)

    #
    # 记录交易收益情况
    def notify_trade(self, trade):
        if trade.isclosed:
            print('毛收益 %0.2f, 扣佣后收益 % 0.2f, 佣金 %.2f, 市值 %.2f, 现金 %.2f' %
                  (trade.pnl, trade.pnlcomm, trade.commission, self.broker.getvalue(), self.broker.getcash()))


##########################
# 主程序开始
#########################
from datetime import datetime,timedelta
if __name__ == '__main__':
    cerebro = bt.Cerebro(stdstats=False, quicknotify=True)
    cerebro.broker.set_filler(bt.broker.fillers.FixedSize()) # 设置filler，阻止停牌期间的买入订单
    cerebro.broker.set_coo(True)  # Cheat on Open
    cerebro.broker.setcash(1000000.0)

    df=Databasic('601919','20230401','20230420')# 取日线和5分钟数据
    df_dayly = df.Rbreak_data()
    df_m = df.get_data_m()

    from_idx = datetime(2023, 4, 1)  # 记录行情数据的开始时间和结束时间
    to_idx = datetime(2023, 4, 20)


    data0 = PandasDataExtend(dataname=df_dayly,fromdate=from_idx, todate=to_idx,plot=False)
    data1 = bt.feeds.PandasData(dataname=df_m,fromdate=from_idx,todate=to_idx,timeframe=bt.TimeFrame.Minutes)
    cerebro.adddata(data0)
    cerebro.adddata(data1)
    print(df_dayly,df_m)

    # 载入策略
    cerebro.addstrategy(R_BreakStrategy)
    print('add strategy DONE.')
    cerebro.addanalyzer(bt.analyzers.PyFolio, _name='PyFolio')
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='_TimeReturn')

    print('add analyzers DONE.')
    start_portfolio_value = cerebro.broker.getvalue()
    results = cerebro.run()
    strat = results[0]
    end_portfolio_value = cerebro.broker.getvalue()
    pnl = end_portfolio_value - start_portfolio_value
    # 输出结果、生成报告、绘制图表
    print(f'初始本金 Portfolio Value: {start_portfolio_value:.2f}')
    print(f'最终本金和 Portfolio Value: {end_portfolio_value:.2f}')
    print(f'利润PnL: {pnl:.2f}')
    # Plot the result绘制结果
    cerebro.plot(volume=False, style='candle', barup='red', bardown='green')

    # portfolio_stats = strat.analyzers.getbyname('PyFolio')
    # # print(portfolio_stats)
    # returns, positions, transactions, gross_lev = portfolio_stats.get_pf_items()
    # returns.index = returns.index.tz_convert(None)
    #
    # # #取时间
    # today = (datetime.now() + timedelta(days=0)).strftime('%Y%m%d')
    # quantstats.reports.html(returns, output='rbreak' + today + '.html', title='rbreak策略')
