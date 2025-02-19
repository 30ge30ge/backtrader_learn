#经典rbreak策略复现
import pandas as pd
import backtrader as bt
import quantstats
import akshare as ak



class Databasic():
    def __init__(self,code,start,end):
        self.code = code
        self.start = start
        self.end = end

    '''获取股票代码分钟数据'''
    def get_data_m(self):
        df = ak.stock_zh_a_hist_min_em(self.code, period='5', start_date=self.start, end_date=self.end)
        df.index = pd.to_datetime(df.时间)
        df = df[['开盘', '最高', '最低', '收盘', '成交量']]
        df['openinterest'] = 0
        columns = ['open', 'high', 'low', 'close', 'volume', 'openinterest']
        df.columns = columns
        return df


class rbreak_Line(bt.Indicator):
    lines = ('pivot', 'bBreak', 'sSetup', 'sEnter', 'bEnter', 'bSetup', 'sBreak',)
    params = (('period', 2),)

    def __init__(self):
        self.addminperiod(self.p.period + 1)

    def next(self):
        High = self.data.high.get(ago=0, size=self.p.period)[-1]
        low = self.data.low.get(ago=0, size=self.p.period)[-1]
        close = self.data.close.get(ago=0, size=self.p.period)[-1]
        pivot =(High + low + close)/3
        self.lines.pivot[0] = pivot #'中枢点
        self.lines.bBreak[0] = High+ 2*(pivot-low) # 突破买入价
        self.lines.sSetup[0] = pivot + (High - low)# 观察卖出价
        self.lines.sEnter[0] = 2 * pivot - low# 反转卖出价
        self.lines.bEnter[0] = 2 * pivot - High# 反转买入价
        self.lines.bSetup[0] = pivot - (High - low)# 观察买入价
        self.lines.sBreak[0] = low - 2 * (High - pivot)  # 突破卖出价




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

        self.dataclose = self.datas[0].close #5分钟收盘价
        self.rbreak_Line = rbreak_Line(self.datas[1])
        self.rbreak_Line = self.rbreak_Line()
        self.datahigh = self.datas[1].high
        self.datalow = self.datas[1].low
        #日线收盘rbreak指标
        self.bBreak = self.rbreak_Line.bBreak
        self.sSetup = self.rbreak_Line.sSetup
        self.sEnter = self.rbreak_Line.sEnter
        self.bEnter = self.rbreak_Line.bEnter
        self.bSetup = self.rbreak_Line.bSetup
        self.sBreak = self.rbreak_Line.sBreak




    def next(self):

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
    # # 记录交易收益情况
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

    df=Databasic('601919','20230202','20230302')# 取日线和5分钟数据
    df_m = df.get_data_m()
    # print(df_m)

    from_idx = datetime(2023, 2, 3)  # 记录行情数据的开始时间和结束时间
    to_idx = datetime(2023, 3, 2)

    data0 = bt.feeds.PandasData(dataname=df_m,fromdate=from_idx,todate=to_idx,timeframe=bt.TimeFrame.Minutes)
    cerebro.adddata(data0)
    cerebro.resampledata(data0,timeframe=bt.TimeFrame.Days)


    # 载入策略
    cerebro.addstrategy(R_BreakStrategy)
    print('add strategy DONE.')
    # cerebro.addanalyzer(bt.analyzers.PyFolio, _name='PyFolio')
    # cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='_TimeReturn')

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
    #
    # portfolio_stats = strat.analyzers.getbyname('PyFolio')
    # # # print(portfolio_stats)
    # returns, positions, transactions, gross_lev = portfolio_stats.get_pf_items()
    # returns.index = returns.index.tz_convert(None)
    # #
    # # #取时间
    # today = (datetime.now() + timedelta(days=0)).strftime('%Y%m%d')
    # quantstats.reports.html(returns, output='rbreak' + today + '.html', title='rbreak策略')
