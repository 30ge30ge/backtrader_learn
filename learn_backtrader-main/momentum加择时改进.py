import pandas as pd
import os
import numpy as np
import collections
from datetime import datetime, timedelta
import backtrader as bt
import matplotlib.pyplot as plt
from scipy.stats import linregress
import quantstats

#计算今天
today=(datetime.now()+timedelta(days=0)).strftime('%Y%m%d')


# 动量momentum函数
def momentum_func(the_array):
    r = np.log(the_array)
    slope, _, rvalue, _, _ = linregress(np.arange(len(r)), r)
    annualized = (1 + slope) ** 252
    return annualized * (rvalue ** 2)

# 动量indicator类，已将动量区间，动量函数参数化
class Momentum(bt.ind.PeriodN):
    lines = ('trend',)
    params = dict(period=50)
    func = momentum_func

    def next(self):
           self.lines.trend[0] = Momentum.func(self.data.get(size=self.p.period))


class indexdataextend(bt.feeds.PandasData):
    # 增加线
    lines = ('position', )
    params = (('position', 7),
              ('dtformat', '%Y-%m-%d'),)

class stockdataextend(bt.feeds.PandasData):

    params = (('dtformat', '%Y-%m-%d'),)

class Strategy(bt.Strategy):
    params = dict(
        momentum=Momentum,  # parametrize the momentum and its period
        momentum_period=88,

        movav=bt.ind.SMA,  # parametrize the moving average and its periods
        idx_period=180,
        stock_period=90,

        volatr=bt.ind.ATR,  # parametrize the volatility and its period
        vol_period=20,

        rebal_weekday=5  # rebalance 5 is Friday
    )

    def __init__(self):
        print('Strategy.__init__ start')
        self.i = 1
        self.o = dict()  # 追踪每只个股的订单
        self.inds = collections.defaultdict(dict)
        self.stocks = self.datas[1:]
        self.max_period = max(self.p.momentum_period, self.p.stock_period, self.p.vol_period)  # 计算个股指标所需要的最短历史数据
        self.d_universe = list()
        self.lastRanks = []

        self.holdings = list()
        self.f_values = open('log_value.txt', 'w+')
        self.f_positions = open('log_positions.txt', 'w+')
        self.f_positions.write(f'date, {[d._name for d in self.stocks]}\n')
        self.f_orders = open('log_orders.txt', 'w+')
        # self.d_test = list()

        self.idx_mav = self.p.movav(self.data0, period=self.p.idx_period)
        # 这里可以加入gap15作为指标
        for d in self.stocks:
            self.inds[d]['mom'] = self.p.momentum(d, period=self.p.momentum_period)
            self.inds[d]['mav'] = self.p.movav(d, period=self.p.stock_period)
            self.inds[d]['vol'] = self.p.volatr(d, period=self.p.vol_period)
            print(f'计算指标 for {d._name}, DONE.')

        self.add_timer(
            when=bt.Timer.SESSION_START,
            weekdays=[self.p.rebal_weekday],
            weekcarry=True,  # if a day isn't there, execute on the next
        )

        print('Strategy.__init__ done')

    def notify_order(self, order):  # 应该怎么写

        dt, dn = self.datetime.date(), order.data._name
        print(f'{dt} {dn} Order {order.ref} Status {order.getstatusname()} {order.exectype} {order.price}')

        if order.status in [order.Submitted, order.Accepted]:
            return
        if not order.alive():
            self.o[order.data] = None

    def prenext(self):
        self.next()

    def next(self):
        self.d_universe = [d for d in self.stocks if (d.datetime.date(0) == self.data.datetime.date(0))  # 数据是新的
                           and d.openinterest[0]>0  # 属于沪深300成分股
                           and (len(d) > self.max_period)  # 数据够长足以计算 指标
                           and d.volume[0]>0]
        print(f'{self.data.datetime.date(0)}: {len(self.d_universe)}')

        self.holdings = [d._name for d in self.d_universe if self.getposition(d).size]
        self.f_values.write(f'{self.data.datetime.date(0)} : {self.broker.get_value()}, {self.broker.get_cash()}, {len(self.holdings)}, {self.holdings}\n')
        sizes = [self.getposition(d).size for d in self.stocks]
        self.f_positions.write(f'{self.data.datetime.date(0)} : {sizes}\n')
        self.f_orders.write(f'{self.data.datetime.date(0)} :')
        for k, v in self.o.items():
            if v:
                self.f_orders.write(f'{k._name}:{v.ref} Status {v.getstatusname()}')
        self.f_orders.write('\n')


    def notify_timer(self, timer, when, *args, **kwargs):
        self.rebalance_portfolio()  # 执行再平衡
        print('周五调仓时间：', self.data0.datetime.date(0))

    def rebalance_portfolio(self):
        # 从指数取得当前日期
        self.currDate = self.data0.datetime.date(0)
        print('周五调仓时间：', self.data0.datetime.date(0),'统计股票数',len(self.stocks))
        if self.data0.position == 1:

            self.order_list = []  # 重置订单列表
            print('当前时间节点{}'.format(self.currDate), '择时指标做多')
            self.rankings = [d for d in self.d_universe if len(d) > self.p.stock_period]  # 留下足以计算均线的stock
            self.rankings.sort(key=lambda d: self.inds[d]['mom'][0], reverse=True)  # 根据momentum排名，注意此处应该降序！！
            num_stocks = len(self.rankings)

            # 退出环节
            # ==================================================================

            # sell stocks based on criteria，需要额外检查退出指数的个股！！
            for i, d in enumerate(self.rankings):
                if self.getposition(d).size and not self.o.get(d, None):  # 有持仓且没有未执行的订单
                    print(
                        f'd:{d._name}, size:{self.getposition(d).size:.2f}, close: {d.close[0]}, mav:{self.inds[d]["mav"][0]}')
                    if i > num_stocks * 0.2:
                        print(f'{d._name} 跌出排名前20%，退出')
                        self.o[d] = self.close(d)
                        # self.close(d)
                    elif d < self.inds[d]['mav']:
                        print(f'{d._name} 跌到均线下，退出')
                        self.o[d] = self.close(d)
            for d in self.stocks:  # 检查是否仍旧是指数成分股
                if self.getposition(d).size and (d not in self.rankings) and not self.o.get(d,None):  # 别忘了pending order
                    print(f'{d._name} 剔除指数成分股，退出')
                    self.o[d] = self.close(d)



            # ==================================================================
            # 买入环节
            # ==================================================================

            # 取消未完成的买单
            for k, v in self.o.items():
                if v:  # 如果v不是None
                    if v.isbuy():
                        self.broker.cancel(v)

            if self.data0 < self.idx_mav:  # 买入准则【1】指数均线下不买
                print('指数在均线下，禁止开新仓')
                return
            # buy stocks with remaining cash
            for i, d in enumerate(self.rankings[:int(num_stocks * 0.2)]):  # 买入准则【2】只看momentum前20%的股票
                cash = self.broker.get_cash()
                value = self.broker.get_value()
                if cash <= 0:  # 没钱不买
                    break
                if (not self.getposition(d).size) and (d > self.inds[d]['mav']) and not self.o.get(d,None):  # 买入准则【3】，之前没有持仓，且在个股均线上
                    size = value * 0.0015 / self.inds[d]['vol']  # 买入准则【4】按风险平价ATR决定买多少，无量/停牌时不买
                    self.o[d] = self.buy(d, size=size)
                    print(f'buy: {d._name}, size={size}')
            self.lastRanks = self.rankings  # 跟踪上次买入的标的


        else:
            print('当前时间节点{}'.format(self.currDate), '择时指标收仓')
            for d in self.lastRanks:
                if len(self.lastRanks) > 0:
                    print('平掉所有仓位')
                    self.close(data=d)


                else:
                    return

        # 如果是指数的最后一本bar，则退出，防止取下一日开盘价越界错
        # if len(self.datas[0]) == self.data0.buflen():
        #     return





if __name__ == '__main__':
    cerebro = bt.Cerebro()
    cerebro.broker.set_filler(bt.broker.fillers.FixedSize()) # 设置filler，阻止停牌期间的买入订单
    cerebro.broker.set_coo(True)  # Cheat on Open
    cerebro.broker.setcash(10000000.0)


    # 先把指数的行情adddata，作为data0
    df_index = pd.read_excel('D:/北京交接20201116/backtrader数据集/沪深指数带指标20210630.xlsx', encoding='utf-8',
                             parse_dates=['trade_date'])
    df_index['openinterest'] = -1
    df_index['volume'] = 1000000
    df_index = df_index[['trade_date', 'open', 'high', 'low', 'close', 'volume', 'openinterest', 'position']]
    df_index['trade_date']=pd.to_datetime(df_index['trade_date'],format = "%m/%d/%Y")

    df_index.index = df_index.trade_date
    df_index.drop(df_index.tail(1).index, inplace=True)
    print(df_index)


    # 统一设置日期，指数的区间，也是测试的区间

    from_idx = datetime(2020, 1, 1)  # 记录行情数据的开始时间和结束时间
    to_idx = datetime(2021, 6, 30)
    print(from_idx,to_idx)


    data0 = indexdataextend(dataname=df_index, fromdate=from_idx, todate=to_idx, name='idx')
    cerebro.adddata(data0)
    print('指数行情数据载入成功 DONE.')
    # 再逐一加入每个ticker的data
    # 数据存储路径 和 读取配置

    data_dir = pd.read_csv('D:/北京交接20201116/tushare数据/每日指标与每日行情数据/'+today+'每日指标.txt', parse_dates=True, index_col=['trade_date'])
    constituents = pd.read_csv('D:/北京交接20201116/tushare数据/沪深300历史成分_是否_20141231.csv', parse_dates=True, index_col=0)
    # # 读入股票代码
    out = list()
    for stock in constituents.columns.unique():
        # 日期对齐
        data = pd.DataFrame(index=df_index.index.unique()) # 获取回测区间内所有交易日
        df = data_dir.query(f"ts_code=='{stock}'")[['open','high','low','close','vol','ts_code']]
        df['vol'] = df['vol'] * 100
        df.rename(columns={'ts_code': 'openinterest', 'vol': 'volume'}, inplace=True)
        data_ = pd.merge(data, df, left_index=True, right_index=True, how='left')
        # 缺失值处理：日期对齐时会使得有些交易日的数据为空，所以需要对缺失数据进行填充
        data_.loc[:,['volume','openinterest']] = data_.loc[:,['volume','openinterest']].fillna(0)
        data_.loc[:,['open','high','low','close']] = data_.loc[:,['open','high','low','close']].fillna(method='pad')
        data_.loc[:,['open','high','low','close']] = data_.loc[:,['open','high','low','close']].fillna(0)
        code = stock
        data_ = data_.join(constituents[code], how='left', sort=True)
        data_.drop('openinterest', axis=1,inplace=True)
        data_.rename(columns={code: 'openinterest'}, inplace=True)
        data_.fillna(method='pad', inplace=True)




        # 导入数据
        datafeed = stockdataextend(dataname=data_, fromdate=from_idx, todate=to_idx)
        cerebro.adddata(datafeed, name=code) # 通过 name 实现数据集与股票的一一对应
        out.append(code)
        # print(len(out))
        print(f"{code} Done !")
    print('统计数量为{}'.format(len(out)),'Done !')

    # 载入策略
    cerebro.addstrategy(Strategy)
    print('add strategy DONE.')
    cerebro.addanalyzer(bt.analyzers.PyFolio, _name='PyFolio')
    print('add analyzers DONE.')

    start_portfolio_value = cerebro.broker.getvalue()
    results = cerebro.run()
    strat = results[0]
    end_portfolio_value = cerebro.broker.getvalue()
    pnl = end_portfolio_value - start_portfolio_value
    # cerebro.plot(volume=False)
    # 输出结果、生成报告、绘制图表
    print(f'初始本金 Portfolio Value: {start_portfolio_value:.2f}')
    print(f'最终本金和 Portfolio Value: {end_portfolio_value:.2f}')
    print(f'利润PnL: {pnl:.2f}')

    portfolio_stats = strat.analyzers.getbyname('PyFolio')
    print(portfolio_stats)
    returns, positions, transactions, gross_lev = portfolio_stats.get_pf_items()

    returns.index = returns.index.tz_convert(None)
    return_pd = pd.DataFrame(returns)
    return_pd['nav_timing'] = (1 + return_pd['return']).cumprod()
    print(return_pd)
    return_pd.to_csv('D:/北京交接20201116/tushare数据/monmentums_300_return.csv',encoding='utf_8')

    quantstats.reports.html(returns, output='hs300momentum' + today + '.html', title='动量+牛熊择时进行回测')

    fig = plt.figure(figsize=(18, 8))
    y2 = return_pd['nav_timing']
    plt.plot(y2, label='monmentums_300', linewidth=2)
    plt.title("monmentums_300")
    plt.legend(loc='best')
    plt.show()