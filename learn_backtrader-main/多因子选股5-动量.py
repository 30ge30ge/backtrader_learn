import pandas as pd
from datetime import datetime,timedelta
import backtrader as bt
import quantstats
import matplotlib.pyplot as plt
#取时间
today=(datetime.now()+timedelta(days=0)).strftime('%Y%m%d')






class indexdataextend(bt.feeds.PandasData):
    # 增加线
    lines = ('position', )
    params = (('position', 7),
              ('dtformat', '%Y-%m-%d'),)



class PandasDataExtend(bt.feeds.PandasData):
    # 增加线
    lines = ('pe', 'pb','momentum',)
    params = (('pe', 6), ('pb', 7), ('momentum', 8),
              ('dtformat', '%Y-%m-%d'),)



class Strategy(bt.Strategy):
    params = dict(
        rebal_weekday=[5],  # 每月1日执行再平衡
        num_volume=30,  # 取前30名
        period=20,
    )

    # 日志函数
    def log(self, txt, dt=None):
        # 以第一个数据data0，即指数作为时间基准
        dt = dt or self.data0.datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):

        self.lastRanks = []  # 上次交易股票的列表
        # 0号是指数，不进入选股池，从1号往后进入股票池
        self.stocks = self.datas[1:]
        # 记录以往订单，在再平衡日要全部取消未成交的订单
        self.order_list = []

        # 移动平均线指标
        self.sma = {d: bt.ind.SMA(d, period=self.p.period) for d in self.stocks}

        # 定时器
        self.add_timer(
            when=bt.Timer.SESSION_START,
            monthdays=self.p.rebal_monthday,  # 每月1号触发再平衡
            monthcarry=True,  # 若再平衡日不是交易日，则顺延触发notify_timer
        )

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # 订单状态 submitted/accepted，无动作
            return

        # 订单完成
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log('买单执行,%s, %.2f, %i' % (order.data._name,order.executed.price, order.executed.size))

            elif order.issell():
                self.log('卖单执行, %s, %.2f, %i' % (order.data._name,order.executed.price, order.executed.size))

        else:
            self.log('订单作废 %s, %s, isbuy=%i, size %i, open price %.2f' %
                     (order.data._name, order.getstatusname(), order.isbuy(), order.created.size, order.data.open[0]))


    def notify_timer(self, timer, when, *args, **kwargs):
        self.rebalance_portfolio()  # 执行再平衡
        print('周五调仓时间：', self.data0.datetime.date(0),len(self.stocks))
        # 如果调仓日择时指标给出做多,调仓，否则平仓
        if data0.position == 1:
            print('当前时间节点{}'.format(self.currDate), '择时指标做多')

            # 取消以往所下订单（已成交的不会起作用）
            for o in self.order_list:
                self.cancel(o)
            self.order_list = []  # 重置订单列表

            # 最终标的选取过程
            # 1 先做排除筛选过程
            self.ranks = [d for d in self.stocks if
                          len(d) > 0  # 重要，到今日至少要有一根实际bar
                          # 今日未停牌 (若去掉此句，则今日停牌的也可能进入，并下订单，次日若复牌，则次日可能成交）（假设原始数据中已删除无交易的记录)
                          and d.datetime.date(0) == self.currDate
                          and d.pe > 0
                          and d.momentum > 0
                          and len(d) >= self.p.period
                          and d.close[0] > self.sma[d][1]
                          ]

            # 2 再做排序挑选过程
            self.ranks.sort(key=lambda d: d.momentum, reverse=True)  # 按营业利润增长率大到小排序
            self.ranks = self.ranks[0:self.p.num_volume]  # 取前num_volume名

            if len(self.ranks) == 0:  # 无股票选中，则返回
                return

            # 3 以往买入的标的，本次不在标的中，则先平仓
            data_toclose = set(self.lastRanks) - set(self.ranks)
            for d in data_toclose:
                print('不在本次选股股票池里：sell平仓', d._name, self.getposition(d).size)
                o = self.close(data=d)
                self.order_list.append(o)  # 记录订单

            # 4 本次标的下单
            # 每只股票买入资金百分比，预留2%的资金以应付佣金和计算误差
            buypercentage = (1 - 0.02) / len(self.ranks)

            # 得到目标市值
            targetvalue = buypercentage * self.broker.getvalue()
            # 为保证先卖后买，股票要按持仓市值从大到小排序
            self.ranks.sort(key=lambda d: self.broker.getvalue([d]), reverse=True)
            self.log('下单, 标的个数 %i, targetvalue %.2f, 当前总市值 %.2f' %
                     (len(self.ranks), targetvalue, self.broker.getvalue()))

            for d in self.ranks:
                # 按次日开盘价计算下单量，下单量是100的整数倍
                size = int(
                    abs((self.broker.getvalue([d]) - targetvalue) / d.open[1] // 100 * 100))
                validday = d.datetime.datetime(1)  # 该股下一实际交易日
                if self.broker.getvalue([d]) > targetvalue:  # 持仓过多，要卖
                    # 次日跌停价近似值
                    lowerprice = d.close[0] * 0.9 + 0.02

                    o = self.sell(data=d, size=size, exectype=bt.Order.Limit,
                                  price=lowerprice, valid=validday)
                else:  # 持仓过少，要买
                    # 次日涨停价近似值
                    upperprice = d.close[0] * 1.1 - 0.02
                    o = self.buy(data=d, size=size, exectype=bt.Order.Limit,
                                 price=upperprice, valid=validday)

                self.order_list.append(o)  # 记录订单

            self.lastRanks = self.ranks  # 跟踪上次买入的标的
        else:
            print('当前时间节点{}'.format(self.currDate), '择时指标收仓')
            for d in self.lastRanks:
                if len(self.lastRanks) > 0:
                    print('平掉所有仓位')
                    self.close(data=d)
                else:
                    return

        # 如果是指数的最后一本bar，则退出，防止取下一日开盘价越界错
        if len(self.datas[0]) == self.data0.buflen():
            return

if __name__ == '__main__':
    cerebro = bt.Cerebro()
    cerebro.broker.set_filler(bt.broker.fillers.FixedSize()) # 设置filler，阻止停牌期间的买入订单
    cerebro.broker.set_coo(True)  # Cheat on Open
    cerebro.broker.setcash(10000000.0)


    # 先把指数的行情adddata，作为data0
    df_index = pd.read_excel('/Users/30ge/Downloads/backtrader/data/沪深指数带指标20210706.xlsx')
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
    data_dir = pd.read_csv('/Users/30ge/Downloads/backtrader/data/today带动量的指标数据.txt', parse_dates=True, index_col=['trade_date'])
    data_dir['openinterest'] = 1
    data_dir['volume'] = data_dir['vol'] * 100
    stocklist_allA = data_dir['ts_code'].unique().tolist()

    # # # 读入股票代码
    out = list()
    for stock in stocklist_allA:
        # 日期对齐
        data = pd.DataFrame(index=df_index.index.unique()) # 获取回测区间内所有交易日
        df = data_dir.query(f"ts_code=='{stock}'")[['open','high', 'low','close', 'volume','openinterest', 'pe','pb', 'momentum']]
        data_ = pd.merge(data, df, left_index=True, right_index=True, how='left')
        # 缺失值处理：日期对齐时会使得有些交易日的数据为空，所以需要对缺失数据进行填充
        data_.loc[:,['volume','openinterest']] = data_.loc[:,['volume','openinterest']].fillna(0)
        data_.loc[:,['open','high','low','close']] = data_.loc[:,['open','high','low','close']].fillna(method='pad')
        data_.loc[:,['open','high','low','close']] = data_.loc[:,['open','high','low','close']].fillna(0)
        print(data_)

        # 导入数据
        datafeed = PandasDataExtend(dataname=data_, fromdate=from_idx, todate=to_idx)
        cerebro.adddata(datafeed, name=stock) # 通过 name 实现数据集与股票的一一对应
        out.append(stock)
        # print(len(out))
        print(f"{stock} Done !")
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
    return_pd.to_csv('/monmentums_allstock_return.csv', encoding='utf_8')

    quantstats.reports.html(returns, output='allstock_momentum' + today + '.html', title='动量+牛熊择时进行回测')

    fig = plt.figure(figsize=(18, 8))
    y2 = return_pd['nav_timing']
    plt.plot(y2, label='monmentums_300', linewidth=2)
    plt.title("monmentums_300")
    plt.legend(loc='best')
    plt.show()

