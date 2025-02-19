
import pandas as pd
import datetime
import backtrader as bt
import quantstats

#取时间
today=(datetime.datetime.now()+datetime.timedelta(days=0)).strftime('%Y%m%d')



df_stock=pd.read_csv('D:/动量策略/zz1000历史成分股动量指标20211221.txt',parse_dates=['trade_date'])
df_stock['trade_date']=pd.to_datetime(df_stock['trade_date'],format='%Y%m%d')
# df_stock.rename(columns={'Unnamed: 0':'ts_code'},inplace=True)
print(df_stock,df_stock.columns)
#选取指数成分股个股
# df_stock=df_stock[df_stock['index']=='000852.SH']
stocklist_allA = df_stock['ts_code'].unique().tolist()
df_stock['openinterest']=1
df_stock['volume']=df_stock['volume']*100
df_stock = df_stock[['trade_date','open','high','low','close','volume','openinterest','momentum5','momentum22','turnover_rate','turnover_rate_up','pct_chg_15','vol_chg_15','ts_code']]
df_stock.index=df_stock.trade_date
print(df_stock.columns)



class indexdataextend(bt.feeds.PandasData):
    # 增加线多因子回测19之沪深500历史成分股MVP周动量调仓2次调整.py
    lines = ('position', )
    params = (('position', 7),
              ('dtformat', '%Y-%m-%d'),)
#
#
#
#
class PandasDataExtend(bt.feeds.PandasData):
    # 增加线
    lines = ('momentum5','momentum22','turnover_rate','turnover_rate_up','pct_chg_15','vol_chg_15')
    params = (('momentum5', 7),('momentum22', 8),('turnover_rate', 9),('turnover_rate_up',10),('pct_chg_15',-1),('vol_chg_15',-1),('dtformat','%Y-%m-%d'),)
#
# #
# #
# #
class Strategy(bt.Strategy):
    params = dict(
        num_volume=50,  # 取前30名
        period=20,
        rebal_weekday1=3,  # 两次调仓
        rebal_weekday2=5
    )
#
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
        self.book = open('log_holding.txt', 'w+')
        self.f_positions = open('log_positions.txt', 'w+')

        # 移动平均线指标
        self.sma = {d: bt.ind.SMA(d, period=self.p.period) for d in self.stocks}

        # 定时器
        self.add_timer(
            when=bt.Timer.SESSION_START,
            weekdays=[self.p.rebal_weekday1],
            weekcarry=True,  # if a day isn't there, execute on the next
            timername='rebaltimer1'
        )
        self.add_timer(
            when=bt.Timer.SESSION_START,
            weekdays=[self.p.rebal_weekday2],
            weekcarry=True,  # if a day isn't there, execute on the next
            timername='rebaltimer2'
        )

    def notify_timer(self, timer, when, *args, **kwargs):
        timername = kwargs.get('timername', None)
        if timername == 'rebaltimer1':
            self.rebalance_portfolio()  # 执行再平衡
            print('调仓时间：', self.data0.datetime.date(0))
        elif timername == 'rebaltimer2':
            self.rebalance_portfolio()  # 执行再平衡
            print('调仓时间：', self.data0.datetime.date(0))

    def next(self):

        print('next 账户总值', self.data0.datetime.datetime(0), self.broker.getvalue())
        for d in self.stocks:
            if(self.getposition(d).size!=0):
                self.book.write(f'code:{d._name},holding: {self.getposition(d).size}\n')


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

    # 记录交易收益情况
    def notify_trade(self, trade):
        if trade.isclosed:
            print('毛收益 %0.2f, 扣佣后收益 % 0.2f, 佣金 %.2f, 市值 %.2f, 现金 %.2f' %
                  (trade.pnl, trade.pnlcomm, trade.commission, self.broker.getvalue(), self.broker.getcash()))

    def rebalance_portfolio(self):
        # 从指数取得当前日期
        self.currDate = self.data0.datetime.date(0)
        print('rebalance_portfolio currDate', self.currDate, len(self.stocks))

        # 如果是指数的最后一本bar，则退出，防止取下一日开盘价越界错
        if len(self.datas[0]) == self.data0.buflen():
            return

            # 取消以往所下订单（已成交的不会起作用）
        for o in self.order_list:
            self.cancel(o)
        self.order_list = []  # 重置订单列表

        # for d in self.stocks:
        #     print('sma', d._name, self.sma[d][0],self.sma[d][1], d.marketdays[0])

        # 最终标的选取过程
        # 1 先做排除筛选过程
        self.ranks = [d for d in self.stocks if
                      len(d) > 0
                      and d.datetime.date(0) == self.currDate
                      and len(d) >= self.p.period
                      and d.close[0] > self.sma[d][0]
                      and d.pct_chg_15[0] > 0.1
                      and d.vol_chg_15[0] > 0.1
                      and d.momentum5[0] > 10
                      and d.volume > 1
                      and d.turnover_rate[0] > d.turnover_rate_up[0]
                      ]

        # 2 再做排序挑选过程
        self.ranks.sort(key=lambda d: d.momentum5, reverse=True)  # 按收益率最大值从小到大排序
        # self.ranks = self.ranks[0:self.p.num_volume]  # 取前num_volume名
        if len(self.ranks) != 0:
            for i, d in enumerate(self.ranks):
                print(f'选股第{i+1}名,{d._name},momtum5值: {d.momentum5[0]},momtum22值: {d.momentum22[0]},'
                      f'turnover_rate值: {d.turnover_rate[0]},turnover_rate_up值: {d.turnover_rate_up[0]}')
                self.f_positions.write(f'{d.datetime.date(0)},code:{d._name},momtum5值: {d.momentum5[0]},momtum22值: {d.momentum22[0]}\n')
        else: # 无股票选入
            return
        #
        # if len(self.ranks) == 0:  # 无股票选中，则返回
        #     return

        # 3 以往买入的标的，本次不在标的中，则先平仓
        data_toclose = set(self.lastRanks) - set(self.ranks)
        for d in data_toclose:
            print('不在本次选股股票池里：sell平仓', d._name, self.getposition(d).size)
            o = self.close(data=d)
            self.order_list.append(o)  # 记录订单


        # 5 本次标的下单
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
                abs((self.broker.getvalue([d]) - targetvalue) / d.open[0] // 100 * 100))
            validday = d.datetime.datetime(1)  # 该股下一实际交易日
            if self.broker.getvalue([d]) > targetvalue:  # 持仓过多，要卖
                # 次日跌停价近似值
                lowerprice = d.close[0] * 0.9 + 0.03

                o = self.sell(data=d, size=size, exectype=bt.Order.Limit,valid=validday,price=lowerprice)
            else:  # 持仓过少，要买
                # 次日涨停价近似值,涨停值过滤不买
                upperprice = d.close[0] * 1.1 - 0.03
                o = self.buy(data=d, size=size, exectype=bt.Order.Limit,valid=validday,price=upperprice)

            self.order_list.append(o)  # 记录订单

        self.lastRanks = self.ranks  # 跟踪上次买入的标的


##########################
# 主程序开始
#########################
from datetime import datetime
if __name__ == '__main__':
    cerebro = bt.Cerebro(stdstats=False, quicknotify=True)
    cerebro.broker.set_filler(bt.broker.fillers.FixedSize()) # 设置filler，阻止停牌期间的买入订单
    cerebro.broker.set_coo(True)  # Cheat on Open
    cerebro.broker.setcash(10000000.0)


    # 先把指数的行情adddata，作为data0
    df_index = pd.read_excel('D:/动量策略/中证1000指数带指标20211202.xlsx',parse_dates=['trade_date'])
    df_index['openinterest'] = -1
    df_index['volume'] = 1000000
    df_index = df_index[['trade_date', 'open', 'high', 'low', 'close', 'volume', 'openinterest', 'position']]
    df_index['trade_date']=pd.to_datetime(df_index['trade_date'])
    df_index.index = df_index.trade_date
    df_index.drop(df_index.tail(1).index, inplace=True)
    df_index=df_index[(df_index['trade_date']>'2016-01-01')&(df_index['trade_date']<'2021-12-01')]
    print(df_index)


    # 统一设置日期，指数的区间，也是测试的区间

    from_idx = datetime(2016, 1, 1)  # 记录行情数据的开始时间和结束时间
    to_idx = datetime(2021, 11, 30)
    print(from_idx,to_idx)


    data0 = indexdataextend(dataname=df_index, fromdate=from_idx, todate=to_idx, name='idx')
    cerebro.adddata(data0)
    print('指数行情数据载入成功 DONE.')


    # 再逐一加入每个ticker的data
    out = list()
    for fn in stocklist_allA:
        data = pd.DataFrame(index=df_index.index.unique())
        df = df_stock[df_stock['ts_code'] == fn].sort_index()
        df = df.sort_index()
        df = df[['trade_date','open','high','low','close','volume','openinterest','momentum5','momentum22','turnover_rate','turnover_rate_up','pct_chg_15','vol_chg_15','ts_code']]
        data_ = pd.merge(data, df, left_index=True, right_index=True, how='left')
        data_.loc[:, ['volume', 'openinterest']] = data_.loc[:, ['volume', 'openinterest']].fillna(0)
        data_.loc[:, ['open', 'high', 'low', 'close']] = data_.loc[:, ['open', 'high', 'low', 'close']].fillna(method='bfill')
        data_.fillna(method='bfill', inplace=True)
        data_.fillna(0, inplace=True)
        data_.dropna(subset=['open', 'high', 'low', 'close'], inplace=True)
        # print(data_)

        feed = PandasDataExtend(dataname=data_, name=fn,fromdate=from_idx, todate=to_idx,plot=False)
        cerebro.adddata(feed, name=fn)
        out.append(fn)

    print('统计数量为{}'.format(len(out)), 'Done !')
    # print(f'add data for {fn} DONE.')

    # 载入策略
    cerebro.addstrategy(Strategy)
    print('add strategy DONE.')
    cerebro.addanalyzer(bt.analyzers.PyFolio, _name='PyFolio')
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='_TimeReturn')

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
    # print(portfolio_stats)
    returns, positions, transactions, gross_lev = portfolio_stats.get_pf_items()
    returns.index = returns.index.tz_convert(None)
    return_pd = pd.DataFrame(returns)
    return_pd['nav_timing'] = (1 + return_pd['return']).cumprod()
    benchmark=df_index[['open','close']]
    return_benchmark=pd.merge(return_pd,benchmark,left_index=True,right_index=True)
    return_benchmark['pct'] = return_benchmark.close.pct_change(1).fillna(0)
    return_benchmark['nav'] = (1 + return_benchmark['pct']).cumprod()
    print(return_benchmark)
    # return_pd.to_csv('D:/动量策略/北向资金策略净值.csv', encoding='utf_8')
    quantstats.reports.html(returns, output='zz1000历史成分股16-21+mvp' + today + '.html', title='zz1000历史成分MVP策略')

    pnl = pd.Series(results[0].analyzers._TimeReturn.get_analysis())
    # 计算累计收益
    cumulative = (pnl + 1).cumprod()
    # 计算回撤序列
    max_return = cumulative.cummax()
    drawdown = (cumulative - max_return) / max_return
    # 计算收益评价指标
    import pyfolio as pf

    # 按年统计收益指标
    perf_stats_year = (pnl).groupby(pnl.index.to_period('y')).apply(
        lambda data: pf.timeseries.perf_stats(data)).unstack()
    # 统计所有时间段的收益指标
    perf_stats_all = pf.timeseries.perf_stats((pnl)).to_frame(name='all')
    perf_stats = pd.concat([perf_stats_year, perf_stats_all.T], axis=0)
    perf_stats_ = round(perf_stats, 4).reset_index()

    # 绘制图形
    import matplotlib.pyplot as plt

    # 设置字体 用来正常显示中文标签
    plt.rcParams['font.sans-serif'] = ['SimHei']
    # 用来正常显示负号
    plt.rcParams['axes.unicode_minus'] = False

    import matplotlib.ticker as ticker  # 导入设置坐标轴的模块

    plt.style.use('seaborn')  # plt.style.use('dark_background')

    fig, (ax0, ax1) = plt.subplots(2, 1, gridspec_kw={'height_ratios': [1.5, 4]}, figsize=(20, 8))
    cols_names = ['date', 'Annual\nreturn', 'Cumulative\nreturns', 'Annual\nvolatility',
                  'Sharpe\nratio', 'Calmar\nratio', 'Stability', 'Max\ndrawdown',
                  'Omega\nratio', 'Sortino\nratio', 'Skew', 'Kurtosis', 'Tail\nratio',
                  'Daily value\nat risk']

    # 绘制表格
    ax0.set_axis_off()  # 除去坐标轴
    table = ax0.table(cellText=perf_stats_.values,
                      bbox=(0, 0, 1, 1),  # 设置表格位置， (x0, y0, width, height)
                      rowLoc='right',  # 行标题居中
                      cellLoc='right',
                      colLabels=cols_names,  # 设置列标题
                      colLoc='right',  # 列标题居中
                      edges='open'  # 不显示表格边框
                      )
    table.set_fontsize(13)

    # 绘制累计收益曲线
    ax2 = ax1.twinx()
    ax1.yaxis.set_ticks_position('right')  # 将回撤曲线的 y 轴移至右侧
    ax2.yaxis.set_ticks_position('left')  # 将累计收益曲线的 y 轴移至左侧
    # 绘制回撤曲线
    drawdown.plot.area(ax=ax1, label='drawdown (right)', rot=0, alpha=0.3, fontsize=13, grid=False)
    # 绘制累计收益曲线
    (cumulative).plot(ax=ax2, color='#F1C40F', lw=3.0, label='cumret (left)', rot=0, fontsize=13, grid=False)
    # 不然 x 轴留有空白
    ax2.set_xbound(lower=cumulative.index.min(), upper=cumulative.index.max())
    # 主轴定位器：每 5 个月显示一个日期：根据具体天数来做排版
    ax2.xaxis.set_major_locator(ticker.MultipleLocator(100))
    # 同时绘制双轴的图例
    h1, l1 = ax1.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    plt.legend(h1 + h2, l1 + l2, fontsize=12, loc='upper left', ncol=1)

    fig.tight_layout()  # 规整排版
    plt.show()
# 绘制策略与基准曲线
    fig = plt.figure(figsize=(18, 8))
    plt.plot(return_benchmark['nav_timing'], label='strategy', linewidth=2)
    plt.plot(return_benchmark['nav'], label='Benchmark', linewidth=2)
    plt.title("Strategy-Benchmark")
    plt.legend(loc='best')
    plt.show()





