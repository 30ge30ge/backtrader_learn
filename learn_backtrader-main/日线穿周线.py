# 量化框架一般包括如下元素
# 1，Cerebro。大脑，统管以下所有元素.
# 2，Data。数据流，比如日线的股票。
# 3，Strategy。策略，买入/卖出的逻辑。
# 4，Signal。信号，一般在 stragegy 中使用。
# 5，Observer。观察指标，日线的话，每天都有值。
# 6，Analyzer。分析指标，回测完后计的指标。
# 7，Writer。将回测记录写入到文件。
# 8，Broker。券商，设置起始资金、佣金等。
#
# 多周期策略：
# ----
# brain.resampledata
# - 买点：周线多头排列，日线金叉，卖点：日线死叉
# ----------
# - 信号化
# '''
import os
import math
import datetime
import backtrader as bt
from backtrader.feeds import GenericCSVData
ROOT_PROJ = os.path.dirname(os.path.dirname(__file__)) # 项目根目录


class MyGenericCSV(GenericCSVData):
# 在默认 line 之外补充以下 line。
# line 是 backtrader 的核心数据类型，几乎可以说一切皆为line
    lines = ('price_change',
    'p_change',
    'ma5',
    'ma10',
    'ma20',
    'v_ma5',
    'v_ma10',
    'v_ma20',
    'turnover'
    )
    # 下面是对数据文件的解析配置
    params = (
    ('nullvalue', float('NaN')), # 缺省的值填写为 NaN，证券组合时一般会存在
    ('dtformat', '%Y-%m-%d'), # 日期的格式
    ("tmformat", "%H:%M:%S"), # 分时的格式

    ('datetime', 0), # 第0列为 datetime 类型，从零开始数，而非一开始数
    ('time', -1), # -1代表不存在，有 Datetime 一般就没有 time 这一列
    ('open', 1), # 第1列是开盘价
    ('high', 2), # 第2列是最高价
    ('close', 3), # 第3列是收盘价
    ('low', 4), # 第4列是最低价
    ('volume', 5), # 第5列是成效量
    ('openinterest', -1), # 第6列默认存在的，不存在的要填写-1
    ('price_change', 6), # 第6列是振幅，单价是金额
    ('p_change', 7), # 第7列是振幅，单价是百分比
    ('ma5', 8), # 第8列是5日收盘价的平均线
    ('ma10', 9), # 第9列是10日收盘价的平均线
    ('ma20', 10), # 第10列是20日收盘价的平均线
    ('v_ma5', 11), # 第11列是5日成效量的平均线
    ('v_ma10', 12), # 第12列是10日成效量的平均线
    ('v_ma20', 13), # 第13列是20日成效量的平均线
    ('turnover', 14) # 第14列是换手率，单是百分比
    )


class IndGoldenMA(bt.Indicator): # Indicator of golden moving average
    '''
    均线金叉策略：短周期均线上穿长周期均线，产生买入信号。短周期均线下穿长周期均线，产生买出信号。
    买入信号用1表示，卖出信号用-1表示，0表示非有效信号
    '''
    lines = ('buy_sell', ) # 输出1个 Line 数据类型, 可通过 self.lines 来引用
    # NOTE: 这里只输出1个 line 数据类型，那么逗号是必需的，这是为了让其是 tuple 类型，否则('buy_sell')就是一个字符串类型。
    params = ( # 支持参数输入，可通过 self.p 或 self.params 来引用
    ('fast', 10), # 参数名为 fast，默认值为10
    ('slow', 30), # 参数名为 slow, 默认值为30
    ('dataW', None)
    )

    def __init__(self):
        self.addminperiod(self.p.slow + 1) # 经过 self.p.slow + 1天(bar) 后才产生第一个值
        self.plotinfo.plot = True # 是否画出信号图，True 代表画，False 代表不画
        # self.plotinfo.plotmaster = self.data # 是否画到self.data(主图)上, self.data 是输入的数据流
        self.ma_fast = bt.indicators.MovingAverageSimple(self.data.close, period=self.params.fast) # 滑动平均
        self.ma_slow = bt.indicators.MovingAverageSimple(self.data.close, period=self.params.slow) # 滑动平均
        self.crossover = bt.indicators.CrossOver(self.ma_fast, self.ma_slow)

        if self.params.dataW is not None:
            ma_fast_w = bt.indicators.MovingAverageSimple(self.params.dataW, period=self.params.fast)
            ma_slow_w = bt.indicators.MovingAverageSimple(self.params.dataW, period=self.params.slow)
            self.signal_w = ma_fast_w > ma_slow_w

    def next(self):
        if self.params.dataW is None:
            self.buy_sell[0] = self.crossover[0]
        else:
            if self.crossover[0] == 1 and self.signal_w[0] == 1:
                self.buy_sell[0] = 1
            elif self.crossover[0] == -1:
                self.buy_sell[0] == -1
            else:
                self.buy_sell[0] = 0


class IndLongW(bt.Indicator):
    lines = ('buy_sell', ) # 输出1个 Line 数据类型, 可通过 self.lines 来引用
    params = ( # 支持参数输入，可通过 self.p 或 self.params 来引用
    ('fast', 10), # 参数名为 fast，默认值为10
    ('slow', 30), # 参数名为 slow, 默认值为30
    )
    def __init__(self):
        self.addminperiod(self.p.slow + 1) # 经过 self.p.slow + 1天(bar) 后才产生第一个值
        self.plotinfo.plot = True # 是否画出信号图，True 代表画，False 代表不画
        self.ma_fast_w = bt.indicators.MovingAverageSimple(self.data, period=self.params.fast)
        self.ma_slow_w = bt.indicators.MovingAverageSimple(self.data, period=self.params.slow)
        self.signal_w = self.ma_fast_w > self.ma_slow_w

    def next(self):
        if self.signal_w[0] == 1:
            self.buy_sell[0] = 1
        else:
            self.buy_sell[0] = -1


class BaseStrategy(bt.Strategy):
    params = ( # 支持参数输入，可通过 self.p 或 self.params 来引用
    ('fast', 10), # 参数名为 fast，默认值为10
    ('slow', 30), # 参数名为 slow, 默认值为30
    )

    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0) # 如果 dt=None，代表当天日期
        print('%s, %s' % (dt.isoformat(), txt)) # 打印日期与 txt 变量

    def __init__(self):
        self.ind = IndGoldenMA(self.datas[0], fast=self.params.fast, slow=self.params.slow, dataW=self.data1) # , dataW=self.data1
        self.ind_w = IndLongW(self.datas[1], fast=self.params.fast, slow=self.params.slow)
        # 画均线到日线上
        self.ma_fast = bt.indicators.MovingAverageSimple(self.data.close, period=self.params.fast) # bt 自带的均线计算方法，返回 line 数据类型
        self.ma_slow = bt.indicators.MovingAverageSimple(self.data.close, period=self.params.slow) # bt 自带的均线计算方法，返回 line 数据类型
        # 画均线到周线上
        self.ma_fast = bt.indicators.MovingAverageSimple(self.data1, period=self.params.fast) # bt 自带的均线计算方法，返回 line 数据类型
        self.ma_slow = bt.indicators.MovingAverageSimple(self.data1, period=self.params.slow) # bt 自带的均线计算方法，返回 line 数据类型

    def notify_order(self, order): # 触发条件 order 的状态改变
        if order.status in [order.Submitted, order.Accepted]:
            self.log('Order submitted/Accepted')
        # Buy/Sell order submitted/accepted to/by broker - Nothing to do
        # return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy(): # isbuy | issell | isalive(Accepted or Partial)
                self.log(
        'BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
        (order.executed.price,
        order.executed.value,
        order.executed.comm))
            elif order.issell(): # Sell
                self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                        (order.executed.price,
                        order.executed.value,
                        order.executed.comm))
                position = self.getposition(self.data)
                self.log(position)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')
        elif order.status in [order.Expired]:
            self.log('Order Expired')
        # Write down: no pending order
        # self.order = None

    def notify_trade(self, trade): # 第一次买会执行一次，持仓为零时执行一次
        if not trade.isclosed: # 持仓为不零时
            return

        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %(trade.pnl, trade.pnlcomm))
        # pnl: profit and loss, 损益=损失/收益
        # comm: commision，佣金
        # pnlcomm: 损益 - 佣金，也就是净收益

    def next(self):
        date_cur = self.datas[0].datetime.date(0) # 当天日期

        close_cur = self.datas[0].close[0] # 当天收盘价
        open_cur = self.datas[0].open[0] # 当天开盘价
        high_cur = self.datas[0].high[0] # 当天最高价
        low_cur = self.datas[0].low[0] # 当天最低价
        v_cur = self.datas[0].volume[0]
        turnover_cur = self.datas[0].turnover[0]
        bars_past = len(self) # 回测了多少天，日线场景
        bars_left = len(list(self.data0.close)) # 还剩下多少天待回测，日线场景

        # w_close_cur = self.datas[1].close[0] # 当周收盘价
        # w_open_cur = self.datas[1].open[0] # 当周开盘价
        # w_high_cur = self.datas[1].high[0] # 当周最高价
        # w_low_cur = self.datas[1].low[0] # 当周最低价
        # w_v_cur = self.datas[1].volume[0]
        # w_date_cur = self.datas[1].datetime.date(0) # 周线对应日期


        # from IPython import embed; embed(); exit()
        # print('==> self.ind.buy_sell[0]: ', self.ind.buy_sell)
        line = 'today: {}, open: {:.2f}, high: {:.2f}, low: {:.2f}, close: {:.2f}, signal: {:.1f}, bars_past: {}, bars_left: {}/{}'.format(
        date_cur, open_cur, high_cur, low_cur, close_cur, self.ind.buy_sell[0], bars_past, bars_left, len(list(self.datas[0]))
        )
        print(line)
        size = math.ceil(10000 / close_cur / 100) * 100 # 买1w 对应多少手，向上取整。1手=100股


        if self.ind.buy_sell[0] == 1 and self.ind_w.buy_sell[0] == 1:
            self.order = self.buy(data=self.data0, # 哪支证券
            price=None, # 以什么价格买入
            size=size, # 买入多少
            exectype=bt.Order.Market, # 买入方式：Limit（限价买入） | Market（按市场价买入）
            # valid = date_nxt, # NOTE: date_nxt = self.datas[0].datetime.date(2)
            valid=self.data.datetime.date(0) + datetime.timedelta(days=2) # 订单多少天内有效，0代表当天有效
            )
            self.order.date = date_cur
            print(date_cur, '\n', self.order)
        elif self.ind.buy_sell[0] == -1 and self.getposition(self.data): # 卖出信号且有持仓
            size = self.getposition(self.data).size # 持仓数量，如果 size = 0 那么也不会执行卖出操作
            self.order = self.sell(data=self.data0, # 哪支证券
                                    price=None, # 以什么价格买入
                                    size=size, # 买入多少
                                    exectype=bt.Order.Market, # 买入方式：Limit（限价买入） | Market（按市场价买入）
                                    valid=self.data.datetime.date(0) + datetime.timedelta(days=2) # 订单多少天内有效，0代表当天有效
                                    )
            self.order.date = date_cur # NOTE



    def add_observers(brain, observers):
        for obs in observers:
            module = getattr(bt.observers, obs)
            brain.addobserver(module)


    def add_analyzers(brain, analyzers):
        for obs in analyzers:
            module = getattr(bt.analyzers, obs)
            brain.addanalyzer(module)


    def parse_results(results, analyzers):
        temp_metrics = dict()
        cur_result = results[0]
        for analyzer in analyzers: # analyzers = ["TradeAnalyzer", "SharpeRatio", "DrawDown", "AnnualReturn"]
            analyzer = analyzer.lower()
            module_analyzer = getattr(cur_result.analyzers, analyzer)
            res_analyzer = module_analyzer.get_analysis()
            print("===={}====".format(analyzer))
            # module_analyzer.pprint() # 打印分析指标，以下是取部分值和计算
        if analyzer == 'tradeanalyzer':
            try:
                n_won = res_analyzer['won']['total']
                n_lost = res_analyzer['lost']['total']
                n_closed = res_analyzer['total']['closed']
                rate_won = n_won / n_closed
                temp_metrics['n_won'] = n_won
                temp_metrics['n_lost'] = n_lost
                temp_metrics['n_closed'] = n_closed
                temp_metrics['rate_won'] = round(rate_won, 3)
                temp_metrics['avg_pnl'] = res_analyzer['long']['pnl']['average']
                temp_metrics['avg_len'] = res_analyzer['len']['average']
                print('n_won: {}, n_lost: {}, n_closed: {}, rate_won: {:.3f}'.format(n_won, n_lost, n_closed, rate_won))
            except:
                pass
        elif analyzer == 'drawdown':
            max_drawdown = round(res_analyzer['max']['drawdown'], 2)
            max_moneydown = int(res_analyzer['max']['moneydown'])
            temp_metrics['max_drawdown'] = max_drawdown
            temp_metrics['max_moneydown'] = max_moneydown
            print('max drawdown', max_drawdown)
            print('max moneydown', max_moneydown)
            # from IPython import embed; embed(); exit()
        elif analyzer == 'sharperatio':
            sharperatio = res_analyzer['sharperatio']
            if sharperatio is not None:
                temp_metrics['sharperatio'] = sharperatio
                print('sharperatio(new)', sharperatio)
            else:
                module_analyzer.pprint()
                return temp_metrics


    def run_one(path_data, write=False, plot=False):
        brain = bt.Cerebro(runonce=False) # 大脑
        date_start = datetime.datetime(2019, 7, 1) # 回测起始时间
        date_end = datetime.datetime(2020, 12, 5) # 回测结束时间, NOTE: 不包括截止日期当天
        datafeed = MyGenericCSV(
        dataname=path_data,
        fromdate=date_start,
        todate=date_end,
        )
        brain.adddata(datafeed) # 数据流加入到大脑
        brain.addstrategy(BaseStrategy) # 策略（信号）加入到大脑
        brain.resampledata(datafeed, timeframe=bt.TimeFrame.Weeks) # NOTE: 周线

        observers = ['TimeReturn']
        add_observers(brain, observers) # 观察指标加入到大脑

        analyzers = ["TradeAnalyzer", "SharpeRatio", "DrawDown", "AnnualReturn"] # 分析指标
        add_analyzers(brain, analyzers) # 分析指标加入到大脑

        if write:
            basename = os.path.basename(path_data)
            path_save = os.path.join(ROOT_PROJ, 'tutorial_v2', 'logs', basename) # log 保存路径
            parent = os.path.dirname(path_save) # 保存路径所在目录
            os.makedirs(parent, exist_ok=True) # 不存在的话，就创建目录
            brain.addwriter(bt.WriterFile, csv=True, out=path_save) # 大脑会写入回测记录到指定路径

        cash = 100000 # 账户初始资金
        commission = 0.0003 # 交易佣金，没有考虑印花税
        brain.broker.setcash(cash) # 设置账户初始资金
        brain.broker.setcommission(commission=commission) # 交易佣金，没有考虑印花税

        results = brain.run() # 运行，并得到analyzer结果
        metrics = parse_results(results, analyzers) # 解析结果
        if plot:
            brain.plot(style='candle') # 画图
            return metrics


    def test_one(write=False, plot=False):
        path_data = os.path.join(ROOT_PROJ, 'datas', '20201204', '000004_reversed.csv') # 数据所在路径, 000700, 600354
        run_one(path_data, write=write, plot=plot)


    def test_all():
        from glob import glob
    def csv2len(csv):
        # 计算文件有多少行，即有多少个交易日
        with open(csv, 'r') as f:
            lines = [each.strip() for each in f.readlines()]
            return len(lines)
    def add_line(txt, line):
    # 在文末加上一行
        with open(txt, 'a+') as f:
            f.write(line + '\n')
    def random_select(paths, prefix, num):
    # 随机挑选 num 个 paths 中以 prefix 开头的子集
        import random
        random.seed(1024)
        candidates = [each for each in paths if os.path.basename(each).startswith(prefix)]
        random.shuffle(candidates)
        return candidates[:num]

        path_log = os.path.join(ROOT_PROJ, 'tutorial_v2', 'logs', 'multistock.log') # log 保存路径
        if os.path.exists(path_log): os.remove(path_log) # 存在的话，在程序运行前先删除
        paths_csv = sorted(glob(os.path.join(ROOT_PROJ, 'datas', '20201204', '*reversed.csv'))) # 所有股票所在路径，日期按升序排序好
        paths_csv = [each for each in paths_csv if csv2len(each) > 100] # 只保留至少存在100个交易日的股票
        paths_000 = random_select(paths_csv, '000', 50) # 随机挑选50个深交所股票
        paths_300 = random_select(paths_csv, '300', 50) # 随机挑选50个创业板股票
        paths_600 = random_select(paths_csv, '600', 50) # 随机挑选50个上交所股票
        paths_csv = paths_000 + paths_300 + paths_600 # 随机挑选股票的集合
        metric = {'n_won': 0, 'n_lost': 0, 'avg_pnl': 0, 'avg_len': 0, 'count': 0} # 记录一些指标，待后面计算总体指标
        for path_csv in paths_csv: # 开始遍历股票
            secID = os.path.basename(path_csv).split('_')[0] # 计算股票代码，如000004_reversed.csv --> 000004
        try:
            temp_metrics = run_one(path_csv, write=False, plot=False) # 回测该支股票，并返回指标
        except Exception as e: # 有些股票数据可能字段不全，导致回测报错，在这里捕获
            line = 'secID: {}, error: {}'.format(secID, e) # 要记录的信息：股票代码，错误信息
            add_line(path_log, line) # 写入 log 文件
    # 跳过，不运行循环内的以下代码

        for k in metric.keys(): # 更新指标
            if k == 'count': # 计数器单独考虑
                metric[k] += 1
                continue
                metric[k] += temp_metrics[k] # 指标先累加

                rate_won = temp_metrics['n_won'] / (temp_metrics['n_won'] + temp_metrics['n_lost'] + 1e-8) # 单支股票的胜率
                line = 'secID: {}, n_won: {}, n_lost: {}, rate_won: {:.3f}, avg_pnl: {:.1f}, avg_len: {:.1f}'.format( # 单支股票要记录的信息
                secID, temp_metrics['n_won'], temp_metrics['n_lost'], rate_won, temp_metrics['avg_pnl'], temp_metrics['avg_len'])
                add_line(path_log, line) # 写入 log 文件
                # print(metric)
                rate_won = metric['n_won'] / (metric['n_won'] + metric['n_lost'] + 1e-8) # 整体胜率
                avg_pnl = metric['avg_pnl'] / metric['count'] # 整体平均每次交易的收益
                avg_len = metric['avg_len'] / metric['count'] # 整体平均每次交易持仓天数
                line = 'rate_won: {:.3f}, avg_pnl: {:.1f}, avg_len: {:.1f}'.format(rate_won, avg_pnl, avg_len) # 要记录的信息
                add_line(path_log, line) # 写入 log 文件
            # from IPython import embed; embed(); exit()


if __name__ == '__main__':
    test_one(write=False, plot=False)
# test_all()