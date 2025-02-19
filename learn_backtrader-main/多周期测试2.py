import backtrader as bt
import datetime
import pandas as pd
import math
import pymysql

# Connect to the database
connection = pymysql.connect(host='localhost',
                             port=3306,
                             user='root',
                             password='seiRaefoe9jeufooT1uipei5gungiFah',
                             db='stock_etf',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.SSDictCursor)

try:
    with connection.cursor() as cursor:
        # Read a single record
        sql = "SELECT * FROM `etf159920_15m` WHERE datetime BETWEEN %s AND %s"
        cursor.execute(sql, (datetime.date(2019, 1, 1), datetime.date(2020, 5, 1)))
        result = cursor.fetchall()
finally:
    connection.close()

df = pd.DataFrame(data=result)
df = df.set_index(['datetime'])
df


class MultiTFStrategy(bt.Strategy):
    params = (
        ('period', 20),
    )

    # states defination
    Empty, M15Hold, H1Hold, D1Hold = range(4)
    States = [
        'Empty', 'M15Hold', 'H1Hold', 'D1Hold',
    ]

    def log(self, txt):
        ''' Logging function for this strategy'''
        dt = self.datas[0].datetime.datetime(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        self.ma15m = bt.talib.SMA(self.dnames.hs15m, timeperiod=self.p.period)
        self.ma1h = bt.talib.SMA(self.dnames.hs1h, timeperiod=self.p.period)
        self.ma1d = bt.talib.SMA(self.dnames.hs1d, timeperiod=self.p.period)

        self.c15m = bt.indicators.CrossOver(self.dnames.hs15m, self.ma15m, plot=False)
        self.c1h = bt.indicators.CrossOver(self.dnames.hs1h, self.ma1h, plot=False)
        self.c1d = bt.indicators.CrossOver(self.dnames.hs1d, self.ma1d, plot=False)

        self.bsig15m = self.c15m == 1
        self.bsig1h = self.c1h == 1
        self.bsig1d = self.c1d == 1
        self.sell_signal = self.c1d == -1

        self.st = self.Empty
        self.st_map = {
            self.Empty: self._empty,
            self.M15Hold: self._m15hold,
            self.H1Hold: self._h1hold,
            self.D1Hold: self._d1hold,
        }

        # To keep track of pending orders
        self.order = None

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status == order.Completed:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, St: %s, Size: %d, Price: %.2f, Cost: %.2f, Comm %.2f' %
                    (
                        self.States[self.st],
                        order.executed.size,
                        order.executed.price,
                        order.executed.value,
                        order.executed.comm,
                    )
                )

            else:  # Sell
                self.log(
                    'SELL EXECUTED, St: %s, Size: %d, Price: %.2f, Cost: %.2f, Comm %.2f' %
                    (
                        self.States[self.st],
                        order.executed.size,
                        order.executed.price,
                        order.executed.value,
                        order.executed.comm
                    )
                )

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        # Write down: no pending order
        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
                 (trade.pnl, trade.pnlcomm))

    def next(self):
        # Check if an order is pending ... if yes, we cannot send a 2nd one
        if self.order:
            return

        # just call state_map function
        self.order = self.st_map[self.st]()

        # Check if we are in the market and no buy order issued
        if self.position and not self.order:
            # Already in the market ... we might sell
            if self.sell_signal:
                self.st = self.Empty
                # Keep track of the created order to avoid a 2nd order
                self.order = self.close()

    def _empty(self):
        if self.bsig15m:
            price = self.data0.close[0]
            cash = self.broker.get_cash()
            # 20% of the cash
            share = int(math.floor((0.2 * cash) / price))

            # set state
            self.st = self.M15Hold
            return self.buy(size=share)

    def _m15hold(self):
        if self.bsig1h:
            price = self.data0.close[0]
            cash = self.broker.get_cash()
            # half of the remain cash ( 60% )
            share = int(math.floor((0.5 * cash) / price))

            # set state
            self.st = self.H1Hold
            return self.buy(size=share)

    def _h1hold(self):
        if self.bsig1d:
            price = self.data0.close[0]
            cash = self.broker.get_cash()
            # half of the remain cash (80%)
            share = int(math.floor((0.5 * cash) / price))

            # set state
            self.st = self.D1Hold
            return self.buy(size=share)

    def _d1hold(self):
        return None


cerebro = bt.Cerebro(oldtrades=True)

feed = bt.feeds.PandasData(dataname=df, openinterest=None, compression=15, timeframe=bt.TimeFrame.Minutes)

cerebro.adddata(feed, name='hs15m')
cerebro.resampledata(feed, name='hs1h', timeframe=bt.TimeFrame.Minutes, compression=60)
cerebro.resampledata(feed, name='hs1d', timeframe=bt.TimeFrame.Days)

cerebro.addstrategy(MultiTFStrategy)

# 小场面1万起始资金
cerebro.broker.setcash(10000.0)

# 手续费万5
cerebro.broker.setcommission(0.0005)

print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

result = cerebro.run()

print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
cerebro.plot(
    iplot=False,
    start=datetime.date(2019, 11, 1),
    end=datetime.date(2020, 1, 1),
    style='bar',
    barup='red',
    bardown='green',
)