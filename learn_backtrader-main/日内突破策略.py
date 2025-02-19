# 突破类策略
# DualThrust
import backtrader as bt
from datetime import datetime, time
import akshare as ak
import pandas as pd


def get_data_m(code):
    df = ak.stock_zh_a_hist_min_em(code, period='5')
    df.index = pd.to_datetime(df.时间)
    df = df[['开盘', '最高', '最低', '收盘', '成交量']]
    df['openinterest'] = 0
    columns = ['open', 'high', 'low', 'close', 'volume', 'openinterest']
    df.columns = columns
    return df



class DT_Line(bt.Indicator):
    lines = ('U', 'D')
    params = (('period', 2), ('k_u', 0.7), ('k_d', 0.7))

    def __init__(self):
        self.addminperiod(self.p.period + 1)

    def next(self):
        HH = max(self.data.high.get(ago=-1, size=self.p.period))
        LC = min(self.data.close.get(ago=-1, size=self.p.period))
        HC = max(self.data.close.get(ago=-1, size=self.p.period))
        LL = min(self.data.low.get(ago=-1, size=self.p.period))
        R = max(HH-LC, HC-LL)
        self.lines.U[0] = self.data.open[0] + self.p.k_u * R
        self.lines.D[0] = self.data.open[0] - self.p.k_d * R


class DualThrust(bt.Strategy):
    def __init__(self):
        self.dataclose = self.data0.close
        self.D_Line = DT_Line(self.data1)
        # 将Dline放到主图上，同时消除daily和min的区别，需要做映射
        self.D_Line = self.D_Line()
        # self.D_Line.plotinfo.plot = False
        self.D_Line.plotinfo.plotmaster = self.data0

        self.buy_signal = bt.indicators.CrossOver(self.D_Line.U, self.dataclose)
        self.sell_signal = bt.indicators.CrossOver(self.D_Line.D, self.dataclose)

    def next(self):

        if self.data.datetime.time() > time(9, 5) and self.data.datetime.time() < time(15, 30):
            if not self.position and self.buy_signal[0] == 1:
                self.order = self.buy()
            if self.getposition().size < 0 and self.sell_signal[0] == 1:
                self.order = self.close()
                self.order = self.buy()
            if not self.position and self.buy_signal[0] == 0:
                self.order = self.sell()
            if self.getposition().size > 0 and self.sell_signal[0] == 0:
                self.order = self.close()
                self.order = self.sell()
        if self.data.datetime.time() >= time(15, 30) and self.position:
            self.order = self.close()

if __name__ == '__main__':
    cerebro = bt.Cerebro()
    df = get_data_m('601919')
    data = bt.feeds.PandasData(
        dataname=df,
        fromdate=datetime(2023, 3, 9),
        todate=datetime(2023, 4, 21),
        timeframe=bt.TimeFrame.Minutes,
        openinterest=-1
    )

    cerebro.adddata(data, name="daily_kline")
    cerebro.resampledata(data, timeframe=bt.TimeFrame.Days)

    cerebro.addstrategy(DualThrust)
    result = cerebro.run()

    cerebro.plot()
