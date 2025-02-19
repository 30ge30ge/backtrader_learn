import datetime  # 用于datetime对象操作
import os.path  # 用于管理路径
import sys  # 用于在argvTo[0]中找到脚本名称
import backtrader as bt  # 引入backtrader框架
import pandas as pd



today=(datetime.datetime.now()+datetime.timedelta(days=-3)).strftime('%Y%m%d')

df_all = pd.read_csv('D:/北京交接20201116/tushare数据/每日指标与每日行情数据/'+today+'每日指标.txt',encoding='utf-8',parse_dates=['trade_date'])
stocklist_allA = df_all['ts_code'].unique().tolist()

df_all['openinterest']=1
df_all['volume']=df_all['vol']



def get_stock_data(code):
    df_stock = df_all[df_all['ts_code']==code]
    df_stock = df_stock[['trade_date','open','high','low','close','volume',
                         'openinterest','pe','pb','turnover_rate','circ_mv','total_mv']]
    df_stock['trade_date']=pd.to_datetime(df_stock['trade_date'])
    df_stock.index=df_stock.trade_date
    df_stock = df_stock.sort_index()
    return df_stock




#增加数据
class Addmoredata(bt.feeds.PandasData):
    lines = ('pe','pb','turnover_rate','circ_mv','total_mv',)
    params = (('pe',7),('pb',8),('turnover_rate',9),('circ_mv',10),('total_mv',11),('dtformat', '%Y-%m-%d'),)







# 创建策略
class SmaCross(bt.Strategy):
    # 可配置策略参数
    params = dict(
        pfast=5,  # 短期均线周期
        pslow=60,  # 长期均线周期
        poneplot=False,  # 是否打印到同一张图
        pstake=1000  # 单笔交易股票数目
    )

    def __init__(self):
        self.inds = dict()
        for i, d in enumerate(self.datas):
            self.inds[d] = dict()
            print(i,d,d._name)
            self.inds[d]['sma1'] = bt.ind.SMA(d.close, period=self.p.pfast)  # 短期均线
            self.inds[d]['sma2'] = bt.ind.SMA(d.close, period=self.p.pslow)  # 长期均线
            self.inds[d]['cross'] = bt.ind.CrossOver(self.inds[d]['sma1'], self.inds[d]['sma2'], plot=False)  # 交叉信号
            # 跳过第一只股票data，第一只股票data作为主图数据
            if i > 0:
                if self.p.poneplot:
                    d.plotinfo.plotmaster = self.datas[0]

    def next(self):
        for i, d in enumerate(self.datas):
            dt, dn = self.datetime.date(), d._name  # 获取时间及股票代码
            pos = self.getposition(d).size
            if not pos:  # 不在场内，则可以买入
                if self.inds[d]['cross'] > 0:  # 如果金叉
                    self.buy(data=d, size=self.p.pstake)  # 买买买
            elif self.inds[d]['cross'] < 0:  # 在场内，且死叉
                self.close(data=d)  # 卖卖卖



if __name__ == '__main__':
    cerebro = bt.Cerebro()
    cerebro.broker.setcash(10000000000.0)
    cerebro.broker.setcommission(commission=0.001)
    cerebro.addstrategy(SmaCross)
    for code in stocklist_allA[1:5]:
        feed = Addmoredata(dataname = get_stock_data(code),name=code)
        cerebro.adddata(feed)
    # cerebro.addsizer(TradeSizer)
    cerebro.run()
    cerebro.plot(volume=False)
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())








# cerebro = bt.Cerebro()  # 创建cerebro
# # 读入股票代码
# stk_code_file = '../TQDat/TQDown2020v1/data/stock_code_update.csv'
# stk_pools = pd.read_csv(stk_code_file, encoding='gbk')
# if stk_num > stk_pools.shape[0]:
#     print('股票数目不能大于%d' % stk_pools.shape[0])
#     exit()
# for i in range(stk_num):
#     stk_code = stk_pools['code'][stk_pools.index[i]]
#     stk_code = '%06d' % stk_code
#     # 读入数据
#     datapath = '../TQDat/day/stk/' + stk_code + '.csv'
#     # 创建价格数据
#     data = bt.feeds.GenericCSVData(
#         dataname=datapath,
#         fromdate=datetime.datetime(2018, 1, 1),
#         todate=datetime.datetime(2020, 3, 20),
#         nullvalue=0.0,
#         dtformat=('%Y-%m-%d'),
#         datetime=0,
#         open=1,
#         high=2,
#         low=3,
#         close=4,
#         volume=5,
#         openinterest=-1
#     )
#     # 在Cerebro中添加股票数据
#     cerebro.adddata(data, name=stk_code)
# # 设置启动资金
# cerebro.broker.setcash(100000.0)
# # 设置交易单位大小
# # cerebro.addsizer(bt.sizers.FixedSize, stake = 5000)
# # 设置佣金为千分之一
# cerebro.broker.setcommission(commission=0.001)
# cerebro.addstrategy(SmaCross, poneplot=False)  # 添加策略
# cerebro.run()  # 遍历所有数据
# # 打印最后结果
# print('Final Portfolio Value: %.2f' % cerebro.broker)
