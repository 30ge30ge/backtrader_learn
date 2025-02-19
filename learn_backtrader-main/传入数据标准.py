# 创建价格数据
import akshare as ak
import baostock as bs
import pandas as pd
import datetime

# 获取股票池数据
from os import listdir

filename = listdir('D:/stock_data')
stk_pools = filename

for i in stk_pools[:]:

    try:
        datapath = 'D:/stock_data/' + i
        df = pd.read_csv('D:/stock_data/' + i)
        # 将数据长度不足的股票删去
        if len(df) < 55:
            pass
        else:
            try:
                data = bt.feeds.GenericCSVData(
                    dataname=datapath,
                    fromdate=datetime.datetime(2010, 4, 1),
                    todate=datetime.datetime(2021, 7, 8),
                    nullvalue=0.0,
                    dtformat=('%Y-%m-%d'),
                    datetime=1,
                    open=2,
                    high=3,
                    low=4,
                    close=5,
                    volume=6,
                    openinterest=-1
                )
                cerebro.adddata(data, name=i)
            except:
                continue
    except:
        continue