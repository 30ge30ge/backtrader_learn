# 引入常用库
import pandas as pd
import talib as ta  # 技术分析
# 使用ts
import tushare as ts
# 画图
import matplotlib.pyplot as plt
import matplotlib.dates as mdate
plt.rcParams['axes.unicode_minus'] = False
# 图表主题
plt.style.use('ggplot')
# 忽略报错
import warnings
warnings.filterwarnings("ignore")
import pysnowball as ball
import datetime
from tqdm import tqdm




'''token'''
pro = ts.pro_api('7f2e09febef74cdf5bc798d9cb63caff1e0e9411acbc484bad2953ae')
ball.set_token('xq_a_token=16534528effa8d3606211a60a5e154a489ff3112;')


#利用tushare把历史数据读取

start_date='20010101'
middle_date='20101231'
next_date='20110101'
end_date=datetime.datetime.now().strftime('%Y%m%d')
code='000300.SH'
df_start= pro.index_dailybasic(ts_code=code,start_date=start_date,end_date=middle_date, fields='ts_code,trade_date,turnover_rate')
df_end= pro.index_dailybasic(ts_code=code,start_date=next_date,end_date=end_date, fields='ts_code,trade_date,turnover_rate')
index_dailybasic=pd.concat([df_start,df_end])

df_dailydata_start = pro.index_daily(ts_code=code, start_date=start_date, end_date=middle_date)
df_dailydata_end= pro.index_daily(ts_code=code, start_date=next_date, end_date=end_date)
index_daily=pd.concat([df_dailydata_start,df_dailydata_end])


df=pd.merge(index_dailybasic,index_daily,on='trade_date')
df=df[['ts_code_x','open','high','low','close','pct_chg','turnover_rate','trade_date']]
df.columns=['ts_code','open','high','low','close','pct_chg','turnover_rate','trade_date']
df=df.sort_values('trade_date')



#利用雪球把今日得数据读取
bs='SH000300'
index_now=ball.quotec(bs)
index_today=pd.DataFrame(index_now['data'][0],index=[0])
#把雪球代码变成tushare代码
index_today['ts_code']=index_today['symbol'].str.slice(2,8)+'.SH'
# index_today['ts_code']=index_today['symbol'].apply(lambda x:x[2:])+'.SH'
index_today=index_today[['ts_code','open','high','low','current','percent','turnover_rate']]
index_today['trade_date']=end_date
index_today.columns=['ts_code','open','high','low','close','pct_chg','turnover_rate','trade_date']





#把2数据合并
data=pd.concat([df,index_today])
#若晚上或者节假日读数据，删除掉多余日期
data=data.drop_duplicates(subset='trade_date',keep='first')

data['trade_date']=pd.to_datetime(data['trade_date'],format='%Y%m%d')


#计算波动率和换手率
data['std_120'] = data['pct_chg'].rolling(120).std()
data['turnover_rate_120'] = data['turnover_rate'].rolling(120).mean()
data['kernel_index'] = round(data['std_120'] / data['turnover_rate_120'],4)

data['up'],data['middle'],data['down']=ta.BBANDS(data.kernel_index, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)
data['up']=round(data['up'],4)
data['middle']=round(data['middle'],4)
data['down']=round(data['down'],4)
data['KIDEX']=data['kernel_index']
print(data)

def BBband(data):
    a=[]
    b=[]
    Data = data.copy()
    Data=Data.reset_index()
    Data['flag'] = 0  # 买卖标记
    Data['position'] = 0  # 持仓标记
    position = 0  # 是否持仓，持仓：1，不持仓：0
    for i in range(0, Data.shape[0]):

        # 开仓
        if Data.loc[i, 'kernel_index'] <Data.loc[i, 'down'] and position == 0:
            Data.loc[i, 'flag'] = 1
            Data.loc[i + 1, 'position'] = 1
            position = 1
            a.append(i)
        # 平仓
        elif Data.loc[i, 'kernel_index'] >Data.loc[i, 'up'] and position == 1:
            Data.loc[i, 'flag'] = -1
            Data.loc[i + 1, 'position'] = 0
            position = 0
            b.append(i)

        # 保持
        else:
            Data.loc[i + 1, 'position'] = Data.loc[i, 'position']

    Data['pct']=Data.close.pct_change(1).fillna(0)
    Data['pct修正'] = Data['pct']
    for i in a:
        Data.loc[i+1, 'pct修正'] = Data['close'][i+1]/Data['open'][i+1]-1
    for i in b:
        Data.loc[i+1, 'pct修正'] = Data['open'][i+1]/Data['close'][i]-1
    Data['pct负值']=Data['pct']*-1
    Data['nav'] = (1 + Data['pct']).cumprod()
    Data['nav_timing'] = (1 + Data['pct'] * Data['position']).cumprod()
    Data['nav_修正'] = (1 + Data['pct修正'] * Data['position']).cumprod()

    '''需要设置一下路径'''
    # Data.to_excel('/Users/30ge/Downloads/indexdata/沪深指数带指标.xlsx')
    # Data.to_excel('D:/北京交接20201116/backtrader数据集/'+'沪深指数带指标'+end_date+'.xlsx')



    return Data

plot_data=BBband(data)

# plot_data=plot_data.set_index('trade_date')
# y1 = plot_data['nav']  # 获取收盘数据
# y2 = plot_data['nav_timing']
# y3 = plot_data['nav_修正']
# fig = plt.figure(figsize=(18, 8))
# plt.plot(y1,label = '原始净值',linewidth = 2)
# plt.plot(y2,label = '沪深300择时净值',linewidth = 2)
# plt.plot(y3,label = 'BBband修正择时',linewidth = 2)
# plt.title("399300择时策略")
# plt.legend(loc='best')
# plt.show()

fig = plt.figure(figsize=(18, 8))
y1 = plot_data['up'][-60:]  # 获取收盘数据
y2 = plot_data['kernel_index'][-60:]
y3 = plot_data['down'][-60:]
plt.plot(y1,label = 'up',linewidth = 2)
plt.plot(y2,label = 'kernel_index',linewidth = 2)
plt.plot(y3,label = 'down',linewidth = 2)
plt.title("走势图")
plt.legend(loc='best')
plt.show()