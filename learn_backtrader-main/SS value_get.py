import pandas as pd
import numpy as np
import os
from io import StringIO
import datetime
from pandas.tseries.offsets import Day
import csv

pd.set_option('display.max_columns', None)

'''运行先读'''

'''1号从恒通上下载数据，11号从海王星上下载，记得先下载当日资金明细，当日成交汇总'''
'''因为设置了时间，当天的表只能当天跑出来'''

path='D:/骏骁1号/11号实时/'

'''先把1号和11号,11r实时查询路径设置好'''
strToday = pd.Timestamp.today().strftime('%Y%m%d')
zj_path_1 = 'D:/骏骁1号/text/ZJ_STOCK_' + strToday
cc_path_1 = 'D:/骏骁1号/text/CC_STOCK_' + strToday


zj_path_11 = 'D:/骏骁1号/11号实时/' + strToday +'资金股份查询.xls'


zj_path_11r = 'D:/骏骁1号/11号实时/' + strToday +'资金股份查询r.xls'


'读取11号的资金和持仓'
with open(zj_path_11) as fp:
    text = fp.read().replace('=', '').replace('\t\n', '\n').replace('"', '')
    text = StringIO(text)
    data_zj_11 = pd.read_csv(text, sep='\t',encoding='utf_8',error_bad_lines=False)
    data_zj_11['产品'] = '骏骁11号'

with open(zj_path_11) as zj:
    zjtext = zj.read().replace('=', '').replace('\t\n', '\n').replace('"', '')
    zjtext = StringIO(zjtext)
    data_cc_11 = pd.read_csv(zjtext, sep='\t', engine='python', skiprows=3)




'''读取11号融资11r的资金'''

with open(zj_path_11r) as fp:
    text = fp.read().replace('=', '').replace('\t\n', '\n').replace('"', '')
    text = StringIO(text)
    data_zj_11r = pd.read_csv(text, sep='\t',encoding='utf_8',error_bad_lines=False)
    data_zj_11r['产品'] = '骏骁11r号'
    data_zj_11r = data_zj_11r.rename(columns={'参考市值价格': '参考市价', '浮动盈亏': '参考盈亏', '盈亏成本价': '参考成本价'})






'''读取1号的资金和持仓'''
data_zj_1=pd.read_csv(zj_path_1+'.csv',encoding='gbk')
data_zj_1=data_zj_1.dropna()
data_cc_1=pd.read_csv(cc_path_1+'.csv',encoding='gbk')
print(data_zj_1)
print(data_cc_1)

data_zj_1['参考市值']=data_zj_1['股票资产']+data_zj_1['基金资产']-900
data_zj_1=data_zj_1.rename(columns={'产品名称':'产品','可用余额':'余额','产品总资产':'资产','沪深T+1交易可用':'可用'})
data_zj_1['币种']='人民币'
data_zj_1['盈亏']=np.nan
data_zj_1['盈亏']=data_cc_1['浮动盈亏'].sum()
data_zj_1=data_zj_1[['币种','余额','可用','参考市值','资产','盈亏','产品']]


'''将1号和11号,11r资产合并'''
df_hebin=pd.concat([data_zj_1,data_zj_11,data_zj_11r])
df_hebin=df_hebin.reset_index(drop=True)




'''读取净值快报的模板,选取1号和11号数据'''

jz_mb=pd.read_excel('D:/骏骁1号/净值快报/净值快报模板.xlsx')
jz_mb_1_11=jz_mb[jz_mb['产品'].str.contains('骏骁1号|骏骁11号')]

jz_mb_1_11=jz_mb_1_11.reset_index(drop=True)
jz_mb_1_11['前收净值'] = jz_mb_1_11['账户净值']


'''计算净值，仓位，增长'''

datamerge = pd.merge(jz_mb_1_11, df_hebin, on='产品', how='outer')
datamerge['证券资产'] = datamerge['资产']
datamerge['证券持仓'] = datamerge['参考市值']
datamerge = datamerge.fillna(0)

datamerge.loc[1, '两融净资产'] = datamerge.loc[2, '证券资产']
datamerge.loc[1, '两融持仓'] = datamerge.loc[2, '证券持仓']
# print(datamerge)


datamerge['合计总资产'] = datamerge['证券资产'] + datamerge['两融净资产'] + datamerge['两融负债'] + datamerge['期货资产'] + datamerge['场外资产'] + datamerge['其他资产']+datamerge['在途资产']
datamerge['合计持仓市值'] = datamerge['证券持仓'] + datamerge['两融持仓']  + datamerge['场外持仓'] + datamerge['其他持仓'] - datamerge['华宝']-datamerge['在途资产']
datamerge['仓位'] = round((datamerge['参考市值']+ datamerge['其他持仓'])/ (datamerge['证券资产']+datamerge['两融净资产']), 4)
datamerge['期指多'] =round(datamerge['期货持仓']/datamerge['合计总资产'],4)
datamerge['合计仓位'] = datamerge['期指多'] + datamerge['仓位']

datamerge['账户净值'] = round(datamerge['合计总资产'] / datamerge['份额'], 4)
datamerge['当日净值增长'] = round(datamerge['账户净值'] / datamerge['前收净值'] - 1, 5)
datamerge['当日净值增长'] = datamerge['当日净值增长'].apply(lambda x: '%.2f%%' % (x * 100))
datamerge['合计仓位'] = datamerge['合计仓位'].apply(lambda x: '%.2f%%' % (x * 100))


data_shuchu=datamerge[['产品','账户净值','当日净值增长','仓位','期指多','合计仓位']]

datamerge.to_excel('D:/骏骁1号/净值快报/实时仓位.xlsx')

'''输出到excle数据'''
data_shuchu.to_excel(path+'1_11号实时净值追踪.xlsx',index=False)
print(data_shuchu.head(2))

