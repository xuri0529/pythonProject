import pandas as pd
import numpy as np

# long_series = pd.Series(np.random.randn(11))
# long_head = long_series.head()
# long_tail = long_series.tail()
# print(long_head)
# print(long_tail)
# print(type(long_series))
# print(dir(long_series))
#
# print('~' * 60)
# print(long_series.shape)

# ser = pd.Series(pd.date_range(start='2023-01-10', end='2023-02-20', periods=30, normalize=False, tz='CET'))
# print(ser)
# to_ser = ser.to_numpy(dtype="datetime64[ns]")
# print(to_ser)

# df = pd.DataFrame({
#     'one': pd.Series(np.random.randn(3), index=['a','b','c']),
#     'two': pd.Series(np.random.randn(4), index=['a','b','c','d']),
#     'three':pd.Series(np.random.randn(3), index=['b','c','d'])
# })
# row = df.iloc[1]
# column = df['two']

# income = pd.Series(['12500元','8000元','8500元','15000元','9000元'])
# aa = income.str[:-1]
# print(income.str[:-1])

# # 列表和字典均可传入DataFrame，我这里用的是字典传入：
data=pd.DataFrame({
    "id":np.arange(101,111),                                # np.arange会自动输出范围内的数据，这里会输出101~110的id号。
    "date":pd.date_range(start="20200310",periods=10),      # 输出日期数据，设置周期为10，注意这里的周期数应该与数据条数相等。
    "money":(5,4,65,-10,15,20,35,16,6,20),                  # 设置一个-10的坑，下面会填（好惨，自己给自己挖坑，幸亏不准备跳~）
    "product":['苏打水','可乐','牛肉干','老干妈','菠萝','冰激凌','洗面奶','洋葱','牙膏','薯片'],
    "department":['饮料','饮料','零食','调味品','水果',np.nan,'日用品','蔬菜','日用品','零食'],                # 再设置一个空值的坑
    "origin":['China',' China','America','China','Thailand','China','america','China','China','Japan']     # 再再设置一个america的坑
})
paixu = data.sort_values(by='money', ascending=True)
data['level'] = np.where(data['money'] >= 10, 'high', 'low')
data.loc[(data['level']=='high') & (data['origin']=='China'), "sign"] = "棒"
data_split = pd.DataFrame((x.split('-') for x in data['date'].astype('str')), index=data.index, columns=['year','month','day'])

a = data.query('product==["零食","水果"]')
print(data)
b = data.groupby(['department','id'])['id'].count()
c = data.groupby("department")['money'].agg([len,np.sum, np.mean])
d = data.query('department=="日用品" & money>20')
print(d)
# print(c)
# print(data.index)
# print(data.dtypes)
# print(data.columns.to_numpy())
# print(data.values)
# print(data.info)
# print(data.describe())
# print(data.isnull)
# print(data['department'].isnull())
# a = data.isnull().sum().sort_values(ascending=False) #将空值判断进行汇总,并降序
# b = data['department'].fillna(method='pad')
# print(data)
# print(b)
# #
# data2=pd.DataFrame({
#     "id":np.arange(102,105),
#     "profit":[1,10,2]
# })
#
# data3=pd.DataFrame({
#     "id":np.arange(111,113),
#     "money":[106,51]
# })

# date_new = pd.merge(left=data2,right=data3, on='id', how='outer')
# dd = data2.merge(data3, on='id', how='outer')
# print(date_new)
# print(dd)
# dj = data2.join(data3, lsuffix='_data2', rsuffix='_data3')
# dj2 = data2.set_index('id').join(data3.set_index('id'))
# print(dj)
# # print(dj2)
# pc = pd.concat([data2, data3], axis=1, keys=['data2, data3'])
# data2['level'] = np.where(data2['profit']>2, 'high', 'low')
# print(data2)

# import pandas as pd
# path = r'D:\housedetail.xlsx'
# data = pd.read_excel(path, sheet_name='2023-02-23', header=3)
# # print(data.dtypes)
# data['totalPrices'] = data['totalPrices'].astype('float')
#
# va = data.loc[(data['totalPrices'].astype('float') < 160) & (data['areas'].str not in ('灞桥', '高陵'))]
# vb = set(data['areaNames'].values)
# vc = data['areaNames'].drop_duplicates()
# data_split = pd.DataFrame((x[:-2] for x in data['mianJis']),index=data.index, columns=['MianJi','DanWei'])
# print(va)
# print(vb)
# print(vc)
# data_split = tuple(x[:-2] for x in data['mianJis'])
# print(data_split)
# vd = data['areaNames'].unique()
# print(vd)
# ve = data.isnull().sum().sort_values(ascending=False)
# print(ve)

# a = {'a':'123你好', 'b':'456世界'}
# print(a['a'][:-2])