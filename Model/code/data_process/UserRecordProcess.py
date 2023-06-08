


# TODO 首張訂單生成在 2023.3.1 之前用戶
# TODO ⽤戶訂單交易紀錄篩選 : 2023.4.1 之前的訂單

# TODO 用戶表整併
"""
訂單數量、總金額
    全部訂單數量、逾期、結清、逾期過、沒逾期過、逾期超過6天(壞訂單)、
    逾期未超過6天(好訂單)、逾期超過12天(壞訂單2)

最初、最後下單日期、金額，用戶存活時間(最後-最初)

最高合同金額、最高合同金額分組

初始風控分、標籤

性別、年紀
"""


# ** 測試號 user_id 330696、196761、196519

# %%
import pandas as pd
from datetime import datetime



# %%
Record_raw = pd.read_csv(r"C:\Users\NiuNi\OneDrive\桌面\demo\Model\dataset\已放款用戶\用戶訂單及基本資料\RecordRaw.csv")
Record_raw

# %%
# Record_raw.isna().sum()
# ** 沒有 異常缺失

# %%
# TODO 測試號資料移除

test_acc = [330696, 196761, 196519]
condition_TestAccDrop = Record_raw['user_id'].isin(test_acc)
Record_raw =Record_raw[~condition_TestAccDrop]
# Record_raw

# %%
date_col = list(Record_raw.filter(like='日期').columns)

def ToDateTime(df,date_col):
    for i in date_col:
        df[i] = pd.to_datetime(df[i])
    return df

ToDateTime(Record_raw,date_col)
# Record_raw.info()

# %%
print(Record_raw['生成日期'].max())
# ** 訂單生日期 最新 2023.3.31

print(len(Record_raw['user_id'].unique()))
# ** raw data 用戶人數 21417人



# %%
# TODO 有展期過的訂單號

leng_order_list = Record_raw[(Record_raw['is_passive_leng']==1)&(Record_raw['leng_order_no'].isna())]['order_no'].to_list()
print(len(leng_order_list))
print(len(set(leng_order_list)))
# ** 沒有重複訂單號

# %%
# TODO 抓出有展期過的訂單包含展期單，篩選出最後一筆展期單

leng_order = Record_raw[(Record_raw['order_no'].isin(leng_order_list))+(Record_raw['leng_order_no'].isin(leng_order_list))]

# TODO 先篩出 leng_order_no 非空值
# TODO 後篩出 is_passive_leng = 0 且 is_leng = 1 能找到最後一筆展期單

leng_order = leng_order[~leng_order['leng_order_no'].isna()]
leng_order = leng_order[(leng_order['is_passive_leng']==0)&(leng_order['is_leng']==1)]
# leng_order

# TODO 所有有關展期單先自 原始表刪去

Record_process = Record_raw[~Record_raw['order_no'].isin(leng_order_list)]
# Record_process

# TODO 最後展期單合併回原始表

Record_process = pd.concat([Record_process,leng_order])
Record_process.reset_index(inplace=True)



# %%
# TODO 逾期天數計算

Record_process['today'] = datetime.now().date()
Record_process['today'] = pd.to_datetime(Record_process['today'])

Record_process['逾期天數'] = ''
Record_process.loc[Record_process['status'] == 12, '逾期天數'] = (Record_process['today'] - Record_process['到期日期']).dt.days
Record_process.loc[Record_process['status'] == 10, '逾期天數'] = (Record_process['還款日期'] - Record_process['到期日期']).dt.days
Record_process['逾期天數'] = Record_process['逾期天數'].astype(int)

Record_process['is_overdue'] = Record_process['逾期天數'].map(lambda x : 1 if x > 0 else 0)
Record_process['overed_6days'] = Record_process['逾期天數'].map(lambda x : 1 if 12 > x > 6 else 0)
Record_process['overed_12days'] = Record_process['逾期天數'].map(lambda x : 1 if x > 12 else 0)

Record_process



# %%
# TODO 全部訂單數量計算、最高合同金額

RecordOrder = Record_process.groupby(by='user_id')['order_no'].count().reset_index().rename(columns={'order_no':'UserOrderSum'})
RecordOrder['MaxOrderMoney'] = Record_process.groupby(by='user_id')['device_money'].max().reset_index()['device_money']

def MaxMoneyGroup(data):
    if data['MaxOrderMoney'] <= 5000:
        val = '5000以內'
    elif data['MaxOrderMoney'] <= 10000:
        val = '5000-10000'
    elif data['MaxOrderMoney'] <= 15000:
        val = '10000-15000'
    elif data['MaxOrderMoney'] <= 20000:
        val = '15000-20000'
    elif data['MaxOrderMoney'] <= 25000:
        val = '20000-25000'
    else:
        val = '25000以上'
    return val

RecordOrder['MaxMoneyGroup'] = RecordOrder.apply(MaxMoneyGroup,axis=1)

def merge_orders_data(col, status, new_col_name):
    grouped_data = Record_process[Record_process[col] == status].groupby('user_id')['order_no'].count().reset_index().rename(columns={'order_no': new_col_name})
    merged_data = pd.merge(RecordOrder, grouped_data, on='user_id', how='left')
    return merged_data

RecordOrder = merge_orders_data('status', 10, 'RepaidOrders')
RecordOrder = merge_orders_data('status', 12, 'OverdueOrders')

RecordOrder = merge_orders_data('is_overdue',1,'OverdueRecordOrders')
RecordOrder = merge_orders_data('is_overdue',0,'NoOverdueRecordOrders')

RecordOrder = merge_orders_data('overed_6days',1,'Over6daysOrders')
RecordOrder = merge_orders_data('overed_12days',1,'Over12daysOrders')

RecordOrder.isna().sum()
RecordOrder.fillna(0,inplace = True)

RecordOrder



# %%
RecordMoney = Record_process.groupby(by='user_id')['device_money'].sum().reset_index().rename(columns={'device_money':'UserMoneySum'})
RecordMoney['OrderAvgMoney'] = RecordMoney['UserMoneySum'] / RecordOrder['UserOrderSum']

def merge_money_data(col, status, new_col_name):
    grouped_data = Record_process[Record_process[col] == status].groupby('user_id')['device_money'].sum().reset_index().rename(columns={'device_money':new_col_name})
    merge_data = pd.merge(RecordMoney,grouped_data,on='user_id',how='left')

    return merge_data

RecordMoney = merge_money_data('status', 10, 'RepaidMount')
RecordMoney = merge_money_data('status', 12, 'OverdueMount')

RecordMoney = merge_money_data('is_overdue', 1, 'OverdueRecordMount')
RecordMoney = merge_money_data('is_overdue', 0, 'NoOverdueRecordMount')

RecordMoney



# %%
first_order_date = Record_process.sort_values(by=['user_id','生成日期'],ascending=True).drop_duplicates(subset=['user_id'],keep='first')
first_order_date.rename(
    columns={
        '生成日期':'FirstOrderDate',
        'device_money':'FirstOrderMoney'
    },inplace=True
)

last_order_date = Record_process.sort_values(by=['user_id','生成日期'],ascending=True).drop_duplicates(subset=['user_id'],keep='last')
last_order_date.rename(
    columns={
        '生成日期':'LastOrderDate',
        'device_money':'LastOrderMoney'
    },inplace=True
)

RecordLifeTime = pd.merge(
    first_order_date[['user_id','FirstOrderDate','FirstOrderMoney']],
    last_order_date[['user_id','LastOrderDate','LastOrderMoney']],
    on='user_id',
    how='left'
)

RecordLifeTime['UserLifeTime'] = RecordLifeTime['LastOrderDate'] - RecordLifeTime['FirstOrderDate']
RecordLifeTime['UserLifeTime'] = RecordLifeTime['UserLifeTime'].astype(str).str.replace('days', '').astype(int)

RecordLifeTime



# %%
# TODO 讓後面的其他資訊表用戶一致
UserList = RecordLifeTime['user_id'].to_list()
len(UserList)



# %%
# ** risk control raw data
# ** 全部用戶(含拒絕用戶風控資料)
# ! raw data 需要重撈 缺少 provider、score、status
# ! 暫時先用現有資料處理 後續補齊資料

RiskScore_raw = pd.read_csv(r"C:\Users\NiuNi\OneDrive\桌面\demo\Model\dataset\AllUserRiskControlData_raw.csv")
RiskScore_raw

# %%
# RiskScore_raw.duplicated(subset='user_id').sum()
# ** 無重複用戶資料

RiskScore_raw = RiskScore_raw[RiskScore_raw['user_id'].isin(UserList)]
RiskScore_raw.isna().sum()
# ** 112 人 風控標籤缺失

# TODO 風控標籤缺失的用戶 生成名單並導出 txt 另外至db填補

RankNullList = RiskScore_raw[RiskScore_raw['rank'].isna()]['user_id']
# len(RankNullList)

file_path = r"C:\Users\NiuNi\OneDrive\桌面\demo\Model\dataset\RankNullUser.txt"

RankNullList_str = ",".join(map(str, RankNullList))
with open(file_path, 'w') as file:
    file.write(RankNullList_str)

RiskScore_fillna = pd.read_csv(r"C:\Users\NiuNi\OneDrive\桌面\demo\Model\dataset\AllUserRiskControlDataFillRankNa_raw.csv")
RiskScore_fillna

# ** 已填補風控的新資料的創建時間與最早的風控的創建時間差距不大，直接以新資料取代原資料缺失

RiskScore_raw = RiskScore_raw[~RiskScore_raw['rank'].isna()]
RiskScore = pd.concat([RiskScore_raw,RiskScore_fillna])
RiskScore.isna().sum()
# ** rank 無缺失 但 score 缺失 1419

# RiskScore[(RiskScore['provider']==4)&(RiskScore['score'].isna())]
# RiskScore[(RiskScore['provider']==16)&(RiskScore['score'].isna())]
# RiskScore[(RiskScore['provider']==17)&(RiskScore['score'].isna())]
# ** 經檢驗 只有 provider = 16 (藍光風控)之分數才有缺失，provider = 4、17 無缺失
# ** 藍光本身不會輸出分數結果



# %%
# ** user certification raw
UserCertification_raw = pd.read_csv(r"C:\Users\NiuNi\OneDrive\桌面\demo\Model\dataset\已放款用戶\用戶訂單及基本資料\UserCertification.csv")

# TODO user_id 38050，有重複值且缺失 直接刪去缺失那筆

UserCertification_raw.dropna(subset=['birthday'],inplace=True)

today = pd.to_datetime(pd.Timestamp.today().date())
UserCertification_raw['birthday'] = pd.to_datetime(UserCertification_raw['birthday'])
UserCertification_raw['age'] = ((today - UserCertification_raw['birthday']).astype('<m8[Y]')).astype(int)
UserCertification = UserCertification_raw[UserCertification_raw['user_id'].isin(UserList)]

UserCertification
# %%
