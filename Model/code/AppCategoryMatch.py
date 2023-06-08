


# TODO google 爬蟲的資料 appId 匹配 類別
# TODO 重點提出類別 : 博奕類、貸款類(依照特定字段篩選)



# %%
import pandas as pd
import os
import re
import math

import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go

from Levenshtein import distance as levenshtein_distance
import fuzzywuzzy
from fuzzywuzzy import fuzz



# %%
def AppList(folder_path):
    file_list = os.listdir(folder_path)
    df_list = []

    for file in file_list:
        if file.endswith('.txt'):
            file_path = os.path.join(folder_path, file)
            with open(file_path) as f:
                app_list = f.readlines()

            App = pd.DataFrame({'data':app_list})
            # 流程 : 讀取 .txt後，逐行讀取內容字串，轉換成 data frame
            #       篩選文字內容前綴提出不同資料(title、appId、...)，提出的資料 type為series
            #       reset為 data frame 順便對內容文字做處理修改 columns name 生成單一內容資料 data frame
            #       水平合併所有 內容資料 data frame，文字資料轉置成為 表格資料

            title = App[App['data'].str.startswith('title')]['data'].str.replace('title : |\\n', '').reset_index().drop(columns=['index']).rename(columns={'data':'title'})
            appId = App[App['data'].str.startswith('appId')]['data'].str.replace('appId : |\\n', '').reset_index().drop(columns=['index']).rename(columns={'data':'appId'})
            score = App[App['data'].str.startswith('score')]['data'].str.replace('score : |\\n', '').reset_index().drop(columns=['index']).rename(columns={'data':'score'})
            installs = App[App['data'].str.startswith('installs')]['data'].str.replace('installs : |\\n', '').reset_index().drop(columns=['index']).rename(columns={'data':'installs'})
            ratings = App[App['data'].str.startswith('ratings')]['data'].str.replace('ratings : |\\n', '').reset_index().drop(columns=['index']).rename(columns={'data':'ratings'})
            reviews = App[App['data'].str.startswith('reviews')]['data'].str.replace('reviews : |\\n', '').reset_index().drop(columns=['index']).rename(columns={'data':'reviews'})

            category = pd.DataFrame({'category': [os.path.splitext(file)[0]] * len(title)}).reset_index().drop(columns='index')

            df = pd.concat([
                title,
                appId,
                score,
                installs,
                ratings,
                reviews,
                category
            ], axis=1)

            df['appId'] = df['appId'].str.replace(' ', '')
            df['category'] = df['category'].str.replace(' ', '')

            df_list.append(df)
    return pd.concat(df_list)



# %% # TODO 爬蟲資料讀取&欄位字串處理
# ** 第一次爬蟲資料
folder = r"D:\Model\dataset\已放款用戶\dataset_raw\爬蟲資料\GooglePlay資料"

GooglePlayData_raw = AppList(folder)

# ** 第二次爬蟲
ScrapyData = pd.read_excel(r"D:\Model\dataset\已放款用戶\dataset_raw\爬蟲資料\output_file.xlsx")

ScrapyData.rename(columns={'genreId':'category'},inplace=True)
ScrapyData['appId'] = ScrapyData['appId'].str.replace(' ', '')
ScrapyData['title'] = ScrapyData['title'].str.replace(' ', '')

AppCategory_raw = pd.concat([GooglePlayData_raw,ScrapyData])

print(GooglePlayData_raw.shape)
print(ScrapyData.shape)
print(AppCategory_raw.shape[0] == (GooglePlayData_raw.shape[0]+ScrapyData.shape[0]))

# %%
def DuplicatedPlot(AppCategory):
    AppIdCategory_dupl = AppCategory[AppCategory['appId'].isin(AppCategory[AppCategory.duplicated(subset=['appId'])]['appId'].to_list())].sort_values(by='appId',ascending=True)
    AppIdCategory_dupl.groupby(by='appId')['category'].count().reset_index().sort_values(by=['category'],ascending=False)

    AppIdCategory_CrossCategory = AppIdCategory_dupl.groupby(by='appId')['category'].agg(lambda x: ','.join(x)).reset_index().rename(columns={'category':'cross_category'})

    AppCrossCategory_count = AppIdCategory_CrossCategory['cross_category'].value_counts().reset_index()
    AppCrossCategory_count.columns = ['cross_category','count']

    fig = px.bar(
    AppCrossCategory_count,
    x='cross_category',
    y='count'
    )

    fig.update_layout(width=800, height=600)
    fig.show()



    # 使用plotly的Table物件建立資料表
    table = go.Table(
        header=dict(values=list(AppCrossCategory_count.columns)),
        cells=dict(values=[AppCrossCategory_count['cross_category'], AppCrossCategory_count['count']])
    )

    # 設置圖表佈局
    layout = go.Layout(
        autosize=True,
        height=600,
        margin=dict(l=20, r=20, t=20, b=20),
    )

    # 包裝圖表和佈局到Figure物件中
    fig = go.Figure(data=[table], layout=layout)

    # 顯示圖表
    fig.show()



# %%
AppIdCategory_dupl = AppCategory_raw[AppCategory_raw['appId'].isin(AppCategory_raw[AppCategory_raw.duplicated(subset=['appId'])]['appId'].to_list())].sort_values(by='appId',ascending=True)
AppIdCategory_dupl.groupby(by='appId')['category'].count().reset_index().sort_values(by=['category'],ascending=False)
# ** 384個跨類別中 4 個跨3個類別 其餘跨2個類別



# %%
DuplicatedPlot(AppCategory_raw)
# ** 除了GAME，FAMILY 跨類最多



# %% # TODO 爬蟲資料跨類別處理
# TODO 全部遊戲類統整為 GAME 除了博弈

AppCategory_raw.loc[AppCategory_raw['appId'].isin(AppCategory_raw[AppCategory_raw['category'].str.contains('casino',case=False)]['appId'].to_list()),'category'] = 'CASINO'
AppCategory_raw.loc[AppCategory_raw['appId'].isin(AppCategory_raw[AppCategory_raw['category'].str.contains('game',case=False)]['appId'].to_list()),'category'] = 'GAME'

# DuplicatedPlot(AppCategory_raw)

# ** FAMILY & EDUCATION 重疊最高
# ** 兩者重疊的 APP name 大多包含 kids
# ** FAMILY 與其他類別的重疊也是此情況

# TODO 所有包含 FAMILY 的appId 全部劃歸FAMILY

AppCategory_raw.loc[AppCategory_raw['appId'].isin(AppCategory_raw[AppCategory_raw['category'].str.contains('family',case=False)]['appId'].to_list()), 'category'] = 'FAMILY'
# DuplicatedPlot(AppCategory_raw)

# ** 剩下 WATCH_FACE 仍有和其他類別重疊
# ** 分別為 PERSONALIZATION、PHOTOGRAPHY、COMMUNICATION

# TODO WATCH_FACE 重疊的全部畫到其他重疊類別

AppCategory_raw.loc[AppCategory_raw['appId'].isin(AppCategory_raw[AppCategory_raw['category'].str.contains('personalization',case=False)]['appId'].to_list()), 'category'] = 'PERSONALIZATION'
AppCategory_raw.loc[AppCategory_raw['appId'].isin(AppCategory_raw[AppCategory_raw['category'].str.contains('photography',case=False)]['appId'].to_list()), 'category'] = 'PHOTOGRAPHY'
AppCategory_raw.loc[AppCategory_raw['appId'].isin(AppCategory_raw[AppCategory_raw['category'].str.contains('communication',case=False)]['appId'].to_list()), 'category'] = 'COMMUNICATION'
# DuplicatedPlot(AppCategory_raw)

# ** 圖表中重疊的應該都是重複appId

# TODO 相同 appId且 相同類別去重

AppCategory = AppCategory_raw.drop_duplicates(subset=['appId','category'],keep='first')
DuplicatedPlot(AppCategory)

# ** 圖表顯示已無重複類別



# %% # TODO 爬蟲資料已處理 export
AppCategory.duplicated(subset=['appId']).sum()
# ** 暫時無 appId 有跨類別

AppCategory.rename(columns={'title':'name'},inplace=True)
AppCategory.to_excel(r"D:\Model\dataset\已放款用戶\dataset_processed\AppCategory.xlsx",index=False)
AppCategory



# %% # TODO 讀取全部app list raw data
app_list_folder = r"D:\Model\dataset\已放款用戶\dataset_raw\app_list"

items = os.listdir(app_list_folder)
AppsLogRaw = pd.DataFrame()

for item in items:
    if item.endswith('.csv'):
        app_list_path = os.path.join(app_list_folder, item)
        app_list_raw = pd.read_csv(app_list_path)

    AppsLogRaw = pd.concat([AppsLogRaw,app_list_raw])

# TODO 刪除測試號
test_acc = [330696, 196761, 196519]
AppsLogRaw = AppsLogRaw[~AppsLogRaw['user_id'].isin(test_acc)].rename(columns={'package_id':'appId'})
AppsLogRaw



# %%
# AppsLogRaw.isna().sum()
# ** app list raw data name na 9539
# ** 有用戶 同個appId 重複問題 僅創建時間不一樣

# TODO 對每個用戶的 appId去重

AppsLogRaw.drop_duplicates(subset=['user_id','appId'],inplace=True)
AppsLogRaw.isna().sum()
#** name na 9490



# %%
AppCategory['name'] = AppCategory['name'].str.upper()

AppCategory_dupl2 = AppCategory.groupby(by=['appId'])['name'].count().reset_index().sort_values(by=['name'],ascending=False)
AppCategory_dupl2
# ** AppCategory appId 沒有對應重複的 name

# %% # TODO 欄位字串正則化處理 AppCategory 填補 AppsLogRaw name
# TODO AppCategory name 刪去 ':' 之後字段

AppCategory['name'] = AppCategory['name'].apply(lambda x : re.sub(':.*|-.*', '', x)).str.replace(' ', '')

# TODO AppsLogRaw name 大寫消除空格

AppsLogRaw['name'] = AppsLogRaw['name'].str.upper().str.replace(' ', '')

# TODO 用對應的appId填補name

app_name_null = AppsLogRaw[AppsLogRaw['name'].isna()].drop(columns=['name'])

AppsLogRaw = AppsLogRaw[~AppsLogRaw['name'].isna()]
app_name_fill = pd.merge(app_name_null,AppCategory[['appId','name']],on='appId',how='left')
AppsLogRaw = pd.concat([AppsLogRaw,app_name_fill])
AppsLogRaw.isna().sum()
# ** AppsLogRaw name na 7461



# %% # TODO AppsLogRaw 內部appId 對應互相填補 name

app_name_NAlist = list(set(AppsLogRaw[AppsLogRaw['name'].isna()]['appId'].to_list()))
app_name_fill2 = AppsLogRaw[(~AppsLogRaw['name'].isna())&(AppsLogRaw['appId'].isin(app_name_NAlist))][['appId','name']]
app_name_fill2.drop_duplicates(subset=['appId'],keep='first',inplace=True)

app_name_null2 = AppsLogRaw[AppsLogRaw['name'].isna()].drop(columns=['name'])
app_name_fill2 = pd.merge(app_name_null2,app_name_fill2,on='appId',how='left')
AppsLogRaw = AppsLogRaw[~AppsLogRaw['name'].isna()]
AppsLogRaw = pd.concat([AppsLogRaw,app_name_fill2])
AppsLogRaw.isna().sum()
# ** AppsLogRaw name na 950



# %% # TODO 使 appId作為唯一識別app 且不會有重複的app name
AppsLogRaw.drop_duplicates(subset=['user_id','appId'],keep='first',inplace=True)
# AppsLogRaw
# ** AppsLogRaw 已填補 app name 且去重 整體 6037241 rows
# ** 使所有 appId 對應的name都一致

AppIdName = AppsLogRaw[['appId','name']]
AppIdName.drop_duplicates(subset=['appId'],keep='first',inplace=True)
# AppIdName

AppsLogRaw.drop(columns=['name'],inplace=True)
AppsLogRaw = pd.merge(AppsLogRaw,AppIdName,on='appId',how='left')
AppsLogRaw



# %% # TODO 類別歸類

AppsLogRaw = AppsLogRaw.merge(
    AppCategory[[
        'appId',
        'category',
        'score',
        'installs',
        'ratings',
        'reviews'
    ]]
,on='appId',how='left')

AppsLogRaw.isna().sum()
# ** 無法歸類 4529419 缺失率 75%



# %% # TODO 已歸類、未歸類 appId export
AppIdCategory = AppsLogRaw[['appId','name','category']].drop_duplicates(subset=['appId'],keep='first')
AppIdCategory.to_excel(r"D:\Model\dataset\已放款用戶\dataset_processed\AppIdCategory.xlsx",index=False)
AppIdCategory



# %% # TODO 對已歸類&未歸類 appId 進行演算法匹配 進一步歸類
AppIdMatch = AppsLogRaw[~AppsLogRaw['category'].isna()][['appId','category']].drop_duplicates(subset=['appId','category'],keep='first')
# AppIdMatch

AppIdNoMatch = AppsLogRaw[AppsLogRaw['category'].isna()][['appId','category']].drop_duplicates(subset=['appId','category'],keep='first')
# AppIdNoMatch

df_A = AppIdNoMatch[['appId']] # 未歸類 appId
df_B = AppIdMatch[['appId']] # 已歸類 appId

df_C = pd.DataFrame(columns=['appId_A', 'appId_B', 'similarity']) # appId匹配表

# 對 A表 的每一筆資料進行遍歷
# 對 B表 的每一筆資料進行遍歷
# 計算兩個字串之間的相似度分數值
# 如果相似度比現有的最大值更大，就更新最大值
# 將相似度最高的字串資料以及相似度數值加入到 C表 中



total_iterations = len(df_A['appId'])
progress_val = math.ceil(total_iterations*0.2)

# TODO levenshtein distance matching

df_levenshtein = df_C.copy()

for i, appId_A in enumerate(df_A['appId']):
    max_similarity = 0
    max_appId_B = ''

    for appId_B in df_B['appId']:
        similarity = 1 - levenshtein_distance(appId_A, appId_B) / max(len(appId_A), len(appId_B))

        if similarity > max_similarity:
            max_similarity = similarity
            max_appId_B = appId_B

    df_levenshtein = df_levenshtein.append({'appId_A': appId_A, 'appId_B': max_appId_B, 'similarity': max_similarity}, ignore_index=True)

    if (i + 1) % progress_val == 0:
        progress = (i + 1) / total_iterations * 100
        print(f"匹配進度:{progress}%")

df_levenshtein.sort_values(by='similarity', ascending=False, inplace=True)
df_levenshtein.to_excel(r"D:\Model\dataset\已放款用戶\dataset_processed\LevenshteinDistance_MatchingResult.xlsx",index=False)
print(df_levenshtein.head(10))



# TODO fuzzywuzzy matching
# https://towardsdatascience.com/string-matching-with-fuzzywuzzy-e982c61f8a84

df_fuzzy = df_C.copy()

for i, appId_A in enumerate(df_A['appId']):
    max_similarity = 0
    max_appId_B = ''

    for appId_B in df_B['appId']:
        similarity = fuzz.ratio(appId_A,appId_B)

        if similarity > max_similarity:
            max_similarity = similarity
            max_appId_B = appId_B

    df_fuzzy = df_fuzzy.append({'appId_A': appId_A, 'appId_B': max_appId_B, 'similarity': max_similarity}, ignore_index=True)

    if (i + 1) % progress_val == 0:
        progress = (i + 1) / total_iterations * 100
        print(f"匹配進度:{progress}%")

df_fuzzy.sort_values(by='similarity', ascending=False, inplace=True)
df_fuzzy.to_excel(r"D:\Model\dataset\已放款用戶\dataset_processed\FuzzyWuzzy_MatchingResult.xlsx",index=False)
print(df_fuzzy.head(10))