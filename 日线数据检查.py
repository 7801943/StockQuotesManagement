import os
import pickle
from enum import Enum
import pandas as pd
import tushare as ts
import tqdm
import time

r = pd.read_excel("check_result.xlsx")
s = pd.read_pickle("./stocks_daily/all_data.pkl")

for index, row in r.iterrows():
    for column, value in row.items():
        # 对每个元素执行操作
        if isinstance(value, list) or isinstance(value, str):
            print(s[row[index]].loc[value, :])
