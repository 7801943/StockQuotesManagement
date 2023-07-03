import os
import pickle
from enum import Enum
import pandas as pd
import tushare as ts
import tqdm
import time
# 数据源2
import akshare as ak
from datetime import datetime

token = '6815b3ea0ec3cc19474beff10e82d6b59bc651c9557c45d15dc5c0d2'

# 上海市场第一只股票上市时间
SH_start_date = '19901219'

daily_bar_data = './stocks_daily/all_data.pkl'


class Column(Enum):
    TRADE_DATE = 'trade_date'
    OPEN = 'open'
    HIGH = 'high'
    LOW = 'low'
    CLOSE = 'close'
    PRE_CLOSE = 'pre_close'
    CHANGE = 'change'
    PCT_CHG = 'pct_chg'
    VOL = 'vol'
    AMOUNT = 'amount'


class StockDataHandler:
    def __init__(self, pkl_file):
        self.pkl_file = pkl_file
        self.data = {}
        self.pro = ts.pro_api(token)

    def create_pkl_file(self):
        with open(self.pkl_file, 'wb') as f:
            pickle.dump(self.data, f)

    def load_pkl_file(self):
        if os.path.isfile(self.pkl_file):
            with open(self.pkl_file, 'rb') as f:
                self.data = pickle.load(f)
        print(f"load {self.pkl_file} done.")

    '''
    
    '''

    def query_api_data(self, start_date, end_date):

        date_range = pd.date_range(start=start_date, end=end_date, freq='D')

        for date in tqdm.tqdm(date_range):
            # 一分钟360次，tushare的限制是500次
            time.sleep(0.15)
            trade_date = date.strftime('%Y%m%d')
            df = self.pro.daily(trade_date=trade_date)

            # 非交易日，空值
            if df.empty:
                print("空值\n")
                continue
            length = len(df)
            for idx, row in df.iterrows():
                # print(f'\nprocedding:{idx}/{length}', end='\033[F')
                ts_code = row['ts_code']
                if ts_code in self.data:
                    self.data[ts_code] = pd.concat([self.data[ts_code], pd.DataFrame([row])], ignore_index=True)
                else:
                    self.data[ts_code] = pd.DataFrame([row])
        print(f'{start_date} to {end_date} done.')

    def update_data(self, end_time=None):
        self.load_pkl_file()

        if not self.data:
            # 从开始开始
            start_date = pd.Timestamp(SH_start_date)
        else:
            last_row_dates = {}

            for ts_code, df in self.data.items():
                last_row = df.iloc[-1]
                last_row_dates[ts_code] = last_row[Column.TRADE_DATE.value]

            start_date = pd.to_datetime(max(last_row_dates.values())) + pd.DateOffset(days=1)

        # 指定日期，放置因为网络问题等更新中断没有保存
        if not end_time:
            end_date = pd.Timestamp.now().strftime('%Y%m%d')
        else:
            end_date = end_time

        if start_date >= pd.to_datetime(end_date):
            print('起始日期错误.\n')
            return

        self.query_api_data(start_date.strftime('%Y%m%d'), end_date)
        self.create_pkl_file()

    def check_data(self, columns=None, max_value=100000000, write_to_file=False):
        if columns:
            pass
        else:
            columns = [Column.OPEN.value,
                       Column.HIGH.value,
                       Column.LOW.value,
                       Column.CLOSE.value,
                       Column.PRE_CLOSE.value,
                       Column.CHANGE.value,
                       Column.PCT_CHG.value,
                       Column.VOL.value,
                       Column.AMOUNT.value]

        # Check specified columns for null values or values greater than max_value

        result = pd.DataFrame(index=list(self.data.keys()), columns=[c for c in columns])

        # 对于每一个dataframe进行遍历
        for ts_code, df in tqdm.tqdm(self.data.items()):
            # 按列遍历
            for column in columns:
                null_dates = df.loc[df[column].isnull(), 'trade_date'].tolist()
                high_value_dates = df.loc[df[column] > max_value, 'trade_date'].tolist()

                if null_dates or high_value_dates:
                    result.loc[ts_code, column] = null_dates + high_value_dates

        # 删除数据ok的行,此结果返回的是一个以股票代码为index的dataframe，其中有异常值的列会包含异常值日期列表
        result.dropna(how='all', inplace=True)

        #输出有问题的行
        for index, row in result.iterrows():
            for column, value in row.items():
                # 对每个元素执行操作
                if isinstance(value, list) or isinstance(value, str):
                    if value:
                        print(self.data[index].loc[self.data[index]['trade_date'].isin(value)])

        if write_to_file:
            with open("check_result.pkl", 'wb') as f:
                pickle.dump(result, f)

        return result


def main():
    s = StockDataHandler(daily_bar_data)
    # s.update_data()
    s.load_pkl_file()
    # s.data_valid_check()
    r = s.check_data()




if __name__ == '__main__':
    main()
