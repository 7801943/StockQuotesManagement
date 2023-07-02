import pandas as pd
import pickle
import os
import time
import tushare as ts
import enum


token = '6815b3ea0ec3cc19474beff10e82d6b59bc651c9557c45d15dc5c0d2'
stocks_list_file = './stocks_list/stocks.pkl'
daily_bar_dictionary = './stocks_daily/'


class DailyDataColumns(enum.Enum):
    HIGH = 'high'
    LOW = 'low'
    CLOSE = 'close'
    VOLUME = 'volume'


class StockListManager:
    def __init__(self, file_path: str = stocks_list_file):
        self.file_path = file_path
        self.stock_list = self.load_list_file()
        # 从Tushare API更新股票列表
        self.pro = ts.pro_api(token)  # 替换为您自己的Tushare API token

    def load_list_file(self):
        try:
            with open(self.file_path, 'rb') as f:
                stock_list = pickle.load(f)
                return stock_list
        except FileNotFoundError:
            return pd.DataFrame(columns=['ts_code', 'symbol', 'name', 'area', 'industry', 'list_date'])

    def save_list_file(self, data):
        #目录不存在则创建
        directory = os.path.dirname(self.file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)
        with open(self.file_path, 'wb') as f:
            pickle.dump(data, f)

    '''用于数据库更新，未实现'''
    def save_list_database(self, data):
        pass

    def load_list_database(self):
        from sqlalchemy import create_engine, text

        stock_list_table_name = 'stocks_list'
        # 设置数据库连接信息
        database_url = 'mysql+pymysql://stocks:Zhangying139@192.168.0.102:3306/stocks?charset=utf8&use_unicode=1'
        # 创建数据库引擎
        engine = create_engine(database_url)

        # 执行SQL查询并将结果加载到DataFrame
        # 替换为你的表名或自定义的查询语句
        query1 = 'SELECT * FROM ' + stock_list_table_name
        #query = 'SELECT list_date,ts_code FROM ' + stock_list_table_name + ' WHERE market = \'主板\'OR market = \'中小板\' OR market = \'科创板\' OR market = \'科创板\''

        df = pd.read_sql(sql=text(query1), con=engine.connect())
        return df

    '''比较数据源，获取不一致的'''
    def compare_stock_lists(self, data):
        existing_stocks = set(self.stock_list['ts_code'])
        #data_stocks = set(data['ts_code'])
        new_stocks = data[~data['ts_code'].isin(existing_stocks)]
        return new_stocks

    def update_from_api(self):
        # 获取上市股票列表
        data = self.pro.query('stock_basic',
                              exchange='',
                              list_status='L',
                              fields='ts_code,symbol,name,area,industry,list_date')
        self.save_list_file(data)
        #self.update_to_file(data)

    '''更新到数据库，未实现'''
    def update_from_database(self, database, time):
        pass
        # 这里假设有一个从数据库获取股票列表的方法，返回一个pandas.DataFrame
        # data = database.get_stock_list(time)
        # self.save_list_database(data)

    '''获取指定股票的数据，未测试'''
    def query_stock(self, query):
        result = self.stock_list
        for key, value in query.items():
            result = result[result[key].str.contains(value, case=False)]
        return result


class StockDataFetcher:
    def __init__(self, list_file, data_dir):
        self.stock_list_file = list_file
        self.data_dir = data_dir
        #self.pro = ts.pro_api()
        self.pro = ts.pro_api('6815b3ea0ec3cc19474beff10e82d6b59bc651c9557c45d15dc5c0d2')

    def fetch_stock_data(self, stock_codes=None):
        if stock_codes:
            stock_list = self.load_stock_list()
            stock_list = stock_list[stock_list['ts_code'].isin(stock_codes)]
        else:
            stock_list = self.load_stock_list()

        for _, row in stock_list.iterrows():
            ts_code = row['ts_code']
            symbol = row['symbol']
            name = row['name']
            list_date = row['list_date']
            data_file = os.path.join(self.data_dir, f'{ts_code}.pkl')

            if os.path.isfile(data_file):
                df = pd.read_pickle(data_file)
            else:
                #print(f'{name} 上市日期:{list_date}\n')
                #获取所有数据，写入到文件
                df = self.fetch_data_from_api(ts_code, list_date)
                df.to_pickle(data_file)  # 保存数据到本地文件
                print(f'save {data_file} done.\n')
                continue  # 跳过后续处理，继续下一轮循环

            new_data = self.fetch_data_from_api(ts_code, df.iloc[-1]['trade_date'])

            if not new_data.empty:
                new_data = self.sort_dataframe_by_date(new_data, 'trade_date')
                pd.concat([df, new_data], ignore_index=True)
                df.to_pickle(data_file)  # 更新保存数据到本地文件
            else:
                print(f"{ts_code}空数据,未更新")

            # Process the stock data dataframe here
            # print(f"Stock: {ts_code} - {symbol} - {name}")
            # print(df)

    def load_stock_list(self):
        return pd.read_pickle(self.stock_list_file)

    '''以日期排序'''
    def sort_dataframe_by_date(self, df, date_column, ascending=True, date_format='%Y%m%d'):
        df[date_column] = pd.to_datetime(df[date_column], format=date_format)  # 将日期列转换为日期格式
        df.sort_values(date_column, ascending=ascending, inplace=True)
        df[date_column] = df[date_column].dt.strftime(date_format)  # 将日期列转换回指定的日期格式
        df.reset_index(drop=True, inplace=True)
        return df

    '''截止2023-6-30，tushare 返回的最大长度是6000条，因此设置了time_window'''
    def fetch_data_from_api(self, ts_code, start_date, time_window=2000):
        end_date = pd.Timestamp.now().strftime('%Y%m%d')
        data = pd.DataFrame()
        while True:
            if pd.Timestamp(start_date) + pd.DateOffset(days=time_window) <= pd.Timestamp(end_date):
                window_end_date = (pd.Timestamp(start_date) + pd.DateOffset(days=time_window)).strftime('%Y%m%d')
            else:
                window_end_date = end_date
            print(f"from {start_date} to {window_end_date}\n")
            df = self.pro.daily(ts_code=ts_code, start_date=start_date, end_date=window_end_date)
            if df.empty:
                print('get empty.')
                break
            #data = pd.concat([data, df], ignore_index=True)
            #判断返回数据的日期顺序
            if df.iloc[0]['trade_date'] > df.iloc[-1]['trade_date']:
                start_date = (pd.to_datetime(df.iloc[0]['trade_date']) + pd.DateOffset(days=1)).strftime('%Y%m%d')
                '''注意拼接顺序'''
                data = pd.concat([df, data], ignore_index=True)
            else:
                '''注意拼接顺序'''
                start_date = (pd.to_datetime(df.iloc[-1]['trade_date']) + pd.DateOffset(days=1)).strftime('%Y%m%d')
                data = pd.concat([data, df], ignore_index=True)
            if start_date >= end_date:
                print("end_date est.")
                break
            #一分钟300次，tushare的限制是500次
            time.sleep(0.2)
            print("next...\n")
        return data


def main():

    #manager = StockListManager()
    # 更新股票列表，每天一次即可
    # manager.update_from_api()
    #以上

    #df_list = manager.load_list_file()
    s = StockDataFetcher(list_file=stocks_list_file, data_dir=daily_bar_dictionary)
    s.fetch_stock_data(['000001.SZ'])


    # 从接口更新股票列表
    #manager.update_from_api()

    # 从数据库更新股票列表
    #manager.update_from_database(database, time)

    # 执行其他操作...

if __name__ == '__main__':
    main()
