from czsc.utils.MyTT import *
import tushare as ts
import czsc.utils.tt_utils as tu
import datetime
import mysql_utils as mysqlUtils
def mytt_invok():
    pro = ts.pro_api()
    data = pro.query('stock_basic', exchange='SSE', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    codes = data["ts_code"]
    list = []
    for index, code in enumerate(codes):
        print(f"正在检测代码: {code}\n")
        close, high, low, open = find_data(code)
        cleaning = CLEANING(open, close, high, low)
        flag = tu.CLEANING_TACTICS(cleaning, 20, 10)
        if flag:
            stock_data = data.head(index)
            list.append(stock_data)

    mysqlUtils.insert()
    print("------------结果------------")
    print(list)

    # pd.Series([5.02, 4.87, 4.76, 4.59, 4.61, 4.56, 4.5,  4.73, 4.69, 4.72, 4.83, 4.88]).rolling(5).min().values


def find_data(code):
    pro = ts.pro_api()
    now = datetime.datetime.now().date()
    date_string = now.strftime("%Y%m%d")
    df = pro.daily(ts_code=code, start_date='20220320', end_date=date_string)
    close = df['close'].values[::-1]
    open = df['open'].values[::-1]
    low = df['low'].values[::-1]
    high = df['high'].values[::-1]
    return close, high, low, open


if __name__ == '__main__':
    mytt_invok()
