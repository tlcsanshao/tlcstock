import pandas as pd
from pandas.tseries.offsets import CustomBusinessDay
from pandas_market_calendars import get_calendar
import akshare as ak

def getStocksInSH():
    # 获取A股所有的上市公司的代码和名称
    stock_info = ak.stock_info_sh_name_code(symbol="主板A股")
    # print(stock_info)
    return stock_info


def getStocksInSZ():
    # 获取A股所有的上市公司的代码和名称
    stock_info = ak.stock_info_sz_name_code(symbol="A股列表")
    # print(stock_info)
    return stock_info

#  获取所股票有的
def getAllStocks():
    # 上海和深圳相加
    stock1 = getStocksInSH()
    stock2 = getStocksInSZ()

    stock2 = stock2.rename(columns={'A股代码': '证券代码', 'A股简称': '证券简称'})

    newstock1 = pd.DataFrame({'证券代码': stock1['证券代码'], '证券简称': stock1['证券简称']})
    newstock2 = pd.DataFrame({'证券代码': stock2['证券代码'], '证券简称': stock2['证券简称']})

    allstocks = pd.concat([newstock1, newstock2]).reset_index(drop=True)
    return allstocks




# 获取当前日期之前的第N个交易日，所对应的日期
def gettradeDay(value):
    # 获取中国交易所的交易日历
    china_calendar = get_calendar('XSHG')  # XSHG 代表上海证券交易所

    # 获取今天的日期
    today = pd.Timestamp.today().normalize()


    # 获取今天之前的三个交易日
    previous_trading_days = china_calendar.valid_days(end_date=today, start_date=today - pd.Timedelta(days=30))

    daylist = list()

    for day in previous_trading_days:
        daylist.append(day.date())
    currentday = daylist[-value]
    currentdate = currentday.strftime('%Y%m%d')

    return currentdate


# 返回两个日期之间所有的交易日期数组
def gettardeDayList(start_date,end_date):
    # 获取上证交易所的交易日历
    calendar = get_calendar('XSHG')  # XSHG 代表上海证券交易所

    # 获取交易日期数组
    trading_dates = calendar.valid_days(start_date=start_date, end_date=end_date)

    # 将交易日期数组转换为字符串形式
    trading_dates_str = [date.strftime('%Y%m%d') for date in trading_dates]

    return trading_dates_str

if __name__ == '__main__':
    gettardeDayList('20180101','20240401')