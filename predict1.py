import akshare as ak
import pandas as pd
import os
import datetime
import pytz
import utils
import concurrent.futures
from datetime import datetime as dt


# 1.自定义三阳开泰
# 连续三天大于6 并且 小于9
def get_sanyang_stocklist():
    results = []
    futurelist = []

    stocks = utils.getAllStocks()

    # A股列表
    print(stocks)

    # 获取当前日期的前三个交易日
    start_date = utils.gettradeDay(3)
    end_date = datetime.datetime.now(pytz.timezone('Asia/Shanghai')).strftime('%Y%m%d')
    print(start_date, end_date)

    # 线程池
    with concurrent.futures.ThreadPoolExecutor(max_workers=200) as executor:
        for row in stocks.itertuples():
            code = getattr(row, '证券代码')
            name = getattr(row, '证券简称')

            future = executor.submit(get_sanyang_stocklist_thread, code, name, start_date, end_date)
            futurelist.append(future)

        for future in concurrent.futures.as_completed(futurelist):
            result = future.result()
            if result != None:
                results.append(result)

    return results


def get_sanyang_stocklist_thread(code, name, start_date, end_date):
    try:
        # 获取三天的历史数据
        stock_zh_a_hist_df = ak.stock_zh_a_hist(
            symbol=code,
            start_date=start_date,
            end_date=end_date
        )

        zhangdiefu = [0, 0, 0]

        i = 0
        for row in stock_zh_a_hist_df.itertuples():
            zhangdiefu[i] = getattr(row, '涨跌幅')
            i = i + 1

        if zhangdiefu[0] > 6 and zhangdiefu[0] < 19 and zhangdiefu[1] > 6 and zhangdiefu[1] < 19 and zhangdiefu[2] > 6 and zhangdiefu[2] < 19:
            return (code, name)

    except Exception as e:
        print("发生异常", code, name, e)
        return None

    return None


# 2. 白武士（代改良）
def get_baiwushi_stocklist():
    results = []
    futurelist = []

    stocks = utils.getAllStocks()

    # A股列表
    # print(stocks)

    # 获取当前日期的前三个交易日
    start_date = utils.gettradeDay(3)
    end_date = datetime.datetime.now(pytz.timezone('Asia/Shanghai')).strftime('%Y%m%d')
    print(start_date, end_date)

    # 线程池
    with concurrent.futures.ThreadPoolExecutor(max_workers=200) as executor:
        for row in stocks.itertuples():
            code = getattr(row, '证券代码')
            name = getattr(row, '证券简称')

            future = executor.submit(get_baiwushi_stocklist_thread, code, name, start_date, end_date)
            futurelist.append(future)

        for future in concurrent.futures.as_completed(futurelist):
            result = future.result()
            if result != None:
                results.append(result)

    return results


def get_baiwushi_stocklist_thread(code, name, start_date, end_date):
    try:
        # 获取三天的历史数据
        stock_zh_a_hist_df = ak.stock_zh_a_hist(
            symbol=code,
            start_date=start_date,
            end_date=end_date
        )

        i = 0
        zhangdiefu = [0, 0, 0]
        kaipanjia = [0, 0, 0]
        shoupanjia = [0, 0, 0]
        max_value = [0, 0, 0]
        min_value = [0, 0, 0]

        for row in stock_zh_a_hist_df.itertuples():
            zhangdiefu[i] = getattr(row, '涨跌幅')
            kaipanjia[i] = getattr(row, '开盘')
            shoupanjia[i] = getattr(row, '收盘')
            max_value[i] = getattr(row, '最高')
            min_value[i] = getattr(row, '最低')

            if zhangdiefu[i] < 1 or zhangdiefu[i] > 6:
                return None

            if (max_value[i] - shoupanjia[i]) / shoupanjia[i] > 0.01:
                return None
            if (kaipanjia[i] - min_value[i]) / kaipanjia[i] > 0.01:
                return None

            i = i + 1

        if kaipanjia[1] < min_value[0] and kaipanjia[2] < min_value[1]:
            return (code, name)

    except Exception as e:
        print("发生异常", code, name, e)
        return None

    return None


# 3. 十字星反转
# 1）选取五天样本
# 2）股价小于30
# 3）前三天下跌，第四天十字星，第五天涨幅6以上
def get_shizixing_stocklist():
    results = []
    futurelist = []

    stocks = utils.getAllStocks()

    # A股列表
    # print(stocks)

    # 获取当前日期的前五个交易日
    start_date = utils.gettradeDay(5)
    end_date = datetime.datetime.now(pytz.timezone('Asia/Shanghai')).strftime('%Y%m%d')
    print(start_date, end_date)

    # 线程池
    with concurrent.futures.ThreadPoolExecutor(max_workers=200) as executor:
        for row in stocks.itertuples():
            code = getattr(row, '证券代码')
            name = getattr(row, '证券简称')

            future = executor.submit(get_shizixing_stocklist_thread, code, name, start_date, end_date)
            futurelist.append(future)

        for future in concurrent.futures.as_completed(futurelist):
            result = future.result()
            if result != None:
                results.append(result)

    return results


def get_shizixing_stocklist_thread(code, name, start_date, end_date):
    try:
        # 获取五天的历史数据
        stock_zh_a_hist_df = ak.stock_zh_a_hist(
            symbol=code,
            start_date=start_date,
            end_date=end_date
        )

        zhangdiefu = [0, 0, 0, 0, 0]
        kaipanjia = [0, 0, 0, 0, 0]
        shoupanjia = [0, 0, 0, 0, 0]
        max_v = [0, 0, 0, 0, 0]
        min_v = [0, 0, 0, 0, 0]

        i = 0
        for item in stock_zh_a_hist_df.itertuples():
            zhangdiefu[i] = getattr(item, '涨跌幅')
            kaipanjia[i] = getattr(item, '开盘')
            shoupanjia[i] = getattr(item, '收盘')
            max_v[i] = getattr(item, '最高')
            min_v[i] = getattr(item, '最低')

            i = i + 1

        # 策略判断
        for v in range(0, 5):
            if kaipanjia[v] > 30:
                return None

        # 1. 前三天下跌
        if zhangdiefu[0] < 0 and zhangdiefu[1] < 0 and zhangdiefu[2] < 0:
            # 2. 第四天十字星
            body_length = abs(kaipanjia[3] - shoupanjia[3])
            upper_shadow = max_v[3] - max(kaipanjia[3], shoupanjia[3])
            lower_shadow = min(kaipanjia[3], shoupanjia[3]) - min_v[3]
            if body_length < 0.05 * shoupanjia[3] and upper_shadow > 2 * body_length and lower_shadow > 2 * body_length:
                # 3. 第五天大阳线
                if zhangdiefu[4] > 6:
                    return (code, name)

    except Exception as e:
        print('发生了异常', code, name, e)
        return None

    return None


def get_celue_sanyang():
    print("获取三阳开泰。。。")
    time1 = dt.now()
    result = get_sanyang_stocklist()
    time2 = dt.now()
    timediff = (time2 - time1).total_seconds()
    print("时间差为", timediff)
    print("---选中值如下----")
    print(result)


def get_celue_baiwushi():
    print("获取三只武士。。。")
    time1 = dt.now()
    result = get_baiwushi_stocklist()
    time2 = dt.now()
    timediff = (time2 - time1).total_seconds()
    print("时间差为", timediff)
    print("---选中值如下----")
    print(result)


def get_celue_shizixing():
    print("获取十字星反转。。。")
    time1 = dt.now()
    result = get_shizixing_stocklist()
    time2 = dt.now()
    timediff = (time2 - time1).total_seconds()
    print("时间差为", timediff)
    print("---选中值如下----")
    print(result)


if __name__ == '__main__':
    get_celue_sanyang()
    get_celue_baiwushi()
    get_celue_shizixing()
