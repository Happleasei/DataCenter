# -*- coding: utf-8 -*-
# @Time : 2022/5/6 16:04
# @Author : hai wan
# @Email : nicewanghai@163.com
# @File : GetMySqlToInfluxdb.py
# @Project : DataCenter
import datetime
import random
from influxdb import InfluxDBClient
import pymysql
import time

# 键接influxdb
client = InfluxDBClient('localhost', 8086, 'root', '******', 'mytestdb', timeout=10)

# 链接myslq
conn = pymysql.connect(host="localhost", port=3306, user="root", passwd="******", db="demo")
cursor = pymysql.cursors.Cursor(conn)

# 查询mysql列表
sql = "select distinct symbol,exchange from dbbardata"
cursor.execute(sql)

# 将mysql标的信息写入列表供循环写入influx用
symbol_list = []


# 处理mysql获取的信息供influx写入使用.
def to_influx(info):
    info_influx = {"measurement": "bar_data", "tags": {"interval": "1m", "vt_symbol": info[0] + "." + info[1]},
                   "time": info[-1], "fields": {"open_price": info[2], "high_price": info[3], "low_price": info[4],
                                                "close_price": info[5], "open_interest": info[6], "volume": info[7]}}
    return info_influx


# 获取所有商品列表
while True:
    row = cursor.fetchone()
    if not row:
        break
    # print(row[0])
    new_info = {"symbol": row[0], "exchange": row[1]}
    symbol_list.append(new_info)

# 循环写入
for i in symbol_list:
    print("开始读取:", i["symbol"])
    start_time = time.time()
    cursor.execute(
        "select symbol,exchange,open_price,high_price,low_price,close_price,open_interest,volume,datetime from dbbardata where symbol='%s'" %
        i["symbol"])
    row = [to_influx(i) for i in cursor.fetchall()]
    time_end = time.time() - start_time
    print("读取完毕,耗时:", time_end, "秒,现在开始写入:", i["symbol"], len(row))
    row_count = len(row)  # 总数
    start_count = 0  # 开始点
    end_count = 100000  # 初始结束点
    count = 100000  # 每次数量
    range_count = (row_count // count) + 1  # 写入次数
    for ii in range(range_count):
        if ii == range_count - 1:
            client.write_points(row[start_count:-1], database="demo")
            print(i["symbol"], "写入完成", start_count, len(row))
            continue
        print(i["symbol"], "写入", start_count, end_count)
        client.write_points(row[start_count:end_count], database="demo")
        row_count -= count
        start_count += count
        end_count = start_count + count

conn.commit()

# 关闭mysql游标
cursor.close()

# 关闭mysql连接
conn.close()

# 关闭influx链接
client.close()