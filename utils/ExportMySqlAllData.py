# -*- coding: utf-8 -*-
# @Time : 2022/5/7 9:13
# @Author : hai wan
# @Email : nicewanghai@163.com
# @File : ExportMySqlAllData.py
# @Project : DataCenter
# @Desc : 将Mysql中的数据库导出为sql文件
import os
import time
import pymysql
"""
#***** 执行程序为出现一个warning如下，原因是mysql的密码在控制台显示，不安全 ******#
#***** 但是无法对导出的mysql的conf文件进行修改，所以此warning不可避免     ******#
# mysqldump: [Warning] Using a password on the command line interface can be insecure.
"""


class InitSql(object):

    def __init__(self):
        # 确定mysql地址 用户名，密码和端口
        self. mysqldump_command_dict = {'server': 'localhost', 'user': 'root',
                                        'password': '19971008', 'port': 3306}
        # 创建导出sql文件目录
        now = time.localtime()
        nowt = time.strftime("%Y%m%d%H%M", now)
        self.out_path = './sqlFiles_{}'.format(nowt)

    def create_file_path(self):
        os.mkdir(self.out_path)

    def show_database_name(self):
        # show所有数据库名
        databases = []
        mcd_temp = self.mysqldump_command_dict
        conn = pymysql.connect(host=mcd_temp['server'], port=mcd_temp['port'], user=mcd_temp['user'],
                               passwd=mcd_temp['password'], db='user')
        cursor = pymysql.cursors.Cursor(conn)
        sql_word = "show databases;"
        cursor.execute(sql_word)
        for cursor_all in cursor.fetchall():
            databases.append(str(cursor_all).replace("('", "").replace("',)", ""))
        return databases

    def export_server_db(self, database_name, out_sql_name):
        # 导出数据库为sql文件
        mcd_temp = self.mysqldump_command_dict
        # mysqldump 命令
        sql_format = "mysqldump --single-transaction --column-statistics=0 -h%s -u%s -p%s -P%s %s > %s"
        # 生成相应的sql语句
        sql = (sql_format % (mcd_temp['server'],
                             mcd_temp['user'],
                             mcd_temp['password'],
                             mcd_temp['port'],
                             database_name,
                             self.out_path + "/" + out_sql_name))
        print("执行的导出数据库的sql：" + sql)
        result = os.system(sql)
        return result


if __name__ == '__main__':
    initSql = InitSql()
    initSql.create_file_path()
    # 输出指定名的数据库
    show_database_name = initSql.show_database_name()
    print(show_database_name)
    # 如果需要批量执行输出，就要确定所有的数据库名，并遍历输出
    for dn in show_database_name:
        initSql.export_server_db(dn, dn + ".sql")

