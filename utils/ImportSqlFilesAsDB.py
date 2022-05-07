# -*- coding: utf-8 -*-
# @Time : 2022/5/7 8:49
# @Author : hai wan
# @Email : nicewanghai@163.com
# @File : ImportSqlFilesAsDB.py
# @Project : DataCenter
# @Desc : 将sql文件导入到新创建的数据库中
import pymysql
from pymysql import ProgrammingError
import os


class ImportSqlFilesAsDB(object):

    @ staticmethod
    def create_database_and_import_sql_files(create_database_name, input_sql_name):
        # 默认进入一个数据库中
        conn = pymysql.connect(host="localhost", port=3306, user="root", passwd="19971008", db="user")
        cursor = pymysql.cursors.Cursor(conn)
        try:
            # 创建对应导入sql文件名的数据库
            sql_01 = "create database {}".format(create_database_name)
            cursor.execute(sql_01)
        except ProgrammingError:
            print("数据库{}已存在".format(create_database_name))
        # 导入sql文件
        os.system("mysql -uroot  -P3306 -p19971008  {} < ./{}".format(create_database_name, input_sql_name))


if __name__ == "__main__":
    importSql = ImportSqlFilesAsDB()
    #  可遍历创建database，导入sql文件
    importSql.create_database_and_import_sql_files("user_copy", "user_copy.sql")

