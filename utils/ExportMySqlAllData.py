# -*- coding: utf-8 -*-
# @Time : 2022/5/7 9:13
# @Author : hai wan
# @Email : nicewanghai@163.com
# @File : ExportMySqlAllData.py
# @Project : DataCenter
# @Desc : 将Mysql中的数据库导出为sql文件
import os


class InitSql(object):

    @staticmethod
    def export_server_db(database_name, out_sql_name):
        mysqldump_command_dict = {'server': 'localhost', 'user': 'root',
                                  'password': '19971008', 'port': 3306, 'db': database_name}
        # mysqldump 命令
        sql_format = "mysqldump --column-statistics=0 -h%s -u%s -p%s -P%s %s > %s"
        # 生成相应的sql语句
        sql = (sql_format % (mysqldump_command_dict['server'],
                             mysqldump_command_dict['user'],
                             mysqldump_command_dict['password'],
                             mysqldump_command_dict['port'],
                             mysqldump_command_dict['db'],
                             out_sql_name))
        print("执行的导出数据库的sql：" + sql)
        result = os.system(sql)
        return result


if __name__ == '__main__':
    initSql = InitSql()
    # 如果需要批量执行输出，就要确定所有的数据库名，并遍历输出
    initSql.export_server_db("user", "user_copy.sql")

