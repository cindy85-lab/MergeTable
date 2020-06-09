import pandas as pd
import pymysql
from sqlalchemy import create_engine
from datetime import datetime
from sqlalchemy.types import NVARCHAR, Float, Integer

# 连接设置 连接mysql 用户名ffzs 密码666 地址localhost：3306 database：stock
class DatabaseAccess():
	#初始化属性
    def __init__(self):
        self.__db_host = "10.119.1.101"
        self.__db_port = 3306
        self.__db_user = "user_CJ"
        self.__db_password = "Xiejing@2019"
        self.__db_database = "testdb_CJ"
	#链接数据库
    def isConnectionOpen(self):
        self.__db = pymysql.connect(
            host=self.__db_host,
            port=self.__db_port,
            user=self.__db_user,
            password=self.__db_password,
            database=self.__db_database,
            charset='utf8'
        )
if __name__ == "__main__":
	#创建实例化对象
    db=DatabaseAccess()
    db.isConnectionOpen()
 
 
    
    


