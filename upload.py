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
    def getData(self,sql):
        self.isConnectionOpen()
        try:
            with self.__db.cursor() as cursor:
                cursor.execute(sql)
                data = cursor.fetchall()
                num_fields = len(cursor.description)
                field_names = [i[0] for i in cursor.description]
        except Exception as e:
            logger.error("%s : %s" % (str(e), sql))
            data,field_names = [],[]
        finally:
            self.__db.close()
        return (data,field_names)

    def insertData(self,user_account,project_name,vm_uuid,vm_name,vm_state,vcpus,memory_mb, timestamp):
        sql = "insert into testdb_CJ.jcloud_mergetable_vm values ('%s','%s','%s','%s','%s',%d,%d,'%s')" % (user_account,project_name,vm_uuid,vm_name,vm_state,vcpus,memory_mb,timestamp)
        try:
            self.isConnectionOpen()
            with self.__db.cursor() as cursor:
                cursor.execute(sql)
                self.__db.commit()
        except Exception as e:
            logger.error("%s : %s" % (str(e), sql))
            self.__db.rollback()
        finally:
            self.__db.close()
    def insertDataSet(self,dataset,current_time):
        try:
            self.isConnectionOpen()
            with self.__db.cursor() as cursor:
                for user_account,project_name,vm_uuid,vm_name,vm_state,vcpus,memory_mb in dataset:
                    sql = "insert into testdb_CJ.jcloud_mergetable_vm values ('%s','%s','%s','%s','%s',%d,%d,'%s')" % (user_account,project_name,vm_uuid,vm_name,vm_state,vcpus,memory_mb,current_time)
                    cursor.execute(sql)
                self.__db.commit()
        except Exception as e:
            logger.error("%s : %s" % (str(e), sql))
            self.__db.rollback()
        finally:
            self.__db.close()

if __name__ == "__main__":
	#创建实例化对象
    nowdate = datetime.now()
    today = str(nowdate.year) + "-" + str(nowdate.month) + "-" + str(nowdate.day) + " " + str(nowdate.hour) + ":00"
    user_account,project_name,vm_uuid,vm_name,vm_state,vcpus,memory_mb,timestamp = "jcloud_agri","jcloud_agri_project","6a8fa1a6-882f-4691-a4a5-fa69f1eaa177","农生院智能办公系统","active",4,16384,today
    db=DatabaseAccess()
    
    sql = "select max(TIMESTAMP) from testdb_CJ.jcloud_mergetable_vm"
    if (datetime.strptime(today,'%Y-%m-%d %H:%M') != db.getData(sql)[0][0][0]):
        db.insertData(user_account,project_name,vm_uuid,vm_name,vm_state,vcpus,memory_mb,timestamp)
    sql = "SELECT * FROM `testdb_CJ`.`jcloud_mergetable_vm` LIMIT 1000;"
    print(db.getData(sql))
 
 
    
    


