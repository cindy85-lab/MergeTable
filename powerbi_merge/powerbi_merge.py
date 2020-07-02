import argparse
import csv
import pymysql
from urllib.parse import urlparse
import os
from datetime import datetime
import logging
from collections import OrderedDict


parser = argparse.ArgumentParser()
opt = parser.parse_args()

logger = logging.getLogger('DataAnalysis')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('da.log')
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)



sql_vm = '''
    select c.*, d.user_name as payer_name,d.user_account as payer_account,d.user_organize as payer_organize,d.user_type as payer_type from (select a.* ,b.payer_id from (select nova.instances.created_at, nova.instances.user_id as creater_id, nova.instances.project_id, nova.instances.vm_state, nova.instances.memory_mb, nova.instances.vcpus,nova.instances.os_type, nova.instances.uuid from nova.instances where nova.instances.deleted = 0) a left join (select keystone.assignment.actor_id as payer_id ,keystone.assignment.target_id  from keystone.assignment where keystone.assignment.role_id = "22f8b7794e5f40f4a21b1c74087936ae") b on a.project_id = b.target_id ) c left join (select a.id as user_id, json_value(a.extra,'$.display_name') as user_name, b.name as user_account, json_value(a.extra,'$.organize_name') as user_organize,json_value(a.extra,'$.user_type') as user_type from keystone.user a left join keystone.local_user b on a.id=b.user_id) d on c.payer_id=d.user_id;
    '''

sql_vm_merge = '''
   INSERT INTO jcloud_resource.vm_merge(created_time,creater_id,project_id,vm_state,memory_mb,vcpus,os_type,uuid,payer_id,payer_name,payer_account,payer_organize,payer_type,analyze_time,payer_classification)  SELECT a.*, b.user_classification FROM   jcloud_resource.vm  a LEFT JOIN   jcloud_resource.user_type  b ON a.payer_account = b.user_account;
   '''    

sql_disk = '''
select e.*, f.user_name as payer_name,f.user_account as payer_account,f.user_organize as payer_organize,f.user_type as payer_type  from (select c.* ,d.payer_id from (select a.*,b.name as type from (select cinder.volumes.created_at, cinder.volumes.id, cinder.volumes.user_id as creater_id, cinder.volumes.project_id, cinder.volumes.size, cinder.volumes.status , cinder.volumes.volume_type_id from cinder.volumes where cinder.volumes.deleted = 0) a left join (select * from cinder.volume_types where cinder.volume_types.deleted = 0) b on a.volume_type_id=b.id ) c left join (select keystone.assignment.actor_id as payer_id ,keystone.assignment.target_id  from keystone.assignment where keystone.assignment.role_id = "22f8b7794e5f40f4a21b1c74087936ae") d on c.project_id = d.target_id  ) e left join (select a.id as user_id, json_value(a.extra,'$.display_name') as user_name, b.name as user_account, json_value(a.extra,'$.organize_name') as user_organize,json_value(a.extra,'$.user_type') as user_type from keystone.user a left join keystone.local_user b on a.id=b.user_id) f on e.payer_id=f.user_id;
'''

# sql_disk_merge = '''
   # INSERT INTO jcloud_resource.disk_merge(created_time,id,creater_id,project_id,size,status,volume_type_id,type,payer_id,payer_name,payer_account,payer_organize,payer_type,analyze_time,payer_classification)  SELECT a.*, b.user_classification FROM   jcloud_resource.disk  a LEFT JOIN   jcloud_resource.user_type  b ON a.payer_account = b.user_account;
   # '''  
sql_disk_merge = '''
   INSERT INTO jcloud_resource.disk_merge  SELECT a.*, b.user_classification FROM   jcloud_resource.disk  a LEFT JOIN   jcloud_resource.user_type  b ON a.payer_account = b.user_account;
   '''      

sql_snapshot = '''
select g.created_at, g.id, g.volume_id, g.user_id, g.project_id,g.status,g.volume_size,g.display_name,g.volume_type_id,g.type,h.user_account as payer_account,g.payer_id,g.snapshot_size, h.user_name as payer_name,h.user_organize as payer_organize,h.user_type as payer_type from (select e.*, f.value as snapshot_size from (select c.* ,d.payer_id from (select a.*,b.name as type from (select created_at, id, volume_id, user_id, project_id, status, volume_size, display_name, volume_type_id from cinder.snapshots where deleted = 0) a left join (select * from cinder.volume_types where cinder.volume_types.deleted = 0) b on a.volume_type_id=b.id ) c left join (select keystone.assignment.actor_id as payer_id ,keystone.assignment.target_id  from keystone.assignment where keystone.assignment.role_id = "22f8b7794e5f40f4a21b1c74087936ae") d on c.project_id = d.target_id ) e left join (select snapshot_metadata.snapshot_id, snapshot_metadata.value  from cinder.snapshot_metadata where snapshot_metadata.deleted=0 and snapshot_metadata.key = 'size') f on e.id = f.snapshot_id) g left join (select a.id as user_id, json_value(a.extra,'$.display_name') as user_name, b.name as user_account, json_value(a.extra,'$.organize_name') as user_organize,json_value(a.extra,'$.user_type') as user_type from keystone.user a left join keystone.local_user b on a.id=b.user_id) h on g.payer_id=h.user_id;
'''

sql_snapshot_merge = '''
   INSERT INTO jcloud_resource.snapshot_merge SELECT a.*, b.user_classification FROM  jcloud_resource.snapshot  a LEFT JOIN jcloud_resource.user_type  b ON a.payer_account = b.user_account;
   ''' 

sql_backup = '''
select e.*, f.user_name as payer_name,f.user_organize as payer_organize,f.user_account as payer_account,f.user_type as payer_type from (select c.* ,d.payer_id from (select a.*, b.value as backup_size from ( select cinder.backups.created_at, cinder.backups.id as backup_id, cinder.backups.volume_id, cinder.backups.user_id as creater_id, cinder.backups.project_id, cinder.backups.size as volume_size from cinder.backups where cinder.backups.deleted=0 ) a left join (select backup_metadata.backup_id, backup_metadata.value  from cinder.backup_metadata where backup_metadata.deleted=0 and backup_metadata.key = 'size') b on a.backup_id = b.backup_id  ) c left join (select keystone.assignment.actor_id as payer_id ,keystone.assignment.target_id  from keystone.assignment where keystone.assignment.role_id = "22f8b7794e5f40f4a21b1c74087936ae") d on c.project_id = d.target_id) e left join (select a.id as user_id, json_value(a.extra,'$.display_name') as user_name, b.name as user_account, json_value(a.extra,'$.organize_name') as user_organize,json_value(a.extra,'$.user_type') as user_type from keystone.user a left join keystone.local_user b on a.id=b.user_id) f on e.payer_id=f.user_id;
'''

sql_backup_merge = '''
   INSERT INTO jcloud_resource.backup_merge SELECT a.*, b.user_classification FROM   jcloud_resource.backup  a LEFT JOIN   jcloud_resource.user_type  b ON a.payer_account = b.user_account;
   ''' 

sql_object = '''
SELECT cloudkitty.rated_data_frames.resource_id as object_id, cloudkitty.rated_data_frames.tenant_id AS project_id, cloudkitty.rated_data_frames.payer_id,   (cloudkitty.rated_data_frames.qty/TIMESTAMPDIFF(HOUR, cloudkitty.rated_data_frames.begin, cloudkitty.rated_data_frames.END))  AS size FROM cloudkitty.rated_data_frames WHERE cloudkitty.rated_data_frames.res_type='ceph_account' AND cloudkitty.rated_data_frames.active=1 and cloudkitty.rated_data_frames.component='radosgw_objects_size'
'''

sql_object_merge = '''
insert into object_merge (SELECT c.*,d.user_classification FROM (SELECT a.object_id,a.project_id,a.size,a.payer_id,b.user_name AS payer_name, b.user_account AS payer_account, b.user_organize AS payer_organize, b.user_type AS payer_type, a.type, a.tag, a.analyze_time FROM object a LEFT JOIN userinfo b ON a.payer_id=b.user_id)c LEFT JOIN user_type d ON c.payer_account = d.user_account)
'''

sql_storage_merge = '''
insert into storage_merge SELECT disk_merge.payer_name, disk_merge.payer_account,disk_merge.payer_organize, disk_merge.payer_classification, disk_merge.`type`, disk_merge.tag ,disk_merge.analyze_time, SUM(disk_merge.size) as size FROM disk_merge GROUP BY disk_merge.payer_name, disk_merge.payer_account,disk_merge.payer_organize, disk_merge.payer_classification, disk_merge.`type`, disk_merge.tag,disk_merge.analyze_time union all SELECT backup_merge.payer_name, backup_merge.payer_account,backup_merge.payer_organize, backup_merge.payer_classification, backup_merge.`type`, backup_merge.tag, backup_merge.analyze_time, SUM(backup_merge.backup_size) as size FROM backup_merge GROUP BY backup_merge.payer_name, backup_merge.payer_account,backup_merge.payer_organize, backup_merge.payer_classification, backup_merge.`type`, backup_merge.tag,backup_merge.analyze_time union all SELECT snapshot_merge.payer_name, snapshot_merge.payer_account,snapshot_merge.payer_organize, snapshot_merge.payer_classification, snapshot_merge.`type`, snapshot_merge.tag ,snapshot_merge.analyze_time, SUM(snapshot_merge.snapshot_size) as size FROM snapshot_merge GROUP BY snapshot_merge.payer_name, snapshot_merge.payer_account,snapshot_merge.payer_organize, snapshot_merge.payer_classification, snapshot_merge.`type`, snapshot_merge.tag,snapshot_merge.analyze_time union all SELECT object_merge.payer_name, object_merge.payer_account,object_merge.payer_organize, object_merge.payer_classification, object_merge.`type`, object_merge.tag ,object_merge.analyze_time, SUM(object_merge.size) as size FROM object_merge GROUP BY object_merge.payer_name, object_merge.payer_account,object_merge.payer_organize, object_merge.payer_classification, object_merge.`type`, object_merge.tag,object_merge.analyze_time
'''

sql_userinfo = '''
select a.id as user_id, json_value(a.extra,'$.display_name') as user_name, b.name as user_account, json_value(a.extra,'$.organize_name') as user_organize,json_value(a.extra,'$.user_type') as user_type from keystone.user a left join keystone.local_user b on a.id=b.user_id
'''

sql_newuser = '''
SELECT  * FROM keystone.local_user WHERE user_id IN (select id from keystone.user  WHERE json_value(extra,'$.consumption') > 0 and DATE_SUB(CURDATE(), INTERVAL 1 DAY) <= date(created_at));
'''

#s1,s2 are source database, d1 is destination database
s1 = ('10.119.0.15',3306,'root','ImzZy7TDHvAW1lb84vw5QW6vweXg3pSwnc6Ra0eA','')
s2 = ('10.119.0.123',3306,'root','ImzZy7TDHvAW1lb84vw5QW6vweXg3pSwnc6Ra0eA','cloudkitty')
d1 = ('10.119.1.128',3306,'root','Jcloud_zytj@2020','jcloud_resource')

# 连接设置 连接数据部mysql 用户名user_CJ 密码Xiejing@2019 地址10.119.1.101：3306 database：testdb_CJ
class DBOpen():
    def __init__(self,host,port,user,password,database_name=""):
        self.__db_host = host
        self.__db_port = port
        self.__db_user = user
        self.__db_password = password
        self.__db_database = database_name
        self.__db = None
    def __enter__(self):
        self.__db = pymysql.connect(
                    host=self.__db_host,
                    port=self.__db_port,
                    user=self.__db_user,
                    password=self.__db_password,
                    database=self.__db_database,
                    charset='utf8'
                )
        return self.__db
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__db.close()
    
class DatabaseAccess():
	#初始化属性
    def __init__(self,db_descriptor):
        host,port,user,password,database_name= db_descriptor
        self.__db_host = host
        self.__db_port = port
        self.__db_user = user
        self.__db_password = password
        self.__db_database = database_name
        self.__db = None
        
    def getData(self,sql):
        try:
            with DBOpen(self.__db_host,self.__db_port,self.__db_user,self.__db_password,self.__db_database) as db:
                with db.cursor() as cursor:
                    cursor.execute(sql)
                    data = cursor.fetchall()
                    num_fields = len(cursor.description)
                    field_names = [i[0] for i in cursor.description]
        except Exception as e:
            logger.error("%s : %s" % (str(e), sql))
            data,field_names = [],[]

        return (data,field_names)

    def genInsSql(self,tablename, row, current_time):
        print("here is tablename:%s" %tablename)
        if (tablename == "jcloud_resource.disk"):
            tag = "云硬盘"
        elif (tablename == "jcloud_resource.backup"):
            tag = "备份"
            type = "低频对象"
        elif (tablename == "jcloud_resource.snapshot"):
            tag = "快照"
        elif (tablename == "jcloud_resource.object"):
            tag = "对象存储"
            type = "标准对象"
        else: 
            tag = 'NULL'
            type = 'NULL'
        sql = "insert into %s values (" % tablename
        for item in row:
            if isinstance(item,datetime):
                sql += "'" +str(item) + "',"
            elif isinstance(item,str):
                sql += "'" + item + "',"
            elif isinstance(item,int):
                sql += str(item) + ","
            elif isinstance(item,float):
                sql += str(item) + ","
            elif item is None:
                sql += 'NULL' + ","
            else:
                sql += str(item) + ","
        if (tablename == "jcloud_resource.backup" or tablename == "jcloud_resource.object" ):
            sql += "'" +str(type) + "',"+"'" +str(tag) + "',"+"'"+ str(current_time) + "')"
        elif (tablename == "jcloud_resource.vm"): 
            sql += "'"+ str(current_time) + "')"
        else:
            sql += "'" +str(tag) + "',"+"'"+ str(current_time) + "')"
        print ("the sql is:%s" % sql)
        return sql
    def insertDataSet(self,tablename, dataset,current_time):
        print("33333")
        tbname = "jcloud_resource.%s" % tablename
        try:
            with DBOpen(self.__db_host,self.__db_port,self.__db_user,self.__db_password,self.__db_database) as db:
                try:
                    with db.cursor() as cursor:
                        cursor.execute("delete from %s" % tbname)
                        print ("delete old data")
                        db.commit()
                        for rowdata in dataset:
                            sql = self.genInsSql(tbname,rowdata,current_time)
                            if sql:
                                cursor.execute(sql)
                        db.commit()
                except Exception as e:
                    logger.error("%s : %s" % (str(e), sql))
                    db.rollback()
        except Exception as e:
            logger.error("%s : %s" % (str(e), self.__db_host))
    
def UpdateMergeData(tablename,sql,db_descriptor):
    tbname = "jcloud_resource.%s" % tablename
    host,port,user,password,database_name= db_descriptor
    print("1111111")
    try:
        with DBOpen(host,port,user,password,database_name) as db:
            try:
                with db.cursor() as cursor:
                    if(tablename == "storage_merge"):
                        cursor.execute("delete from %s" % tbname)
                        print ("delete old data")
                        db.commit()
                    cursor.execute(sql)
                db.commit()
            except Exception as e:
                logger.error("%s : %s" % (str(e), sql))
                db.rollback()
    except Exception as e:
        logger.error("%s : %s" % (str(e), self.__db_host))    

def Merge(dbdata,outputfile):
    print("1111111")
    db_data, db_header = dbdata
    tablename = os.path.basename(outputfile)

    nowdate = datetime.now()
    today = str(nowdate.year) + "-" + str(nowdate.month) + "-" + str(nowdate.day) + " " + str(nowdate.hour) + ":00"
    #today = str(nowdate.year) + "-" + str(nowdate.month) + "-" + str(nowdate.day)
    merge_data = [[item for item in row] for row in db_data]
    if not merge_data:
        logger.warning("No data retrieved from query %s." % outputfile.split("_")[-1].split(".")[0])
        return 0
    insert_jcloud_mergetable(tablename,merge_data)
    return 0

def insert_jcloud_mergetable(tablename,dataset):
    print("22222")
    nowdate = datetime.now()
    today = str(nowdate.year) + "-" + str(nowdate.month) + "-" + str(nowdate.day) + " " + str(nowdate.hour) + ":00"
    db_target_1 = DatabaseAccess(d1)
    sql = "select max(ANALYZE_TIME) from jcloud_resource.%s" % tablename
    #if (datetime.strptime(today,'%Y-%m-%d %H:%M') != db_target_1.getData(sql)[0][0][0]):
    db_target_1.insertDataSet(tablename, dataset,today)

if __name__ == "__main__":
    db_source_1 = DatabaseAccess(s1)
    db_source_2 = DatabaseAccess(s2)
    
    # db_data_1 = db_source_1.getData(sql_vm)
    # outputfile_1 = "vm"
    # Merge(db_data_1,outputfile_1)
    
    # outputfile_2 = "disk"
    # db_data_2 = db_source_1.getData(sql_disk)
    # Merge(db_data_2,outputfile_2)
    
    # outputfile_3 = "backup"
    # db_data_3 = db_source_1.getData(sql_backup)
    # Merge(db_data_3,outputfile_3)
    
    # outputfile_4 = "snapshot"
    # db_data_4 = db_source_1.getData(sql_snapshot)
    # Merge(db_data_4,outputfile_4)
    
    # outputfile_5 = "object"
    # db_data_5 = db_source_2.getData(sql_object)
    # Merge(db_data_5,outputfile_5)
    
    # outputfile_6 = "userinfo"
    # db_data_6 = db_source_1.getData(sql_userinfo)
    # Merge(db_data_6,outputfile_6)
    
    outputfile_7 = "vm_merge"
    UpdateMergeData(outputfile_7,sql_vm_merge,d1)
    
    # outputfile_8 = "disk_merge"
    # UpdateMergeData(outputfile_8,sql_disk_merge,d1)
   
    # outputfile_9 = "backup_merge"
    # UpdateMergeData(outputfile_9,sql_backup_merge,d1)
    
    # outputfile_10 = "snapshot_merge"
    # UpdateMergeData(outputfile_10,sql_snapshot_merge,d1)
    
    # outputfile_11 = "object_merge"
    # UpdateMergeData(outputfile_11,sql_object_merge,d1)
    
    # outputfile_12 = "storage_merge"
    # UpdateMergeData(outputfile_12,sql_storage_merge,d1)
    
