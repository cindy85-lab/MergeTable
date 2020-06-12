import argparse
import csv
import pymysql
from urllib.parse import urlparse
import os
from datetime import datetime
import logging
from collections import OrderedDict


parser = argparse.ArgumentParser()

parser.add_argument("--db", dest="dburl", help="mysql database url. mysql:\\username:password@hostname",required=True)
parser.add_argument("--csvfile", dest="csvfile", help="cvs file path",required=True)
parser.add_argument("--output", dest="outputfile", help="output file path",required=True)
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
    SELECT * FROM (SELECT e.user_account, f.name as project_name, e.vm_name, e.vm_uuid, e.vm_state, e.vcpus, e.memory_mb FROM (select c.*, d.user_account from (select a.* ,b.payer_id from (select nova.instances.uuid as vm_uuid, nova.instances.display_name as vm_name, nova.instances.vm_state, nova.instances.vcpus, nova.instances.memory_mb, nova.instances.project_id from nova.instances where nova.instances.deleted = 0) a left join (select keystone.assignment.actor_id as payer_id ,keystone.assignment.target_id  from keystone.assignment where keystone.assignment.role_id = "22f8b7794e5f40f4a21b1c74087936ae") b on a.project_id = b.target_id ) c left join (select a.id as user_id, b.name as user_account  from keystone.user a left join keystone.local_user b on a.id=b.user_id ) d on c.payer_id = d.user_id )e  left join keystone.project f on e.project_id=f.id )g WHERE g.user_account REGEXP 'jcloud_' ORDER BY g.user_account
;
    '''
sql_storage = '''
SELECT f.user_account as payer_account, e.instance_uuid as vm_uuid,e.display_name AS volume_name, e.size AS volume_size_gb from (SELECT * FROM (select cinder.volumes.id,cinder.volumes.project_id, cinder.volumes.size,cinder.volumes. display_name  from cinder.volumes where cinder.volumes.deleted = 0 AND cinder.volumes.attach_status='attached') a LEFT JOIN (SELECT cinder.volume_attachment.volume_id, cinder.volume_attachment.instance_uuid FROM cinder.volume_attachment WHERE cinder.volume_attachment.deleted = 0) b ON a.id = b.volume_id ) e left join (select c.vm_uuid, d.user_account from (select a.* ,b.payer_id from (select nova.instances.uuid as vm_uuid, nova.instances.project_id from nova.instances where nova.instances.deleted = 0) a left join (select keystone.assignment.actor_id as payer_id ,keystone.assignment.target_id  from keystone.assignment where keystone.assignment.role_id = "22f8b7794e5f40f4a21b1c74087936ae") b on a.project_id = b.target_id ) c left join (select a.id as user_id, b.name as user_account  from keystone.user a left join keystone.local_user b on a.id=b.user_id ) d on c.payer_id = d.user_id ) f on e.instance_uuid=f.vm_uuid  WHERE f.user_account REGEXP 'jcloud_' ORDER BY f.user_account
;
'''

sql_snapshot = '''
SELECT k.user_account AS payer_account, l.name as project_name,  k.display_name AS snapshot_name, k.snapshot_size_gb from (SELECT  g.project_id, g.display_name, g.payer_id, g. snapshot_size_gb, h.name AS user_account from (select e.*, f.value as snapshot_size_gb from (select c.* ,d.payer_id from ( select id,  project_id,  display_name from cinder.snapshots where deleted = 0 ) c left join (select keystone.assignment.actor_id as payer_id ,keystone.assignment.target_id  from keystone.assignment where keystone.assignment.role_id = "22f8b7794e5f40f4a21b1c74087936ae") d on c.project_id = d.target_id ) e left join (select snapshot_metadata.snapshot_id, snapshot_metadata.value  from cinder.snapshot_metadata where snapshot_metadata.deleted=0 and snapshot_metadata.key = 'size') f on e.id = f.snapshot_id ) g left JOIN (select keystone.local_user.user_id, keystone.local_user.name  FROM keystone.local_user) h on g.payer_id=h.user_id WHERE h.name REGEXP 'jcloud_' ) k left join  keystone.project l on k.project_id=l.id ORDER BY k.user_account;
'''

sql_backup = '''
select g.payer_account, h.name as project_name, g.backup_name, g.backup_size_gb  From (select e.*, f.name as payer_account from (select c.* ,d.payer_id from (select a.*, b.value as backup_size_gb from ( select  cinder.backups.id as backup_id, cinder.backups.display_name as backup_name, cinder.backups.project_id from cinder.backups where cinder.backups.deleted=0 ) a left join (select backup_metadata.backup_id, backup_metadata.value  from cinder.backup_metadata where backup_metadata.deleted=0 and backup_metadata.key = 'size') b on a.backup_id = b.backup_id  ) c left join (select keystone.assignment.actor_id as payer_id ,keystone.assignment.target_id  from keystone.assignment where keystone.assignment.role_id = "22f8b7794e5f40f4a21b1c74087936ae") d on c.project_id = d.target_id) e left join (select keystone.local_user.user_id, keystone.local_user.name  FROM keystone.local_user) f on e.payer_id=f.user_id WHERE f.name REGEXP 'jcloud_' ) g left join keystone.project h on g.project_id=h.id ORDER BY h.name;
'''

sql_ip_null = '''
SELECT g.payer_account, h.project_name, g.floating_ip_address, g.fixed_port_id as device_owner, g.fixed_port_id as device_name from (SELECT e.*,f.name AS payer_account FROM (SELECT a.project_id,  a.floating_ip_address,  a.fixed_port_id , b.payer_id FROM (SELECT neutron.floatingips.project_id, neutron.floatingips.floating_ip_address, neutron.floatingips.fixed_port_id FROM neutron.floatingips ) a LEFT JOIN (select keystone.assignment.actor_id as payer_id ,keystone.assignment.target_id  from keystone.assignment where keystone.assignment.role_id = "22f8b7794e5f40f4a21b1c74087936ae") b on a.project_id = b.target_id ) e left join (select keystone.local_user.user_id, keystone.local_user.name  FROM keystone.local_user) f on e.payer_id=f.user_id WHERE f.name REGEXP 'jcloud_' AND e.fixed_port_id IS NULL AND floating_ip_address REGEXP '111.')g  left JOIN (select keystone.project.name AS project_name, keystone.project.id FROM keystone.project) h on g.project_id=h.id
'''

sql_ip_vm = '''
SELECT k.payer_account, k.project_name, k.floating_ip_address, k.device_owner, m.vm_name as device_name FROM (SELECT * FROM (SELECT g.payer_account, h.project_name, g.floating_ip_address, g.fixed_port_id from (SELECT e.*,f.name AS payer_account FROM (SELECT a.project_id,  a.floating_ip_address,  a.fixed_port_id, a.fixed_ip_address , b.payer_id FROM (SELECT neutron.floatingips.project_id, neutron. floatingips.floating_ip_address, neutron.floatingips.fixed_port_id, neutron.floatingips.fixed_ip_address FROM neutron.floatingips ) a LEFT JOIN (select keystone.assignment.actor_id as payer_id ,keystone.assignment.target_id  from keystone.assignment where keystone.assignment.role_id = "22f8b7794e5f40f4a21b1c74087936ae") b on a.project_id = b.target_id ) e left join (select keystone.local_user.user_id, keystone.local_user.name  FROM keystone.local_user) f on e.payer_id=f.user_id WHERE f.name REGEXP 'jcloud_' AND e.fixed_port_id IS not NULL  AND e.floating_ip_address REGEXP '111.' AND e.fixed_ip_address IS NOT NULL)g  left JOIN (select keystone.project.name AS project_name, keystone.project.id FROM keystone.project) h on g.project_id=h.id ) i LEFT JOIN (select neutron.ports.id, neutron.ports.device_id , neutron.ports.device_owner from neutron.ports  ) j ON i.fixed_port_id=j.id ) k LEFT JOIN (SELECT nova.instances.display_name AS vm_name, nova.instances.uuid FROM nova.instances WHERE nova.instances.deleted = 0) m ON k.device_id = m.uuid WHERE k.device_owner REGEXP 'compute:nova'
'''

sql_ip_router = '''
SELECT m.payer_account, m.project_name, m.floating_ip_address, m.device_owner, n.name AS device_name FROM (SELECT * FROM (SELECT g.payer_account, h.project_name, g.floating_ip_address, g.fixed_port_id, g.fixed_ip_address from (SELECT e.*,f.name AS payer_account FROM (SELECT a.project_id,  a.floating_ip_address,  a.fixed_port_id, a.fixed_ip_address , b.payer_id FROM (SELECT neutron.floatingips.project_id, neutron.floatingips.floating_ip_address, neutron.floatingips.fixed_port_id, neutron.floatingips.fixed_ip_address FROM neutron.floatingips ) a LEFT JOIN (select keystone.assignment.actor_id as payer_id ,keystone.assignment.target_id  from keystone.assignment where keystone.assignment.role_id = "22f8b7794e5f40f4a21b1c74087936ae") b on a.project_id = b.target_id ) e left join (select keystone.local_user.user_id, keystone.local_user.name  FROM keystone.local_user) f on e.payer_id=f.user_id WHERE f.name REGEXP 'jcloud_' AND e.floating_ip_address REGEXP '111.' AND e.fixed_port_id IS NOT NULL AND e.fixed_ip_address IS null )g  left JOIN (select keystone.project.name AS project_name, keystone.project.id FROM keystone.project) h on g.project_id=h.id ) i LEFT JOIN (select neutron.ports.id, neutron.ports.device_id , neutron.ports.device_owner from neutron.ports  ) j ON i.fixed_port_id=j.id)m LEFT JOIN (SELECT neutron.routers.name, neutron.routers.id FROM neutron.routers) n ON m.device_id = n.id
'''

sql_ip_database = '''
SELECT k.payer_account, k.project_name, k.floating_ip_address, k.device_owner, m.database_name as device_name FROM (SELECT * FROM (SELECT g.payer_account, h.project_name, g.floating_ip_address, g.fixed_port_id from (SELECT e.*,f.name AS payer_account FROM (SELECT a.project_id,  a.floating_ip_address,  a.fixed_port_id, a.fixed_ip_address , b.payer_id FROM (SELECT neutron.floatingips.project_id, neutron.floatingips.floating_ip_address, neutron.floatingips.fixed_port_id, neutron.floatingips.fixed_ip_address FROM neutron.floatingips ) a LEFT JOIN (select keystone.assignment.actor_id as payer_id ,keystone.assignment.target_id  from keystone.assignment where keystone.assignment.role_id = "22f8b7794e5f40f4a21b1c74087936ae") b on a.project_id = b.target_id ) e left join (select keystone.local_user.user_id, keystone.local_user.name  FROM keystone.local_user) f on e.payer_id=f.user_id WHERE f.name REGEXP 'jcloud_' AND e.fixed_port_id IS not NULL  AND e.floating_ip_address REGEXP '111.' AND e.fixed_ip_address IS NOT NULL)g  left JOIN (select keystone.project.name AS project_name, keystone.project.id FROM keystone.project) h on g.project_id=h.id ) i LEFT JOIN (select neutron.ports.id, neutron.ports.device_id , neutron.ports.device_owner from neutron.ports  ) j ON i.fixed_port_id=j.id ) k LEFT JOIN (SELECT trove.clusters.name AS database_name, trove.clusters.id FROM trove.clusters WHERE trove.clusters.deleted = 0) m ON k.device_id = m.id WHERE k.device_owner REGEXP 'database'
'''

# 连接设置 连接数据部mysql 用户名user_CJ 密码Xiejing@2019 地址10.119.1.101：3306 database：testdb_CJ
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
        try:
            self.__db = pymysql.connect(
                host=self.__db_host,
                port=self.__db_port,
                user=self.__db_user,
                password=self.__db_password,
                database=self.__db_database,
                charset='utf8'
            )
        except Exception as e:
            logger.error("%s : %s" % (str(e), self.__db_host))
            exit(-1)
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

    # def insertData(self,tablename, user_account,project_name,vm_name,vm_state,vcpus,memory_mb, timestamp):
        # sql = "insert into testdb_CJ.jcloud_%s values ('%s','%s','%s','%s',%d,%d,'%s')" % (tablename, user_account,project_name,vm_name,vm_state,vcpus,memory_mb,timestamp)
        # try:
            # self.isConnectionOpen()
            # with self.__db.cursor() as cursor:
                # cursor.execute(sql)
                # self.__db.commit()
        # except Exception as e:
            # logger.error("%s : %s" % (str(e), sql))
            # self.__db.rollback()
        # finally:
            # self.__db.close()
    def genSql(self,tablename, row, current_time):
        sql = "insert into %s values (" % tablename
        for item in row:
            if isinstance(item,str):
                sql += "'" + item + "',"
            elif isinstance(item,int):
                sql += str(item) + ","
            elif isinstance(item,float):
                sql += str(item) + ","
            elif item is None:
                sql += 'NULL' + ","
            else:
                logger.error(",".join([str(i) for i in row]))
                return ""
        sql += "'" + str(current_time) + "')"
        return sql
    def insertDataSet(self,tablename, dataset,current_time):
        tbname = "testdb_CJ.jcloud_%s" % tablename
        try:
            self.isConnectionOpen()
            with self.__db.cursor() as cursor:
                for rowdata in dataset:
                    sql = self.genSql(tbname,rowdata,current_time)
                    if sql:
                        cursor.execute(sql)
                self.__db.commit()
        except Exception as e:
            logger.error("%s : %s" % (str(e), sql))
            self.__db.rollback()
        finally:
            self.__db.close()


def RetrieveDataFromMysql(sql):
    url_rt = urlparse(opt.dburl)
    
    if url_rt.scheme != "mysql":
        logger.error("Error. dburl is invalid")
        exit(-1)
    if "@" in url_rt.netloc:
        userinfo, hostname = url_rt.netloc.split("@")
        if ":" in userinfo:
            username, password = userinfo.split(":")
        else:
            username = userinfo
            password = ""
    else:
        hostname = url_rt.netloc
        username = ""
        password = ""   
    try:
        db = pymysql.connect(hostname,username,password)
    except Exception as e:
        logger.error("%s : %s" % (str(e), hostname))
        exit(-1)

    try:
        with db.cursor() as cursor:
            cursor.execute(sql)
            data = cursor.fetchall()
            num_fields = len(cursor.description)
            field_names = [i[0] for i in cursor.description]
    except Exception as e:
        logger.error("%s : %s" % (str(e), sql))
        data,field_names = [],[]
    finally:
        db.close()
    return (data,field_names)


def Merge(dbdata,outputfile):

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
    nowdate = datetime.now()
    today = str(nowdate.year) + "-" + str(nowdate.month) + "-" + str(nowdate.day) + " " + str(nowdate.hour) + ":00"
    db=DatabaseAccess()
    # sql = "select max(TIMESTAMP) from testdb_CJ.jcloud_%s" % tablename
    # if (datetime.strptime(today,'%Y-%m-%d %H:%M') != db.getData(sql)[0][0][0]):
        # db.insertDataSet(tablename, dataset,today)
    db.insertDataSet(tablename, dataset,today)

if __name__ == "__main__":
    # db_data_1 = RetrieveDataFromMysql(sql_vm)
    # outputfile_1 = opt.outputfile + "_vm"
    # Merge(db_data_1,outputfile_1)
    
    # outputfile_2 = opt.outputfile + "_storage"
    # db_data_2 = RetrieveDataFromMysql(sql_storage)
    # Merge(db_data_2,outputfile_2)
    
    outputfile_3 = opt.outputfile + "_backup"
    db_data_3 = RetrieveDataFromMysql(sql_backup)
    Merge(db_data_3,outputfile_3)
    
    # outputfile_4 = opt.outputfile + "_snapshot"
    # db_data_4 = RetrieveDataFromMysql(sql_snapshot)
    # Merge(db_data_4,outputfile_4)
    
    # outputfile_5 = opt.outputfile + "_ip"
    # db_data_5 = RetrieveDataFromMysql(sql_ip_null)
    # Merge(db_data_5,outputfile_5)
    
    # outputfile_6 = opt.outputfile + "_ip"
    # db_data_6 = RetrieveDataFromMysql(sql_ip_vm)
    # Merge(db_data_6,outputfile_6)
    
    # outputfile_7 = opt.outputfile + "_ip"
    # db_data_7 = RetrieveDataFromMysql(sql_ip_router)
    # Merge(db_data_7,outputfile_7)
    
    # outputfile_8 = opt.outputfile + "_ip"
    # db_data_8 = RetrieveDataFromMysql(sql_ip_database)
    # Merge(db_data_8,outputfile_8)