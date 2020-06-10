import argparse
import csv
import pymysql
from urllib.parse import urlparse
import os
from datetime import datetime
import logging
from collections import OrderedDict
from upload import DatabaseAccess
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
    SELECT * FROM (SELECT e.user_account, f.name as project_name, e.vm_uuid, e.vm_name, e.vm_state, e.vcpus, e.memory_mb FROM (select c.*, d.user_account from (select a.* ,b.payer_id from (select nova.instances.uuid as vm_uuid, nova.instances.display_name as vm_name, nova.instances.vm_state, nova.instances.vcpus, nova.instances.memory_mb, nova.instances.project_id from nova.instances where nova.instances.deleted = 0) a left join (select keystone.assignment.actor_id as payer_id ,keystone.assignment.target_id  from keystone.assignment where keystone.assignment.role_id = "22f8b7794e5f40f4a21b1c74087936ae") b on a.project_id = b.target_id ) c left join (select a.id as user_id, b.name as user_account  from keystone.user a left join keystone.local_user b on a.id=b.user_id ) d on c.payer_id = d.user_id )e  left join keystone.project f on e.project_id=f.id )g WHERE g.user_account REGEXP 'jcloud_' ORDER BY g.user_account
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

#sql_newuser = '''
#SELECT  * FROM keystone.local_user WHERE user_id IN (select id from keystone.user  WHERE json_value(extra,'$.consumption') > 0 and DATE_SUB(CURDATE(), INTERVAL 7 DAY) <= date(created_at));
#'''

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

    db = pymysql.connect(hostname,username,password)

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

def RetriveDataFromCsv():
    
    new_user,_ = RetrieveDataFromMysql(sql_newuser)
    csvfile = os.path.abspath(opt.csvfile)
    try:
        with open(csvfile,"r",encoding='utf-8-sig') as fr:
            content = OrderedDict([(item[0].strip(),item[1].strip()) for item in csv.reader(fr)])
    except:
        logger.error("Open file %s failed." % csvfile)
        exit(-1)
    header = ["payer_classic","analyze_time"]
    newuser = (set(new_user) - set(content.keys()))
    if newuser:
        for nu in newuser:
            content[nu[3]] = '待定'
        try:
            with open(csvfile,"w",newline='', encoding='utf-8-sig') as fw:
                usertypewriter = csv.writer(fw)
                for username,usertype in content.items():
                    usertypewriter.writerow([username,usertype])
        except:
            logger.error("Write file %s failed." % csvfile)
            exit(-1)
    return (content,header)

def Merge(dbdata,outputfile):
   # csv_data, csv_header = csvdata
    db_data, db_header = dbdata
    output_path = os.path.abspath(outputfile+".csv")
    tablename = os.path.basename(outputfile)
    try:
        with open(output_path,"r",encoding='utf-8-sig') as fr:
            content = [item for item in csv.reader(fr) if item]
    except:
        if os.path.exists(output_path):
            logger.error("Open File %s failed" % output_path)
            return 0
        else:
            content = []
    nowdate = datetime.now()
    today = str(nowdate.year) + "-" + str(nowdate.month) + "-" + str(nowdate.day) + " " + str(nowdate.hour) + ":00"
    #today = str(nowdate.year) + "-" + str(nowdate.month) + "-" + str(nowdate.day)
    merge_data = [[item for item in row] for row in db_data]
    if not merge_data:
        logger.warning("No data retrieved from query %s." % outputfile.split("_")[-1].split(".")[0])
        return 0
    if not content:
        content = [[item for item in db_header]]
   # if content[-1][-1] == today:
    #    logger.info("Data has been collected for %s in this hour." % output_path)
   #     return 0
    content.extend(merge_data)
    insert_jcloud_mergetable(tablename,merge_data)
    try:
        with open(output_path,"w",encoding='utf-8-sig',newline="") as fw:
            csv_writer = csv.writer(fw)
            for item in content:
                csv_writer.writerow(item)
    except:
        logger.error("Open File %s failed" % output_path)
    return 0

def insert_jcloud_mergetable(tablename,dataset):
    nowdate = datetime.now()
    today = str(nowdate.year) + "-" + str(nowdate.month) + "-" + str(nowdate.day) + " " + str(nowdate.hour) + ":00"
    user_account,project_name,vm_uuid,vm_name,vm_state,vcpus,memory_mb,timestamp = "jcloud_agri","jcloud_agri_project","6a8fa1a6-882f-4691-a4a5-fa69f1eaa177","农生院智能办公系统","active",4,16384,today
    db=DatabaseAccess()
    
    sql = "select max(TIMESTAMP) from testdb_CJ.jcloud_%s" % tablename
    if (datetime.strptime(today,'%Y-%m-%d %H:%M') != db.getData(sql)[0][0][0]):
        db.insertDataSet(tablename, dataset,today)
    sql = "SELECT * FROM `testdb_CJ`.`jcloud_%s` LIMIT 1000;" % tablename
    print(db.getData(sql))
    
if __name__ == "__main__":
    test_insert()

if __name__ == "__main__1":
    #csv_data = RetriveDataFromCsv()
    db_data_1 = RetrieveDataFromMysql(sql_vm)
    outputfile_1 = opt.outputfile + "_vm"
    Merge(db_data_1,outputfile_1)
    
    outputfile_2 = opt.outputfile + "_storage"
    db_data_2 = RetrieveDataFromMysql(sql_storage)
    Merge(db_data_2,outputfile_2)
    
    outputfile_3 = opt.outputfile + "_backup"
    db_data_3 = RetrieveDataFromMysql(sql_backup)
    Merge(db_data_3,outputfile_3)
    
    outputfile_4 = opt.outputfile + "_snapshot"
    db_data_4 = RetrieveDataFromMysql(sql_snapshot)
    Merge(db_data_4,outputfile_4)