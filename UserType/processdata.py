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
    select c.*, d.user_name as payer_name,d.user_account as payer_account,d.user_organize as payer_organize,d.user_type as payer_type from (select a.* ,b.payer_id from (select nova.instances.created_at, nova.instances.user_id as creater_id, nova.instances.project_id, nova.instances.vm_state, nova.instances.memory_mb, nova.instances.vcpus,nova.instances.os_type, nova.instances.uuid from nova.instances where nova.instances.deleted = 0) a left join (select keystone.assignment.actor_id as payer_id ,keystone.assignment.target_id  from keystone.assignment where keystone.assignment.role_id = "22f8b7794e5f40f4a21b1c74087936ae") b on a.project_id = b.target_id ) c left join (select a.id as user_id, json_value(a.extra,'$.display_name') as user_name, b.name as user_account, json_value(a.extra,'$.organize_name') as user_organize,json_value(a.extra,'$.user_type') as user_type from keystone.user a left join keystone.local_user b on a.id=b.user_id) d on c.payer_id=d.user_id;
    '''
sql_storage = '''
select e.*, f.user_name as payer_name,f.user_account as payer_account,f.user_organize as payer_organize,f.user_type as payer_type  from (select c.* ,d.payer_id from (select a.*,b.name as type from (select cinder.volumes.created_at, cinder.volumes.id, cinder.volumes.user_id as creater_id, cinder.volumes.project_id, cinder.volumes.size, cinder.volumes.status , cinder.volumes.volume_type_id from cinder.volumes where cinder.volumes.deleted = 0) a left join (select * from cinder.volume_types where cinder.volume_types.deleted = 0) b on a.volume_type_id=b.id ) c left join (select keystone.assignment.actor_id as payer_id ,keystone.assignment.target_id  from keystone.assignment where keystone.assignment.role_id = "22f8b7794e5f40f4a21b1c74087936ae") d on c.project_id = d.target_id  ) e left join (select a.id as user_id, json_value(a.extra,'$.display_name') as user_name, b.name as user_account, json_value(a.extra,'$.organize_name') as user_organize,json_value(a.extra,'$.user_type') as user_type from keystone.user a left join keystone.local_user b on a.id=b.user_id) f on e.payer_id=f.user_id;
'''

sql_snapshot = '''
select g.created_at, g.id, g.volume_id, g.user_id, g.project_id,g.status,g.volume_size,g.display_name,g.volume_type_id,g.type,h.user_account as payer_account,g.payer_id,g.snapshot_size, h.user_name as payer_name,h.user_organize as payer_organize,h.user_type as payer_type from (select e.*, f.value as snapshot_size from (select c.* ,d.payer_id from (select a.*,b.name as type from (select created_at, id, volume_id, user_id, project_id, status, volume_size, display_name, volume_type_id from cinder.snapshots where deleted = 0) a left join (select * from cinder.volume_types where cinder.volume_types.deleted = 0) b on a.volume_type_id=b.id ) c left join (select keystone.assignment.actor_id as payer_id ,keystone.assignment.target_id  from keystone.assignment where keystone.assignment.role_id = "22f8b7794e5f40f4a21b1c74087936ae") d on c.project_id = d.target_id ) e left join (select snapshot_metadata.snapshot_id, snapshot_metadata.value  from cinder.snapshot_metadata where snapshot_metadata.deleted=0 and snapshot_metadata.key = 'size') f on e.id = f.snapshot_id) g left join (select a.id as user_id, json_value(a.extra,'$.display_name') as user_name, b.name as user_account, json_value(a.extra,'$.organize_name') as user_organize,json_value(a.extra,'$.user_type') as user_type from keystone.user a left join keystone.local_user b on a.id=b.user_id) h on g.payer_id=h.user_id;
'''

sql_backup = '''
select e.*, f.user_name as payer_name,f.user_organize as payer_organize,f.user_account as payer_account,f.user_type as payer_type from (select c.* ,d.payer_id from (select a.*, b.value as backup_size from ( select cinder.backups.created_at, cinder.backups.id as backup_id, cinder.backups.volume_id, cinder.backups.user_id as creater_id, cinder.backups.project_id, cinder.backups.size as volume_size from cinder.backups where cinder.backups.deleted=0 ) a left join (select backup_metadata.backup_id, backup_metadata.value  from cinder.backup_metadata where backup_metadata.deleted=0 and backup_metadata.key = 'size') b on a.backup_id = b.backup_id  ) c left join (select keystone.assignment.actor_id as payer_id ,keystone.assignment.target_id  from keystone.assignment where keystone.assignment.role_id = "22f8b7794e5f40f4a21b1c74087936ae") d on c.project_id = d.target_id) e left join (select a.id as user_id, json_value(a.extra,'$.display_name') as user_name, b.name as user_account, json_value(a.extra,'$.organize_name') as user_organize,json_value(a.extra,'$.user_type') as user_type from keystone.user a left join keystone.local_user b on a.id=b.user_id) f on e.payer_id=f.user_id;
'''

sql_newuser = '''
SELECT  * FROM keystone.local_user WHERE user_id IN (select id from keystone.user  WHERE json_value(extra,'$.consumption') > 0 and DATE_SUB(CURDATE(), INTERVAL 7 DAY) <= date(created_at));
'''

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
            print(nu)
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

def Merge(dbdata, csvdata,outputfile):
    csv_data, csv_header = csvdata
    db_data, db_header = dbdata
    output_path = os.path.abspath(outputfile)
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
    #today = str(nowdate.year) + "-" + str(nowdate.month) + "-" + str(nowdate.day) + " " + str(nowdate.hour) + ":00"
    today = str(nowdate.year) + "-" + str(nowdate.month) + "-" + str(nowdate.day)
    merge_data = [[item for item in row]+[csv_data.get(row[10],'NULL'),today] for row in db_data]
    if not merge_data:
        logger.warning("No data retrieved from query %s." % outputfile.split("_")[-1].split(".")[0])
        return 0
    if not content:
        content = [[item for item in db_header + csv_header]]
    if content[-1][-1] == today:
        logger.info("Data has been collected for %s in this hour." % output_path)
        return 0
    content.extend(merge_data)
    try:
        with open(output_path,"w",encoding='utf-8-sig',newline="") as fw:
            csv_writer = csv.writer(fw)
            for item in content:
                csv_writer.writerow(item)
    except:
        logger.error("Open File %s failed" % output_path)
    return 0

if __name__ == "__main__":
    csv_data = RetriveDataFromCsv()
    db_data_1 = RetrieveDataFromMysql(sql_vm)
    outputfile_1 = opt.outputfile + "_vm.csv"
    Merge(db_data_1,csv_data,outputfile_1)
    
    outputfile_2 = opt.outputfile + "_storage.csv"
    db_data_2 = RetrieveDataFromMysql(sql_storage)
    Merge(db_data_2,csv_data,outputfile_2)
    
    outputfile_3 = opt.outputfile + "_backup.csv"
    db_data_3 = RetrieveDataFromMysql(sql_backup)
    Merge(db_data_3,csv_data,outputfile_3)
    
    outputfile_4 = opt.outputfile + "_snapshot.csv"
    db_data_4 = RetrieveDataFromMysql(sql_snapshot)
    Merge(db_data_4,csv_data,outputfile_4)