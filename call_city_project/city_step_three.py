#!/usr/local/bin/python3
# -*- coding:utf-8 -*-

from city.field_check import new_airport_insert,add_others_source_city
from city.add_city import read_file
from city.share_airport import update_share_airport
from city.city_map_ciytName import revise_pictureName
from city.update_city_pic import update_city_pic
from city.db_insert import shareAirport_insert
from city.config import config,base_path,OpCity_config,ota_config
import os
import sys
import zipfile
from collections import defaultdict
import json
import traceback
import pymysql
pymysql.install_as_MySQLdb()
import configparser
import pymongo
def get_zip_path(param):
    conn = pymysql.connect(**OpCity_config)
    cursor = conn.cursor()
    select_sql = "select path1 from city_order  where id=%s"
    path = ''
    try:
        cursor.execute(select_sql,(param,))
        path = cursor.fetchone()[0]
    except Exception as e:
        conn.rollback()
    return path

def update_step_report(csv_path,param,step_front,step_after):
    conn = pymysql.connect(**OpCity_config)
    cursor = conn.cursor()
    update_sql_front = "update city_order set report3=%s,step3=%s where id=%s"
    update_sql_after = "update city_order set step4=%s where id=%s"
    try:
       cursor.execute(update_sql_front,(csv_path,step_front,param))
       cursor.execute(update_sql_after,(step_after,param))
       conn.commit()
    except Exception as e:
        conn.rollback()
    finally:
        conn.close()

def task_start():
    param = sys.argv[1]
    zip_path = get_zip_path(param)
    file_name = zip_path.split('/')[-1]
    zip_path = ''.join([base_path,file_name])
    zip = zipfile.ZipFile(zip_path)
    file_name = zip.filename.split('.')[0].split('/')[-1]
    path = ''.join([base_path, str(param), '/'])
    save_path = []
    database_name = ''.join(['add_city_',param])
    temp_config = config
    temp_config['db'] = database_name
    if path.endswith('/'):
        file_path = ''.join([path, file_name])
    else:
        file_path = '/'.join([path, file_name])
    file_list = os.listdir(file_path)
    hotels_path = None
    for child_file in file_list:
        path = '/'.join([file_path, child_file])
        if '新增城市' in child_file:
            city_path = path
        elif '新增机场' in child_file:
            airport_path = path
        elif os.path.isdir(path):
            picture_path = path
        elif '酒店配置' in child_file:
            hotels_path = path
        elif '景点配置' in child_file:
            attr_path = path

    conf = configparser.ConfigParser()
    conf.read('/search/cuixiyi/ks3up-tool-2.0.6-20170801/city.conf', encoding='utf-8')
    conf.set('city','srcPrefix',picture_path)
    conf.write(open('/search/cuixiyi/ks3up-tool-2.0.6-20170801/city.conf','w'))

    try:
        return_result = defaultdict(dict)
        return_result['data'] = {}
        return_result['error']['error_id'] = 0
        return_result['error']['error_str'] = ''

        city_insert_path = read_file(city_path,temp_config,param)

        city_map_path = revise_pictureName(picture_path,temp_config,param)
        if city_map_path:
            city_map_path = '/'.join([param,city_map_path])
            temp_path = ''.join([base_path,city_map_path])
            os.system("rsync -vI {0} 10.10.150.16::opcity/{1}".format(temp_path,param))
            save_path.append(city_map_path)
  
        city_pic_path = update_city_pic(picture_path,temp_config,param)
        if city_pic_path:
            city_pic_path = '/'.join([param,city_pic_path])
            save_path.append(city_pic_path)
            temp_path = ''.join([base_path,city_pic_path])
            os.system("rsync -vI {0} 10.10.150.16::opcity/{1}".format(temp_path,param))
              
        share_airport_path = update_share_airport(temp_config,param)
        if share_airport_path:
            share_airport_path = list(share_airport_path)
            share_airport_path[0] = '/'.join([param,share_airport_path[0]])
            share_airport_path[1] = '/'.join([param,share_airport_path[1]]) 
            temp_path = ''.join([base_path,share_airport_path[0]])
            os.system("rsync -vI {0} 10.10.150.16::opcity/{1}".format(temp_path,param))
            temp_path = ''.join([base_path,share_airport_path[1]])
            os.system("rsync -vI {0} 10.10.150.16::opcity/{1}".format(temp_path,param))
            save_path.extend(share_airport_path)
         
        #shareAirport_insert(temp_config,param)
        #new_airport_insert(temp_config,param)
        if hotels_path:
            add_others_source_city(city_path,hotels_path,attr_path,ota_config,param)
        return_result = json.dumps(return_result)
        print('[result][{0}]'.format(return_result))
        csv_path = ';'.join(save_path)
        update_step_report(csv_path, param, 1,0)
        os.system('java -jar ks3up-tool-2.0.6-20170801/ks3up-2.0.6.jar -c city.conf start')
    except Exception as e:
        csv_path = ';'.join(save_path)
        return_result['error']['error_id'] = 1
        return_result['error']['error_str'] = traceback.format_exc()
        return_result = json.dumps(return_result)
        print('[result][{0}]'.format(return_result))
        update_step_report(csv_path, param, -1,0)


if __name__ == "__main__":
    # task_start()
    client = pymongo.MongoClient(host='10.10.231.105')
    collection = client['MongoTask']['Task_Queue_file_downloader_TaskName_google_driver_52_20180131']
    total_count = collection.find({})
    total_count.count()

