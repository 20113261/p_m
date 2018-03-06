#!/usr/local/bin/python3
# -*- coding:utf-8 -*-

from city.field_check import new_airport_insert,add_others_source_city
from city.add_city import read_file
from city.share_airport import update_share_airport
from city.city_map_ciytName import revise_pictureName
from city.update_city_pic import update_city_pic
from city.db_insert import shareAirport_insert
from city.config import config,base_path,OpCity_config,ota_config,test_config
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
from my_logger import get_logger
from city.find_hotel_opi_city import add_city_suggest
from MongoTask.crawl_all_source_suggest import create_task
from call_city_project.step_status import modify_status
from city.share_airport import from_file_get_share_airport
from city.db_insert import from_file_airport_insert
import csv
from city.find_hotel_opi_city import from_ota_get_city
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
    logger = get_logger('step3',path)
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
    judge_city_id = 1
    try:
        path = ''.join([base_path, str(param), '/'])
        return_result = defaultdict(dict)
        return_result['data'] = {}
        return_result['error']['error_id'] = 0
        return_result['error']['error_str'] = ''
        conn = pymysql.connect(**test_config)
        cursor = conn.cursor()
        logger.debug("新增城市入库执行开始")
        city_insert_path = read_file(city_path,temp_config,param)
        if city_insert_path:
            select_sql = "select * from city where id=%s"
            with open(path+'city_id.csv','r+') as city:
                reader = csv.DictReader(city)
                for row in reader:
                    city_id = row['city_id']
                    cursor.execute(select_sql,(city_id,))
                    if cursor.fetchall():
                        judge_city_id = 0
                        break
            save_path.append(city_insert_path[1])
            temp_path = ''.join([base_path, city_insert_path[1]])
            os.system("rsync -vI {0} 10.10.150.16::opcity/{1}".format(temp_path, param))
        logger.debug("[新增城市入库执行完毕]")
        logger.debug("[新增城市图片名称更新开始]")
        if judge_city_id:
            city_map_path = revise_pictureName(picture_path,temp_config,param)
            logger.debug("[新增城市的图片名称更新完毕]")
        logger.debug("城市更新后的图片名称更新到city表响应的new_product_pic字段-开始")
        city_pic_path = update_city_pic(picture_path,temp_config,param)
        if city_pic_path and judge_city_id:
            city_pic_path = '/'.join([param,city_pic_path])
            save_path.append(city_pic_path)
            temp_path = ''.join([base_path,city_pic_path])
            os.system("rsync -vI {0} 10.10.150.16::opcity/{1}".format(temp_path,param))
        logger.debug("城市更新后的图片名称更新到city表响应的new_product_pic字段-结束")
        logger.debug("新增机场入库开始执行")
        if judge_city_id:
            new_airport_insert(temp_config, param)
        logger.debug("新增机场入库执行完毕")
        logger.debug("为城市提供共享机场开始执行")
        share_airport_path = []
        share_airport_to_data_path = []
        if not airport_path:

            share_airport_path = update_share_airport(temp_config,param)
        elif airport_path:
            share_airport_path = from_file_get_share_airport(param)
            citys = share_airport_path[2]
            if citys:
                need_share_airport_path = update_share_airport(temp_config,param,citys)
            else:
                need_share_airport_path = []
            share_airport_path = list(share_airport_path)[:2]
            share_airport_to_data_path = share_airport_path
            logger.debug("share_airport_to_data_path:",share_airport_to_data_path)
            if need_share_airport_path:
                share_airport_path = share_airport_path.extend(need_share_airport_path)
                logger.debug("share_airport_path:",share_airport_path)
        if share_airport_path and judge_city_id:
            for airport_file_path in share_airport_path:
                airport_file_path = '/'.join([param,airport_file_path])
                temp_path = ''.join([base_path,airport_file_path])
                os.system("rsync -vI {0} 10.10.150.16::opcity/{1}".format(temp_path,param))
                save_path.append(temp_path)
        logger.debug("城市共享机场执行完毕")
        logger.debug("城市共享机场入库开始")
        if judge_city_id and share_airport_to_data_path:
            count = from_file_airport_insert(temp_config,param,share_airport_to_data_path)

        logger.debug("城市共享机场入库结束,机场入库总数：{0}".format(count))
        logger.debug("将新增城市更新到ota_location的各个源-开始")
        if hotels_path and judge_city_id:
            add_others_source_city(city_path,hotels_path,attr_path,config,param)
        logger.debug("将新增城市更新到ota_location的各个源-结束")
        return_result = json.dumps(return_result)
        print('[result][{0}]'.format(return_result))
        csv_path = ';'.join(save_path)
        update_step_report(csv_path, param, 1,0)
        logger.debug("上传图片开始")
        if judge_city_id:
            os.system('java -jar /search/cuixiyi/ks3up-tool-2.0.6-20170801/ks3up-2.0.6.jar -c /search/cuixiyi/ks3up-tool-2.0.6-20170801/city.conf start')
        logger.debug("上传图片结束")
        # logger.debug("开始更新ota_location表")
        # collection_name,task_name = create_task(city_path,path,database_name,param)
        # tasks = modify_status('step3',param,[collection_name,task_name])
        # hotel_file_name,poi_file_name = from_ota_get_city(temp_config,param)
        # temp_path = ''.join([base_path,hotel_file_name,])
        # os.system("rsync -vI {0} 10.10.150.16::opcity/{1}".format(temp_path, param))
        # temp_path = ''.join([base_path,poi_file_name])
        # os.system("rsync -vI {0} 10.10.150.16::opcity/{1}".format(temp_path, param))
        # logger.debug("结束更新ota_location表")

    except Exception as e:
        csv_path = ';'.join(save_path)
        return_result['error']['error_id'] = 1
        return_result['error']['error_str'] = traceback.format_exc()
        return_result = json.dumps(return_result)
        print('[result][{0}]'.format(return_result))
        update_step_report(csv_path, param, -1,0)
        logger.debug('[result][{0}]'.format(return_result))


if __name__ == "__main__":
    task_start()
    #client = pymongo.MongoClient(host='10.10.231.105')
    #collection = client['MongoTask']['Task_Queue_file_downloader_TaskName_google_driver_52_20180131']
    #total_count = collection.find({})
    #total_count.count()

