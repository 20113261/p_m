#!/usr/local/bin/python3
# -*- coding:utf-8 -*-

import pymysql
pymysql.install_as_MySQLdb()
from city.config import OpCity_config,base_path,config
from collections import defaultdict
import json
import traceback
import sys
import csv
import os
from city.generate_urls import inner_city
from city.inter_city_task import city_inter_google_driver
import pymongo
from my_logger import get_logger
from call_city_project.step_status import modify_status

param = sys.argv[1]
path = ''.join([base_path, str(param), '/'])
logger = get_logger('step9', path)

urlsfile_path = '/search/cuixiyi/urls/'

def update_step_report(csv_path,param,step_front,step_after):
    conn = pymysql.connect(**OpCity_config)
    cursor = conn.cursor()
    update_sql_front = "update city_order set report9=%s,step9=%s where id=%s"
    # update_sql_after = "update city_order set step7=%s where id=%s"
    try:
       cursor.execute(update_sql_front,(csv_path,step_front,param))
       # cursor.execute(update_sql_after,(step_after,param))
       conn.commit()
    except Exception as e:
        conn.rollback()
    finally:
        conn.close()

def task_start():
    logger.info('[step9][%s]======== start =======' % (param,))
    try:
        return_result = defaultdict(dict)
        return_result['data'] = {}
        return_result['error']['error_id'] = 0
        return_result['error']['error_str'] = ''
        data_name = ''.join(['add_city_',str(param)])
        config['db'] = data_name
        save_cityId = []
        path = ''.join([base_path, param, '/', 'city_id.csv'])
        with open(path, 'r+') as city:
            reader = csv.DictReader(city)
            for row in reader:
                save_cityId.append(row['city_id'])

        #获取google url
        logger.info('[step9]获取google_url开始')
        inner_city(cid_list=save_cityId,config=config)
        logger.info('[step9]获取google_url完成')

        file_list = os.listdir(urlsfile_path)
        if '.DS_Store' in file_list:
            file_list.remove('.DS_Store')
        save_urls = []
        for city_id in save_cityId:
            for child_path in file_list:
                if city_id in child_path:
                    with open(urlsfile_path+'{0}'.format(child_path),'r+') as city:
                        urls = city.readlines()
                        save_urls.extend(urls)

        logger.info('[step9] 开启城市内任务')
        collection_name,task_name = city_inter_google_driver(save_urls,param)
        logger.info('[step9] 开启城市内任务完成  %s %s' % (collection_name, task_name))
        with open('/search/cuixiyi/PoiCommonScript/call_city_project/tasks.json', 'r+') as f:
            tasks = json.load(f)
            tasks[param] = [collection_name, task_name]
            f.seek(0)
            json.dump(tasks, f)
        tasks = modify_status('step5', param, [collection_name, task_name])

        return_result = json.dumps(return_result)
        logger.info('[step9][%s]======== success =======' % (param,))
    except Exception as e:
        return_result['error']['error_id'] = 1
        return_result['error']['error_str'] = traceback.format_exc()
        return_result = json.dumps(return_result)
        logger.info('[step9][%s]======== failed =======' % (return_result,))
        update_step_report('', param, -1,0)


if __name__ == "__main__":
    task_start()