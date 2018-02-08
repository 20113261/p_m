#!/usr/local/bin/python3
# -*- coding:utf-8 -*-

from city.config import base_path,OpCity_config,config
from collections import defaultdict
import json
import traceback
import sys
import pymysql
pymysql.install_as_MySQLdb()
import csv
import pymongo
from city.config import config
from city.insert_daodao_city import daodao_city
from city.insert_hotel_city import hotel_city
from city.insert_qyer_city import qyer_city
from my_logger import get_logger
from call_city_project.step_status import modify_status

param = sys.argv[1]
path = ''.join([base_path, str(param), '/'])
logger = get_logger('step5', path)

def update_step_report(csv_path,param,step_front,step_after):
    conn = pymysql.connect(**OpCity_config)
    cursor = conn.cursor()
    update_sql_front = "update city_order set report5=%s,step5=%s where id=%s"
    update_sql_after = "update city_order set step6=%s where id=%s"
    try:
       cursor.execute(update_sql_front,(csv_path,step_front,param))
       cursor.execute(update_sql_after,(step_after,param))
       conn.commit()
    except Exception as e:
        conn.rollback()
    finally:
        conn.close()
#
# def monitor_daodao(collection):
#     result = collection.find({'finished': 0})
#     not_finish_num = result.count()
#     total_num = collection.find({})
#     return not_finish_num / total_num
# def monitor_qyer(collection):
#     result = collection.find({'finished': 0})
#     not_finish_num = result.count()
#     total_num = collection.find({})
#     return not_finish_num / total_num
# def monitor_hotel(collections):
#     save_result = []
#     for collection in collections:
#         result = collection.find({'finished': 0})
#         not_finish_num = result.count()
#         total_num = collection.find({})
#         save_result.append(not_finish_num / total_num)
#     return max(save_result)
#
# def monitor_daodao_qyer_hotel(daodao_collection_name,qyer_collection_name,hotel_collections_name,param):
#     client = pymongo.MongoClient(host='10.10.231.105')
#     daodao_collection = client['MongoTask'][daodao_collection_name]
#     qyer_collection = client['MongoTask'][qyer_collection_name]
#     hotel_collections = []
#     for collection in hotel_collections_name:
#         hotel_collections.append(client['MongoTask'][collection])
#
#     conn = pymysql.connect(**OpCity_config)
#     cursor = conn.cursor()
#     select_sql = "select step5 from city_order where id=%s"
#     cursor.execute(select_sql)
#     status_id = cursor.fetchone()[0]
#     if int(status_id) == 2:
#
#         daodao_not_finish = monitor_daodao(daodao_collection)
#         qyer_not_finish = monitor_qyer(qyer_collection)
#         hotel_not_finish = monitor_hotel(hotel_collections)
#
#     if not daodao_not_finish and not qyer_not_finish and not hotel_not_finish:
#         job = backgroudscheduler.get_job('step5')
#         job.remove()
#         update_step_report('', param, 1, 0)


def task_start():
    logger.info('[step5][%s]======== start =======' % (param,))
    try:
        sources = ['agoda', 'ctrip', 'elong', 'hotels', 'expedia', 'booking']
        return_result = defaultdict(dict)
        return_result['data'] = {}
        return_result['error']['error_id'] = 0
        return_result['error']['error_str'] = ''
        save_cityId = []
        database_name = ''.join(['add_city_', param])
        temp_config = config
        temp_config['db'] = database_name
        path = ''.join([base_path, param, '/', 'city_id.csv'])
        with open(path, 'r+') as city:
            reader = csv.DictReader(city)
            for row in reader:
                save_cityId.append(row['city_id'])
        logger.info('[step5][%s, %s, %s] 启动发 daodao 任务' % (save_cityId, param, temp_config))
        daodao_collection_name,daodao_task_name = daodao_city(save_cityId,param, temp_config)
        logger.info('[step5] 发 daodao 任务完成 [%s, %s]' % (daodao_collection_name, daodao_task_name))
        logger.info('[step5][%s, %s, %s] 启动发 qyer 任务' % (save_cityId, param, temp_config))
        qyer_collection_name,qyer_task_name = qyer_city(save_cityId,param, temp_config)
        logger.info('[step5] 发 qyer 任务完成 [%s, %s]' % (qyer_collection_name, qyer_task_name))
        logger.info('[step5][%s, %s, %s] 启动发 hotel 任务' % (save_cityId, param, temp_config))
        hotel_collections_name = hotel_city(save_cityId, param, sources, temp_config)
        logger.info('[step5] 发 hotel 任务完成 [%s]' % (hotel_collections_name))

        save_collection_names = []
        for collection_name in hotel_collections_name:
            save_collection_names.append(collection_name)
        save_collection_names.append((daodao_collection_name, daodao_task_name))
        save_collection_names.append((qyer_collection_name, qyer_task_name))

        tasks = modify_status('step5', param, save_collection_names)

        logger.info('[step5] 发 hotel 任务完成 [%s]' % (tasks))

        return_result = json.dumps(return_result)
        logger.info('[step5] [result][{0}]'.format(return_result))

    except Exception as e:
        return_result['error']['error_id'] = 1
        return_result['error']['error_str'] = traceback.format_exc()
        return_result = json.dumps(return_result)
        update_step_report('', param, -1, 0)
        logger.info('[step5] [result][{0}]'.format(return_result))



if __name__ == "__main__":
    task_start()