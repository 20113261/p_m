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
from city.insert_daodao_city import daodao_city
from city.insert_hotel_city import hotel_city
from city.insert_qyer_city import qyer_city
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
backgroudscheduler = BackgroundScheduler()
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

def monitor_daodao(collection):
    result = collection.find({'finished': 0})
    not_finish_num = result.count()
    total_num = collection.find({})
    return not_finish_num / total_num
def monitor_qyer(collection):
    result = collection.find({'finished': 0})
    not_finish_num = result.count()
    total_num = collection.find({})
    return not_finish_num / total_num
def monitor_hotel(collections):
    save_result = []
    for collection in collections:
        result = collection.find({'finished': 0})
        not_finish_num = result.count()
        total_num = collection.find({})
        save_result.append(not_finish_num / total_num)
    return max(save_result)

def monitor_daodao_qyer_hotel(daodao_collection_name,qyer_collection_name,hotel_collections_name,param):
    client = pymongo.MongoClient(host='10.10.231.105')
    daodao_collection = client['MongoTask'][daodao_collection_name]
    qyer_collection = client['MongoTask'][qyer_collection_name]
    hotel_collections = []
    for collection in hotel_collections_name:
        hotel_collections.append(client['MongoTask'][collection])

    conn = pymysql.connect(**OpCity_config)
    cursor = conn.cursor()
    select_sql = "select step5 from city_order where id=%s"
    cursor.execute(select_sql)
    status_id = cursor.fetchone()[0]
    if int(status_id) == 2:

        daodao_not_finish = monitor_daodao(daodao_collection)
        qyer_not_finish = monitor_qyer(qyer_collection)
        hotel_not_finish = monitor_hotel(hotel_collections)

    if not daodao_not_finish and not qyer_not_finish and not hotel_not_finish:
        job = backgroudscheduler.get_job('step5')
        job.remove()
        update_step_report('', param, 1, 0)


def task_start():
    try:
        sources = ['agoda', 'ctrip', 'elong', 'hotels', 'expedia', 'booking']
        param = sys.argv[1]
        return_result = defaultdict(dict)
        return_result['data'] = {}
        return_result['error']['error_id'] = 0
        return_result['error']['error_str'] = ''
        save_cityId = []
        path = ''.join([base_path, param, '/', 'city_id.csv'])
        with open(path, 'r+') as city:
            reader = csv.DictReader(city)
            for row in reader:
                save_cityId.append(row['city_id'])

        daodao_collection_name = daodao_city(save_cityId,param)
        qyer_collection_name = qyer_city(save_cityId,param)
        hotel_collections_name = hotel_city(save_cityId,param,sources)

        kwargs = {
            'daodao_collection_name':daodao_collection_name,
            'qyer_collection_name':qyer_collection_name,
            'hotel_collections_name':hotel_collections_name,
            'param':param
        }
        job = backgroudscheduler.add_job(monitor_daodao_qyer_hotel, trigger='cron', minute='*/2', hour='*', id='step5',
                                         kwargs=kwargs)
        backgroudscheduler.start()
        return_result = json.dumps(return_result)
        print('[result][{0}]'.format(return_result))

    except Exception as e:

        return_result['error']['error_id'] = 1
        return_result['error']['error_str'] = traceback.format_exc()
        return_result = json.dumps(return_result)
        print('[result][{0}]'.format(return_result))
        update_step_report('', param, -1,0)



if __name__ == "__main__":
    task_start()