#!/usr/local/bin/python3
# -*- coding:utf-8 -*-

import pymysql
pymysql.install_as_MySQLdb()
from city.config import OpCity_config,base_path,config, data_config, ota_config
from collections import defaultdict
from service_platform_report.crawl_data_check_script import detectOriData
import json
import traceback
import sys
from my_logger import get_logger
from city.send_email import send_email
from MongoTask.MongoTaskInsert import InsertTask, TaskType

# param = sys.argv[1]
param = '671'
SEND_TO = ['luwanning@mioji.com', 'lidongwei@mioji.com', 'chaisiyuan@mioji.com', 'dujun@mioji.com', 'zhaoxiaoyang@mioji.com']
path = ''.join([base_path, str(param), '/'])
logger = get_logger('step7', path)

def update_step_report(csv_path,param,step_front,step_after):
    conn = pymysql.connect(**OpCity_config)
    cursor = conn.cursor()
    update_sql_front = "update city_order set report7=%s,step7=%s where id=%s"
    update_sql_after = "update city_order set step8=%s where id=%s"
    try:
       cursor.execute(update_sql_front,(csv_path,step_front,param))
       cursor.execute(update_sql_after,(step_after,param))
       conn.commit()
    except Exception as e:
        conn.rollback()
    finally:
        conn.close()

def selectServicePlatform2BaseDataFinal():
    daodao_attr_sql = 'insert into BaseDataFinal.attr_final_{0} select * from ServicePlatform.detail_attr_daodao_{0};'
    qyer_sql = 'insert into BaseDataFinal.total_final_{0} select * from ServicePlatform.detail_total_qyer_{0};'
    select_tag = "select * from Report.service_platform_product_mongo_report where tag like '%%{}'".format(param)
    conn = pymysql.connect(**data_config)
    cursor = conn.cursor()
    try:
        cursor.execute(select_tag, ())
        result = cursor.fetchone()
        if result:
            tag = result[0]
        else:
            raise Exception('没有获取到tag')
        with open('/search/cuixiyi/PoiCommonScript/call_city_project/daodao_attr.sql') as f:
            create_table = f.read()
            create_table = create_table.format(tag)
        cursor.execute(create_table, ())
        with open('/search/cuixiyi/PoiCommonScript/call_city_project/qyer.sql') as f:
            create_table = f.read()
            create_table = create_table.format(tag)
        cursor.execute(create_table, ())

        _daodao_attr_sql = daodao_attr_sql.format(tag)
        cursor.execute(_daodao_attr_sql, ())
        _qyer_sql = qyer_sql.format(tag)
        cursor.execute(_qyer_sql, ())
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise Exception('汇总数据出错: \n{}'.format(traceback.format_exc(e)))
    finally:
        conn.close()

    return tag


def mapping_daodao_by_sid_and_sourcecityid(tag):
    sql = "select sid, city_id from source_info.ota_location where source = 'daodao' and city_id <> 'NULL';"
    conn_ota = pymysql.connect(**ota_config)
    cursor_ota = conn_ota.cursor()
    cursor_ota.execute(sql, ())
    sid_map_city = {sid: city_id for sid, city_id in cursor_ota.fetchall()}
    cursor_ota.close()
    conn_ota.close()

    update_sql = "update BaseDataFinal.attr_final_{0} set city_id=%s where source_city_id=%s".format(tag)
    select_daodao_sql = "select distinct source_city_id from BaseDataFinal.attr_final_{};".format(tag)
    conn_final = pymysql.connect(**data_config)
    cursor_final = conn_final.cursor()
    cursor_final.execute(select_daodao_sql, ())
    for line in cursor_final.fetchall():
        source_city_id = line[0]
        city_id = sid_map_city.get('g'+source_city_id, None)
        if city_id:
            print(source_city_id)
            cursor_final.execute(update_sql, (city_id, source_city_id))
    conn_final.commit()
    cursor_final.close()
    conn_final.close()

def mapping_daodao_by_othersinfoscityid_and_sourcecityid(tag):
    sql = "select city_id, JSON_EXTRACT(others_info, \"$.s_city_id\") as sid from source_info.ota_location where source = 'qyer' and JSON_EXTRACT(others_info, \"$.s_city_id\")<>'' and city_id<>'NULL';"
    conn_ota = pymysql.connect(**ota_config)
    cursor_ota = conn_ota.cursor()
    cursor_ota.execute(sql, ())
    sid_map_city = {sid[1:-1]: city_id for city_id, sid in cursor_ota.fetchall()}
    cursor_ota.close()
    conn_ota.close()

    update_sql = "update BaseDataFinal.total_final_{0} set city_id=%s where source_city_id=%s".format(tag)
    select_daodao_sql = "select distinct source_city_id from BaseDataFinal.total_final_{};".format(tag)
    conn_final = pymysql.connect(**data_config)
    cursor_final = conn_final.cursor()
    cursor_final.execute(select_daodao_sql, ())
    for line in cursor_final.fetchall():
        source_city_id = line[0]
        city_id = sid_map_city.get(source_city_id, None)
        if city_id:
            print(source_city_id)
            cursor_final.execute(update_sql, (city_id, source_city_id))
    conn_final.commit()
    cursor_final.close()
    conn_final.close()

def check_POI_data(tag):
    qyer_table_name = 'total_final_{}'.format(tag)
    daodao_table_name = 'attr_final_{}'.format(tag)
    qyer_report_result, qyer_tasks_data = detectOriData(qyer_table_name)
    daodao_report_result, daodao_tasks_data = detectOriData(daodao_table_name)
    print(qyer_report_result)
    print(daodao_report_result)
    return qyer_tasks_data, daodao_tasks_data

def send_tasks(tasks_data, tag):
    source = tasks_data[0]['source']
    task_name = source + '_mapinfo_' + tag
    with InsertTask(worker='proj.total_tasks.supplement_map_info', queue='supplement_field', routine_key='supplement_field',
                    task_name=task_name, source=source.capitalize(), _type='CityInfo',
                    priority=3) as it:
        for line in tasks_data:
            args = line
            it.insert_task(args)

        return it.generate_collection_name(), task_name

def task_start():
    logger.info('[step7][%s]======== start =======' % (param,))
    try:
        return_result = defaultdict(dict)
        return_result['data'] = {}
        return_result['error']['error_id'] = 0
        return_result['error']['error_str'] = ''
        return_result = json.dumps(return_result)

        logger.info('[step6][%s] 汇总数据到BaseDataFinal 开始' % (param,))
        tag = selectServicePlatform2BaseDataFinal()
        logger.info('[step6][%s]  汇总数据到BaseDataFinal 完成' % (param,))
        logger.info('[step6][%s] mapping daodao 开始' % (param,))
        mapping_daodao_by_sid_and_sourcecityid(tag)
        logger.info('[step6][%s] mapping daodao 开始' % (param,))
        logger.info('[step6][%s] mapping qyer 开始' % (param,))
        mapping_daodao_by_othersinfoscityid_and_sourcecityid(tag)
        logger.info('[step6][%s] mapping qyer 开始' % (param,))
        logger.info('[step6][%s] 检查数据 开始' % (param,))
        qyer_tasks_data, daodao_tasks_data = check_POI_data(tag)
        logger.info('[step6][%s] 检查数据 完成' % (param,))
        if qyer_tasks_data:
            logger.info('[step6][%s] qyer补充mapinfo任务 开始' % (param,))
            send_tasks(qyer_tasks_data, tag)
            logger.info('[step6][%s] qyer补充mapinfo任务 完成' % (param,))
        if daodao_tasks_data:
            logger.info('[step6][%s] daodao补充mapinfo任务 完成' % (param,))
            send_tasks(daodao_tasks_data, tag)
        logger.info('[step6][%s] daodao补充mapinfo任务 完成' % (param,))

        # logger.info('[step6][%s] 导出数据 开始' % (param,))
        # data_path = dumps_sql(tag)
        # logger.info('[step6][%s] 导出数据 完成' % (param,))




        send_email('城市上线POI融合' + '第%s批次' % param, """
        第{}批次 共{}数据已处理完毕，检验合格
        """.format(param, '100万'), SEND_TO)
        update_step_report('', param, 1, 0)
        logger.info('[step7][%s]======== success =======' % (param,))
    except Exception as e:
        return_result['error']['error_id'] = 1
        return_result['error']['error_str'] = traceback.format_exc()
        return_result = json.dumps(return_result)
        send_email('城市上线酒店融合' + '第 %s 批次' % param,
                   """POI融合前检查失败""", SEND_TO[:1])
        update_step_report('', param, -1, 0)
        logger.info('[step7][%s]======== failed =======' % (return_result,))


if __name__ == "__main__":
    task_start()