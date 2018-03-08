#!/usr/local/bin/python3
# -*- coding:utf-8 -*-

import pymysql
pymysql.install_as_MySQLdb()
from city.config import OpCity_config,base_path,config, data_config
from collections import defaultdict
import json
import traceback
import sys
import os
import subprocess
import time
from my_logger import get_logger
from city.send_email import send_email

param = sys.argv[1]
SEND_TO = ['luwanning@mioji.com', 'mazhenyang@mioji.com', 'chaisiyuan@mioji.com', 'dujun@mioji.com', 'zhaoxiaoyang@mioji.com', 'xuzhanlei@mioji.com']
# SEND_TO = ['luwanning@mioji.com', 'cuixiyi@mioji.com']
path = ''.join([base_path, str(param), '/'])
logger = get_logger('step6', path)
hotels = ['agoda', 'booking', 'ctrip', 'elong', 'expedia', 'hotels']

def update_step_report(csv_path,param,step_front,step_after):
    conn = pymysql.connect(**OpCity_config)
    cursor = conn.cursor()
    update_sql_front = "update city_order set report6=%s,step6=%s where id=%s"
    # update_sql_after = "update city_order set step7=%s where id=%s"
    try:
       cursor.execute(update_sql_front,(csv_path,step_front,param))
       # cursor.execute(update_sql_after,(step_after,param))
       conn.commit()
    except Exception as e:
        conn.rollback()
    finally:
        conn.close()

def selectServicePlatform2BaseDataFinal():
    sql = 'insert into BaseDataFinal.hotel_final_{0} select hotel_name, hotel_name_en, `source`, source_id, brand_name, map_info, address, city, country, city_id, postal_code, star, grade, review_num, has_wifi, is_wifi_free, has_parking, is_parking_free, service, img_items, description, accepted_cards, check_in_time, check_out_time, hotel_url, update_time, continent, country_id from ServicePlatform.detail_hotel_{1}_{0};'
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
        with open('/search/cuixiyi/PoiCommonScript/call_city_project/hotel.sql') as f:
            create_table = f.read()
            create_table = create_table.format(tag)
        cursor.execute(create_table, ())

        for hotel_source in hotels:
            _sql = sql.format(tag, hotel_source)
            cursor.execute(_sql, ())
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise Exception('汇总数据出错: \n{}'.format(traceback.format_exc(e)))
    finally:
        conn.close()

    return tag

def check_hotel_data(tag):
    cmd = 'python get_illegal_ori_parse_data.py {}'.format(tag)
    result = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True, cwd='/search/cuixiyi/PoiCommonScript/check_base_hotel', )
    for i in range(600):
        time.sleep(0.1)
        logger.info(result.stdout.read())
        if not result.wait():
            result_path = os.path.join('/search/cuixiyi/PoiCommonScript/check_base_hotel', time.strftime('%Y%m%d') + '.log')
            with open(result_path, 'r') as f:
                data = f.read()
                index = 0
                for i in range(5):
                    index = data.find('\n', index+1)
                    if i < 2:
                        data = data[:index+1] + '        ' + data[index+1:]
                if index>-1:
                    update_step_report('', param, -1, 0)
                    raise Exception('执行检查命令失败')
                else:
                    return data
            break

def dumps_sql(tag):
    cmd = """mysqldump -h10.10.228.253 -umioji_admin -pmioji1109 BaseDataFinal hotel_final_{0} > /data/hourong/output/hotel_final_{0}.sql"""
    cmd = cmd.format(tag)
    status = os.system(cmd)
    if status==0:
        return '10.10.114.35::output/hotel_final_{0}.sql'.format(tag)


def task_start():
    logger.info('[step6][%s]======== start =======' % (param,))
    try:
        return_result = defaultdict(dict)
        return_result['data'] = {}
        return_result['error']['error_id'] = 0
        return_result['error']['error_str'] = ''
        logger.info('[step6][%s] 汇总数据到BaseDataFinal 开始' % (param,))
        tag = selectServicePlatform2BaseDataFinal()
        logger.info('[step6][%s]  汇总数据到BaseDataFinal 完成' % (param,))
        logger.info('[step6][%s] 检查数据 开始' % (param,))
        check_result = check_hotel_data(tag)
        logger.info('[step6][%s] 检查数据 完成' % (param,))
        logger.info('[step6][%s] 导出数据 开始' % (param,))
        data_path = dumps_sql(tag)
        logger.info('[step6][%s] 导出数据 完成' % (param,))

        return_result = json.dumps(return_result)
        send_email('城市上线酒店融合' + '第 %s 批次' % param,
"""
大家好:
    脚本检查结果：
        {check_result}
    数据地址：
        {data_path}
    融合类型：增量融合
        """ .format(check_result=check_result[:-2], data_path=data_path), SEND_TO)
        # update_step_report('', param, 1, 0)
        logger.info('[step6][%s]======== success =======' % (param,))
    except Exception as e:
        return_result['error']['error_id'] = 1
        return_result['error']['error_str'] = traceback.format_exc()
        send_email('城市上线酒店融合' + '第 %s 批次' % param,
                   """酒店融合前检查失败""", SEND_TO[:1])
        return_result = json.dumps(return_result)
        update_step_report('', param, -1, 0)
        logger.info('[step6][%s]======== failed =======' % (return_result,))


if __name__ == "__main__":
    task_start()