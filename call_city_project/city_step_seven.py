#!/usr/local/bin/python3
# -*- coding:utf-8 -*-

import pymysql
pymysql.install_as_MySQLdb()
from city.config import OpCity_config,base_path,config
from collections import defaultdict
import json
import traceback
import sys
from my_logger import get_logger
from city.send_email import send_email, SEND_TO

param = sys.argv[1]
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

def task_start():
    logger.info('[step7][%s]======== start =======' % (param,))
    try:
        return_result = defaultdict(dict)
        return_result['data'] = {}
        return_result['error']['error_id'] = 0
        return_result['error']['error_str'] = ''
        return_result = json.dumps(return_result)
        send_email('城市上线POI融合' + '第%s批次' % param, """
        第{}批次 共{}数据已处理完毕，检验合格
        """.format(param, '100万'), SEND_TO)
        update_step_report('', param, 1, 0)
        logger.info('[step7][%s]======== success =======' % (param,))
    except Exception as e:
        return_result['error']['error_id'] = 1
        return_result['error']['error_str'] = traceback.format_exc()
        return_result = json.dumps(return_result)
        update_step_report('', param, -1, 0)
        logger.info('[step7][%s]======== failed =======' % (return_result,))


if __name__ == "__main__":
    task_start()