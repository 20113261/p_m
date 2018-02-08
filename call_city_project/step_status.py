#coding:utf-8
import json
import pymysql
from city.config import data_config
from city.config import base_path
from my_logger import get_logger
import sys
import traceback


def modify_status(step, key, values=[], flag=True):
    """

    :param step:
    :param key:
    :param value:
    :param flag: True添加任务
    :return:
    """
    # base_path = '/Users/luwn/'
    path = ''.join([base_path, str(key), '/'])
    logger = get_logger('status', path)

    conn = pymysql.connect(**data_config)
    cursor = conn.cursor()
    upd_sql = "update step_status set json_status=%s where id=%s"
    tasks = getStepStatus(step)
    logger.info('--0==', tasks)
    try:
        if flag:
            tasks[key] = values
        else:
            del tasks[key]
        logger.info('--1==', tasks)
        cursor.execute(upd_sql, (json.dumps(tasks), step))
        conn.commit()
        logger.info('--2==提交')
    except Exception as e:
        conn.rollback()
        logger.info('--3==huigun')
    finally:
        logger.info('--4==关闭连接')
        cursor.close()
        conn.close()

    return tasks

def getStepStatus(step):
    path = '/search/service/nginx/html/MioaPyApi/store/opcity/668/'
    # path = '/Users/luwn/'
    logger = get_logger('status1', path)

    conn = pymysql.connect(**data_config)
    cursor = conn.cursor()
    logger.info('==-1--')
    tasks = {}
    sel_sql = "select json_status from step_status where id=%s"
    try:
        logger.info('==-4--')
        cursor.execute(sel_sql, (step,))
        logger.info('==51--')
        result = cursor.fetchone()
        logger.info('52---', type(result))
        if result is None:
            logger.info('==8--', tasks)
            return tasks
        for line in result:
            tasks = line[0]
        logger.info('==0--', tasks)
    except TypeError as e:
        logger.info('==1-- %s', str(traceback.format_exc()))
    finally:
        cursor.close()
        conn.close()
    logger.info('==8--', tasks)
    return tasks

if __name__ == '__main__':
    modify_status('step4', 668, ['aaaaa', 'bbbbb'])