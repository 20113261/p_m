#coding:utf-8
import json
import pymysql
from city.config import data_config
from city.config import base_path
from my_logger import get_logger
import traceback

logger = get_logger('monitor', base_path)


def modify_status(step, key, values=[], flag=True):
    """

    :param step:
    :param key:
    :param value:
    :param flag: True添加任务
    :return:
    """
    conn = pymysql.connect(**data_config)
    cursor = conn.cursor()
    str_key = str(key)
    upd_sql = "replace into step_status (id, json_status) values (%s, %s)"
    tasks = getStepStatus(step)
    try:
        word = '空'
        if flag:
            tasks[key] = values
            word = '添加'
        else:
            del tasks[key]
            word = '删除'
        logger.info('{} {}, {}'.format(str_key, word, tasks))
        cursor.execute(upd_sql, (step, json.dumps(tasks)))
        conn.commit()
    except Exception as e:
        logger.info('{} 更新状态报错 {}'.format(str_key, str(traceback.format_exc())))
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
    logger.info('{} 更新完成'.format(key))
    return tasks

def getStepStatus(step):
    conn = pymysql.connect(**data_config)
    cursor = conn.cursor()
    sel_sql = "select json_status from step_status where id=%s"
    try:
        cursor.execute(sel_sql, (step,))

        for line in cursor.fetchone():
            tasks = line
        logger.info('{} 查询到状态 {}'.format(step, repr(tasks)))
    except TypeError as e:
        logger.info('{} 查询状态报错 {}'.format(step, str(traceback.format_exc())))
        tasks = '{}'
    finally:
        cursor.close()
        conn.close()
    return json.loads(tasks)

if __name__ == '__main__':
    modify_status('step4', 668, ['aaaaa', 'bbbbb'])