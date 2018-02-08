#coding:utf-8
import json
import pymysql
from city.config import data_config

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
    upd_sql = "update step_status set json_status=%s where id=%s"
    tasks = getStepStatus(step)
    try:
        if flag:
            tasks[key] = values
        else:
            del tasks[key]

        cursor.execute(upd_sql, (tasks, step))
    except Exception as e:
        conn.rollback()
    finally:
        conn.close()

    # with open('/search/cuixiyi/PoiCommonScript/call_city_project/tasks.json', 'r') as f:
    #     tasks = json.load(f)
    #     if not tasks.haskey(step):
    #         return {}
    #     step_tasks = tasks[step]
    #     if flag:
    #         step_tasks[key] = values
    #     else:
    #         del step_tasks[key]
    # with open('/search/cuixiyi/PoiCommonScript/call_city_project/tasks.json', 'w+') as f:
    #     json.dump(tasks, f)

    return tasks[step]

def getStepStatus(step):
    conn = pymysql.connect(**data_config)
    cursor = conn.cursor()
    sel_sql = "select json_status from step_status where id=%s"
    try:
        cursor.execute(sel_sql, (step,))
        for line in cursor.fetchone():
            tasks = line[0]
        return tasks
    except TypeError as e:
        return {}
    finally:
        conn.close()
    # with open('/search/cuixiyi/PoiCommonScript/call_city_project/tasks.json', 'r') as f:
    #     tasks = json.load(f)
    #     tasks.setdefault(step, {})
    #     return tasks[step]