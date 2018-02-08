#coding:utf-8
import json

def modify_status(step, key, values=[], flag=True):
    """

    :param step:
    :param key:
    :param value:
    :param flag: True添加任务
    :return:
    """
    with open('/search/cuixiyi/PoiCommonScript/call_city_project/tasks.json', 'r') as f:
        tasks = json.load(f)
        if not tasks.haskey(step):
            return {}
        step_tasks = tasks[step]
        if flag:
            step_tasks[key] = values
        else:
            step_tasks.pop(key)
    with open('/search/cuixiyi/PoiCommonScript/call_city_project/tasks.json', 'w+') as f:
        json.dump(tasks, f)

    return tasks[step]

def getStepStatus(step):
    with open('/search/cuixiyi/PoiCommonScript/call_city_project/tasks.json', 'r') as f:
        tasks = json.load(f)
        tasks.setdefault(step, {})
        return tasks[step]