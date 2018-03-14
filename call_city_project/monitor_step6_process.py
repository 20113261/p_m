#! /usr/bin/env python
# -*- coding:utf-8 -*-

import sys
import os
from call_city_project.step_status import getStepStatus
from city.config import base_step_six_path,OpCity_config
from apscheduler.schedulers.blocking import BlockingScheduler
import pymysql
pymysql.install_as_MySQLdb()
scheduler = BlockingScheduler()
def update_step_report(csv_path,param,step_front,step_after):
    conn = pymysql.connect(**OpCity_config)
    cursor = conn.cursor()
    update_sql_front = "update city_order set report6=%s,step3=%s where id=%s"
    try:
       cursor.execute(update_sql_front,(csv_path,step_front,param))
       conn.commit()
    except Exception as e:
        conn.rollback()
    finally:
        conn.close()

def monitor_step_six():
    tasks = getStepStatus('step6')
    tasks_list = list(tasks.items())
    if len(tasks_list) == 0:
        return
    param = tasks.keys()[0]
    os.system("rsync -avI 10.10.150.16::opcity/{0} {1}".format(param,base_step_six_path))
    step_six_path = ''.join([base_step_six_path,str(param)])
    file_list = os.listdir(step_six_path)
    for file_name in file_list:
        if 'hotel_report' in file_name:
            save_path = '/'.join([str(param),file_name])
            update_step_report(save_path,param,1)

if __name__ == "__main__":
    pass