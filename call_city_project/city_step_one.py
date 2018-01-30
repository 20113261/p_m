#!/usr/bin/env python
# -*- coding:utf-8 -*-

from city.config import config,base_path,OpCity_config
import os
import sys
import zipfile
import pymysql
pymysql.install_as_MySQLdb()
import json
import traceback
from collections import defaultdict
def task_start_one(param):
    try:
        return_result = defaultdict(dict)
        return_result['data'] = {}
        return_result['error']['error_id'] = 0
        return_result['error']['error_str'] = ''
        select_sql = "select path1 from city_order where id=%s"
        #param = sys.argv[1]
        conn = pymysql.connect(**OpCity_config)
        cursor = conn.cursor()
        cursor.execute(select_sql,(param,))
        path = cursor.fetchone()[0]
        file_name = path.split('/')[-1]
        zip_path = ''.join([base_path,'/',file_name])
        params = ''.join(['add_city_',str(param)])
        os.system("/search/cuixiyi/PoiCommonScript/call_city_project/add_new_city.sh {0} {1} {2}".format(zip_path,params,param))
        return_result = json.dumps(return_result)
        print('[result][{0}]'.format(return_result))
    except Exception as e:
        return_result['error']['error_id'] = 1
        return_result['error']['error_str'] = traceback.format_exc()
        return_result = json.dumps(return_result)
        print('[result][{0}]'.format(return_result))
if __name__ == "__main__":
    task_start_one(param)
