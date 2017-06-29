#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/6/29 上午10:29
# @Author  : Hou Rong
# @Site    : 
# @File    : grail_api.py
# @Software: PyCharm
import requests
import datetime
import hashlib
import json
import time

API_KEY = '09856f85e1544b64af18fe34b38be0f5'
SECURE = '343928bd-2164-454f-8198-b5d3d2b702c5'


class GrailApi(object):
    def __init__(self):
        self.url_prefix = 'https://alpha.api.detie.cn/api/v1/'

    @staticmethod
    def get_http_header(params):
        now_time = datetime.datetime.now()
        unix_timestamp = int(now_time.timestamp())
        date_str = now_time.strftime('%a, %d %b %Y %H:%M:%S +0800')
        auth_map = {
            'api_key': API_KEY,
            't': unix_timestamp
        }
        auth_map.update(params)

        auth_str = ''.join(
            map(lambda x: '{0}={1}'.format(x[0], x[1]), sorted(auth_map.items(), key=lambda x: x[0]))) + SECURE
        return {
            "From": API_KEY,
            "Date": date_str,
            "Authorization": hashlib.md5(auth_str.encode()).hexdigest(),
            "content_type": "application/json"
        }

    def req(self, method, url_suffix, headers, **kwargs):
        print(self.url_prefix + url_suffix)
        page = requests.request(method=method, headers=headers,
                                url=self.url_prefix + url_suffix,
                                **kwargs)
        return page.text


if __name__ == '__main__':
    # params_list = [('ST_EZVOJP9W', 'ST_D8NNN9ZK'), ('ST_L239VZKQ', 'ST_E5KKQR00'), ('ST_EZVVG1X5', 'ST_EZVOJP9W'),
    #                ('ST_D8NNN9ZK', 'ST_EZVVG1X5'), ('ST_ENZZ722P', 'ST_ENZZ7QVN'), ('ST_EZVOJP9W', 'ST_D9444YR4')]
    params_list = [('ST_L239VZKQ', 'ST_EG62437J'), ('ST_E5KKQR00', 'ST_L4GG28Z1')]
    GA = GrailApi()

    for p in params_list:
        req_params = {
            "s": p[0],
            "d": p[1],
            "dt": "2017-07-23 06:00",
            "na": 1,
            "nc": 0
        }
        headers = GA.get_http_header(req_params)
        async_key = json.loads(GA.req('get', 'online_solutions', headers=headers, params=req_params))['async']
        print(async_key)

        j_data = {'description': 'Async result not ready.'}

        try:
            while j_data['description'] == 'Async result not ready.':
                headers = GA.get_http_header({'async_key': async_key})
                j_str = GA.req('get', 'async_results/{0}'.format(async_key), headers=headers)
                j_data = json.loads(j_str)
                time.sleep(1)
        except:
            print('DOWNLOAD OK!')
            print(j_str, file=open('/tmp/Grail/{0}_{1}'.format(p[0], p[1]), 'w'))
