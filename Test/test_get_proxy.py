#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/27 下午5:33
# @Author  : Hou Rong
# @Site    : 
# @File    : test_get_proxy.py
# @Software: PyCharm
import json
import time
import requests
from my_logger import get_logger

logger = get_logger("get_proxy")


def get_proxy(source, ):
    time_st = time.time()
    logger.info("开始获取代理")

    msg = {
        "req": [
            {
                "source": source,
                "type": "platform",
                "num": 1,
                "ip_type": "",
            }
        ]
    }
    msg = json.dumps(msg)

    qid = time.time()
    ptid = "platform"
    try:
        get_info = '/?type=px001&qid={0}&query={1}&ptid={2}&tid=tid&ccy=AUD'.format(qid, msg, ptid)
        logger.info("get proxy info :http://10.10.189.85:48200{0}".format(get_info))
        p = requests.get("http://10.10.32.22:48200" + get_info).content
        time_end = time.time() - time_st
        logger.info("获取到代理，代理信息{0},获取代理耗时{1}".format(p, time_end))
        p = [json.loads(p)['resp'][0]['ips'][0]['inner_ip'], [p, time_end]]
    except Exception as e:
        p = ''
    return p


if __name__ == '__main__':
    print(get_proxy('platform'))
