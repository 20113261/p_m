#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/6/23 上午11:24
# @Author  : Hou Rong
# @Site    : 
# @File    : verify_case.py
# @Software: PyCharm
"""
Created on 2017年5月2日

@author: dujun
"""
import functools
import requests, datetime, json

# import file_dao

QUERY_API = 'http://oa.mioji.com/opui/api/public/logreq'
YHD_F = '%Y%m%d'

case_headers = ['qid', 'type', 'source', 'content', 'spider_host', 'code', '爬虫抓取数', '匹配数', '入库数', 'base_match_filter',
                'info', 'task_info']


def verify_case_csv(qid, env='test', filter_fun=None):
    res = query_verify_case(qid, env=env)
    node = json.loads(res)
    rows = []
    for line in node['data']:
        if not line:
            continue
        if 'type=b103' in line and 'query=' in line:
            query_str = line[line.index('query=') + 6:]
        elif 'response' in line:
            query_debug_data = json.loads(line)['response']['data']
            for item_case in query_debug_data.get('hotel', []):
                rows += parse_hotel_case(qid, item_case)

            for item_case in query_debug_data.get('traffic', []):
                rows += parse_traffic_case(qid, item_case)

    if filter_fun:
        tmp_rows = []
        for r in rows:
            if filter_fun(r):
                tmp_rows.append(r)
        rows = tmp_rows
    return rows


def parse_hotel_case(qid, item_case):
    vs = []
    ticket_info = item_case.get('task_info', {})
    ticket_info = json.dumps(ticket_info, ensure_ascii=False)
    for k, v in item_case['debug'].items():
        key_array = k.split('|')
        if len(key_array) > 5:
            vs.append([qid, key_array[0], v.get('source'), v.get('task_content'), key_array[6]]
                      + parse_case_callback(v)
                      + [ticket_info, ])
    return vs


def parse_traffic_case(qid, item_case):
    vs = []
    ticket_info = item_case.get('task_info', {})
    ticket_info = json.dumps(ticket_info, ensure_ascii=False)
    for k, v in item_case['debug'].items():
        key_array = k.split('|')
        if len(key_array) > 5:
            vs.append([qid, key_array[0], v.get('source'), v.get('task_content'), key_array[4]]
                      + parse_case_callback(v)
                      + [ticket_info, ])
    return vs


def parse_case_callback(item_case):
    error = item_case.get('err_code', -1)
    if error == -1:
        error = item_case.get('callback_err', -1)
    return [error, item_case.get('ori_cnt', -1), item_case.get('final_count', -1), item_case.get('storage_count', -1),
            item_case.get('base_match_filter', -1), json.dumps(item_case)]


def query_verify_case(qid, env='test'):
    res = query(qid, env, req_type='b103')
    return res


def query(qid, env, req_type=None):
    query_params = {'qid': qid}
    if req_type:
        query_params['req_type'] = req_type

    day_date = datetime.datetime.utcfromtimestamp(qid / 1000)

    params = {'tname': 'nginx_api_log_' + day_date.strftime(YHD_F),
              'env': env,
              'log_source': 14,
              'condition': json.dumps(query_params)}
    print(params)
    resp = requests.post(url=QUERY_API, data=params)
    return resp.text


def final_count_filter(row):
    if row[7] <= 0:
        return True
    else:
        return False


def source_filter(row, allow=None):
    if not allow:
        return True
    elif row[2] in allow and row[7] <= 0:
        return True
    else:
        return False


if __name__ == '__main__':
    # print(query(1498119073186, 'test'))
    allows = ['ctripHotel']
    s_filter = functools.partial(source_filter, allow=allows)

    #     qids = '''1498144017656
    # 1498144017656
    # 1498144017656
    # 1498127746520
    # 1498126273740
    # 1498123778493
    # 1498123207822
    # 1498119344658
    # 1498119073186
    # 1498116666698
    # 1498116830615
    # 1498115994091
    # 1498116057497
    # 1498116057497
    # 1498116057497
    # 1498116057497
    # 1498116057497
    # 1498116057497
    # 1498113489248
    # 1498111073074
    # 1498109279314
    # 1498105892696
    # 1498104433782
    # 1498103809357
    # 1498103809357
    # 1498103461849
    # 1498103461849
    # 1498101371619
    # 1498101371619
    # 1498101066491
    # 1498097884459
    # 1498098027920
    # 1498098027920
    # 1498098027920
    # 1498098031031
    # 1498097732599
    # 1498097578692
    # 1498097281627
    # 1498062166228
    # 1498061859712
    # 1498061404897'''

    qids = '''1498057733145
1498057733145
1498057733145
1498057242674
1498057242674
1498057242674
1498057242674
1498054396438
1498054165485
1498038175432
1498037591640
1498037212523
1498037212523
1498036495484
1498034561925
1498034561925
1498034561925
1498034561925
1498034561925
1498032642497
1498031884035
1498031508535
1498031508535
1498030945148
1498030945148
1498030602109
1498030602109
1498030602109
1498028284869
1498028284869
1498027724211
1498027724211
1498026436478
1498026436478
1498026436478
1498026436478
1498026024009
1498026024009
1498022563719
1498022563719
1498013402722
1498013402722
1498013563859
1498013563859
1498012671147
1498012671147
1498012671147
1498012671147
1498012671147
1498011069673
1498011069673
1498011069673
1498011069673'''

    #     qids = '''1497962112257
    # 1497962112257
    # 1497961575888
    # 1497961575888
    # 1497961575888
    # 1497954884488
    # 1497954592519
    # 1497953074840
    # 1497951136703
    # 1497951136703
    # 1497951136703
    # 1497948675562
    # 1497948378640
    # 1497948031086
    # 1497947802361
    # 1497947802361
    # 1497943322154
    # 1497942671359
    # 1497942671359
    # 1497942671359
    # 1497930979657
    # 1497930979657
    # 1497930573441
    # 1497929902219
    # 1497929902219
    # 1497930070920
    # 1497928971778
    # 1497928971778
    # 1497928971778
    # 1497928679985
    # 1497928679985
    # 1497928679985
    # 1497925714964
    # 1497925714964
    # 1497925714964
    # 1497925667139
    # 1497925339902
    # 1497925339902
    # 1497925339902'''

    # qids = '1498144017656'
    qids = '''1497437585047
1497437585047
1497431219148
1497423668151
1497423507658
1497409780376
1497410023895
1497409397261
1497409138078
1497408544321
1497408813168
1497408217408
1497407908165
1497407643453
1497407378081
1497406711848
1497407007867
1497406197131
1497405611083
1497405777862
1497405777862
1497405881216
1497405302344
1497405011093
1497404737986
1497404396073
1497404090339
1497403826801
1497403232927
1497403496675
1497402891195
1497402615335
1497402262856
1497401966514
1497401437782
1497401666386
1497401106318
1497400766342
1497400501905
1497400196364
1497399877627
1497399551004
1497399282667
1497398996039
1497398703015
1497398401409
1497398098369
1497397791335
1497397260274
1497397552268
1497396926768
1497396581181
1497396322249
1497396009645
1497395411564
1497393495515
1497391789051
1497390041293
1497388116196
1497386330773
1497384650984
1497382736581
1497381085583
1497377425930
1497375455576
1497374205880
1497373820256
1497373494633
1497373185430
1497372595070
1497372865280
1497372276629
1497371971542
1497371205750
1497371467741
1497371740632
1497370887260
1497370466863
1497370225458
1497369875824'''

    qids = '''1496491101841
1496491101841
1496462254443
1496461894222
1496458112054
1496457391634
1496455230413
1496454510136
1496454149952
1496453969841
1496453789732
1496453609619'''

    qids = '''1497349140970
1497336647317
1497335246820
1497333819168
1497333819168
1497331657987
1497331838096
1497331477877
1497331297768
1497331117659
1497330397227
1497330577336
1497330757447
1497330841826
1497330937556
1497330217115
1497330037006
1497329316558
1497328956334
1497328596127
1497328416017
1497327155287
1497326975209
1497326795148
1497326254814
1497326434924
1497326615034
1497326074704
1497325534429
1497325174213
1497317429927
1497317069806
1497317039398
1497316889694
1497316709583
1497316529474
1497316349363
1497316169254
1497315989141
1497315809027'''
    res = []

    for qid in qids.split():
        res.extend(verify_case_csv(qid=int(qid), env='online', filter_fun=s_filter))

    print(res)
