#! /usr/bin/env python
# -*- coding: utf-8 -*-
import grpc
import data_pb2, data_pb2_grpc

_HOST = '10.10.94.53'
_PORT = '8765'


def run(country,city,map_info=None):
    if map_info:
        text = country + '&' + city + '&' + map_info
    else:
        text = country + '&' + city
    conn = grpc.insecure_channel(_HOST + ':' + _PORT)
    client = data_pb2_grpc.FormatDataStub(channel=conn)
    response = client.DoFormat(data_pb2.Data(text=text))
    print("received: " + response.text)


if __name__ == '__main__':
    run('Jordan', 'Wadi Musa','35.4832992553711,30.3166999816895')
    run('意大利', '塔兰托(省)')
    run('约旦', '安曼 (及邻近地区)')
    run('日本', '大阪县')  # "2.35147638246417,48.8566821749061")[0].cid)
    run('中国', '达县县')
    for map in ["115.041500091553,-8.45612507720947", "115.210892,-8.273642", "115.222,-8.656", "115.22156,-8.65667","115.111241,-8.380058", "115.188916,-8.409518"]:
        run('印度尼西亚', '巴厘岛', map)
    run('中国', '马祖')
    run('美国', 'Wilmington','-77.9447102,34.2257255')
    run('Jordan', 'Wadi Musa')
