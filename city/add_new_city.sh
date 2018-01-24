#!/bin/bash

echo "Hello world";

#将zip 文件解压到指定文件夹
#echo "第一个参数：$1";
unzip -d /data/city/ $1

#执行python脚本,并传入工单参数

#echo "第二个参数：$2";
python3 city_control_flow.py $2
