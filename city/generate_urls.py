#!/usr/bin/python
#coding=utf-8
import os
import sys
import re
import MySQLdb
import multiprocessing
import time
table_list=["chat_attraction","hotel","airport","station","chat_shopping"]
#table_list=["chat_attraction","station"]

file_path = '/search/cuixiyi'

def formatBaseQuery(url_fp,base_query,city_type,nameA,pointA,nameB,pointB):

	if ',' not in pointA or ',' not in pointB:
		return '-1'
	point_buf1=pointA.split(",")
	pointa=point_buf1[1]+","+point_buf1[0]
	
	point_buf2=pointB.split(",")
	pointb=point_buf2[1]+","+point_buf2[0]

	base_query={}
	base_query["origin"]=pointa
	base_query["destination"]=pointb
	base_query["region"]="es"
	base_query["model"]=""
	base_query["a1"]=nameA
	base_query["a2"]=nameB
	base_query["type"]=city_type
	for mode in ['transit','driving','walking']:
		base_url="http://maps.google.cn/maps/api/directions/json?origin=A&destination=B&region=es&mode=C&a1=D&a2=E&type=F"
		orgin=base_query["origin"]
		destination=base_query["destination"]
		a1=base_query["a1"]
		a2=base_query["a2"]
		ty=base_query["type"]
		last_url=base_url.replace("origin=A","origin="+orgin).replace("destination=B","destination="+destination).replace("mode=C","mode="+mode).replace("a1=D","a1="+a1).replace("a2=E","a2="+a2).replace("type=F","type="+ty)

		url_fp.write(last_url+"\n")
def Process(cid,config):
	filename=str(cid)+"_urls"
	logname=str(cid)+"_logs"
	fp=open(file_path+"/urls/"+filename,"w")
	fs=open(file_path+"/logs/"+logname,"w")
	fs.write("Process enter cid:"+str(cid)+"\n")
    
	all_data=[]

	#每个进程连接mysql
    #默认为chat_attraction的sql
	conn=MySQLdb.connect(host="10.10.69.170",user="reader",passwd="miaoji1109",db="base_data",charset="utf8")
	cursor=conn.cursor()
	for table in table_list:
		sql='select id,map_info from %s where city_id='
		sql=sql.replace("%s",table)
		if table =="hotel":
			sql=sql.replace("id","uid",1)
			sql=sql.replace("city_id","city_mid")
		elif table == "airport":
			sql=sql.replace("id","iata_code",1)
		elif table =="station" or table == "bus":
			sql=sql.replace("id","station_id",1)
        
		sql1=sql+'"%s";' %(str(cid))
		cursor.execute(sql1)
		datas=cursor.fetchall()
		list_buf=[]
		for row in datas:

			list_buf.append(str(row[0])+'\t'+str(row[1]))
		all_data.append(list_buf)
	fs.write(str(cid)+"  the Process all_data get success,size:"+str(len(all_data))+"\n")
    
	forbidList=[2,]
	specialList= [100000000,]
	for i in range(len(table_list)):
		for j in range(len(table_list)):
			try:
				v=i**3+j**3
				if i<j:
					continue
                
				if v in forbidList:
					continue
				if 100000000 not in specialList:
					if v not in specialList:#需要拼接的组合
						continue
				if i!=j:
					for itemA in all_data[i]:
						for itemB in all_data[j]:
							if itemA == itemB:
								continue
							if len(itemA.split('\t'))!=2 or len(itemB.split('\t'))!= 2:
								continue
							task_map={}
                            
							formatBaseQuery(fp,task_map,"innerCity",itemA.split('\t')[0],itemA.split('\t')[1],itemB.split('\t')[0],itemB.split('\t')[1])
				else:
					for data_i in range(len(all_data[i])):
						for data_j in range(len(all_data[j])):
							if data_i < data_j:
								itemA=all_data[i][data_i]
								itemB=all_data[j][data_j]

								if itemA==itemB:
									continue
								if len(itemA.split('\t')) != 2 or len(itemB.split('\t')) != 2:
									continue
								task_map={}
								formatBaseQuery(fp,task_map,"innerCity",itemA.split('\t')[0],itemA.split('\t')[1],itemB.split('\t')[0],itemB.split('\t')[1])
			except Exception as e:
				continue
	fp.close()
	fs.write("Process:%s end" %(str(cid)))
	fs.close()

def inner_city(cid_list,config):
	start_time=time.time()
	pool=multiprocessing.Pool(8)
	for city in cid_list:
		pool.apply_async(Process,(city,config))
	time.sleep(1)
	pool.close()
	pool.join()
	end_time=time.time()
	print(start_time, end_time)

if __name__ == "__main__":
    #获取参数，参数是存储城市id的文件,可以有多个文件
    cid_list=[]
    if len(sys.argv) == 1:
        print("Usage: python generate_urls.py filenames")
        sys.exit()
    for i in range(1,len(sys.argv)):
        try:
            fp=open(sys.argv[i])
        except IOError:
            print("can't find the file %s") %(sys.argv[i])
            continue
        for line in fp:
            line=line.strip()
            cid=re.search("^\d+$",line).group()
            if cid is None:
                continue
            #print cid
            cid_list.append(str(cid))
        fp.close()
    #cid去重
    cid_list=list(set(cid_list))
    #需要读取的sql表
    start_time=time.time()
    pool=multiprocessing.Pool(8)
    for city in cid_list:
        pool.apply_async(Process,(city,))
    time.sleep(1)
    pool.close()
    pool.join()
    end_time=time.time()
    print("Pool use time:%d s") %(int(end_time-start_time))


