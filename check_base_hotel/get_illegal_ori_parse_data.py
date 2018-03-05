#encoding=utf-8

import sys
# import MySQLdb
import pymysql as MySQLdb
MySQLdb.install_as_MySQLdb()
import traceback
import random
import re
import string
import math
import datetime
import time
from multiprocessing import Pool

sys.path.append('./common/')
from Base import get_city_map
from cal_sim import getDistByMap
from Base import getHotelSource
from cal_sim import is_chinese
from cal_sim import is_contain_ch
from cal_sim import is_full_contain_ch
import sys

import config

tag = sys.argv[1]
filter_dist = 50000
white_city_list = ["50033","50061","50066","50067","50846"]
punctuation="！？。“＃¥%＆’（）*+，-／：；《=》@【、】……——·「|」～｟｠｢｣〃『』〔〕〖〗〘〙〜〝〞”“〟〰〾〿–—‘’‛“”„‟…‧﹏.".decode("utf-8")

#dev_ip = "10.10.171.224"
dev_ip = config.dev_db_ip
dev_user = 'reader'
dev_passwd = 'miaoji1109'
dev_db = config.dev_db_name

dev_conn = MySQLdb.connect(host=dev_ip, user=dev_user, charset='utf8',passwd = dev_passwd,db = dev_db)
dev_cursor = dev_conn.cursor()

#ori_db_name = 'ori_hotel_data_from_parser'
#ori_table_list = ['hotelinfo_ama_add','hotelinfo_canada_add','hotelinfo_google',\
#'hotelinfo_hotelbeds','hotelinfo_norway_add','hotelinfo_spain_add','hotelinfo_swit_add']
ori_db_name = config.ori_db_name
ori_table_list = config.unmerged_table_list


def loadCid2CountryInfo():
	sql = 'select id,country_id from city'
	cursor = dev_conn.cursor()
	cursor.execute(sql)
	datas = cursor.fetchall()
	cid2countryId = {}
	for data in datas:
		if data[0] == None or data[1] == None:
			continue
		cid = str(data[0])
		countryId = str(data[1])
		cid2countryId[cid] = countryId
	return cid2countryId

def printErrorToFile(total_count,error_count,success_count,haveCase,fw):
	fw.write('total:' + str(total_count) + '\n')
	if total_count <= 0:
		return 

	fw.write('success\t' + str(success_count) + '\t' +  str(success_count * 1.0/ total_count) + '\n')

	if haveCase == False:
		for cand_error in error_count:
			fw.write(cand_error + '\t' +  str(len(error_count[cand_error])) + '\t' + str(len(error_count[cand_error]) * 1.0 / total_count) + '\n')
	else:
		for error_type in error_count:
			lenth = len(error_count[error_type])
			if lenth > 10:
				error_list = random.sample(error_count[error_type],10)
			else:
				error_list = error_count[error_type]
			fw.write(error_type + '\t' + str(lenth) + '\t随机抽取前十个：' + '\n')
			for error_info in error_list:
				fw.write('\t'+error_info + '\n')

	fw.write('\n')


def read_mysql_dispose(cand_table, hotel_source, city_map, cid2countryId, start_idx, unit):
	sql = "select hotel_name,hotel_name_en,source,source_id,city_id,map_info,grade from %s limit %d, %d" %(cand_table, start_idx, unit) 
	conn = MySQLdb.connect(host=config.ori_db_ip, user=config.ori_db_user,charset='utf8',passwd=config.ori_db_pwd,db=config.ori_db_name)
	cursor = conn.cursor()
	cursor.execute(sql)
	datas = cursor.fetchall()

	total = 0
	success = 0
	error_sourceid_list = []
	error_count={}

	for data in datas:
		total += 1
		word_list = []

		for word in data:
			if None == word:
				word_list.append('')
			else:
				word_list.append(word.decode('utf-8'))

		name = word_list[0]
		name_en = word_list[1]
		source = word_list[2]
		sid = word_list[3]
		cid = word_list[4]
		map_info = word_list[5]
		grade = word_list[6]
		#key=source+','+sid+','+cid
		key = ('source="%s" and source_id="%s" and city_id="%s"' % (source, sid, cid))
		if cid in cid2countryId and cid2countryId[cid] == "121" and ('apa hotel' in name_en.lower() or 'apa villa hotel' in name_en.lower()):
			error_sourceid_list.append(source+'\t'+sid+'\t'+cid) #'source="%s" and source_id="%s" and city_id="%s"' % (source, sid, cid)
			error_type = '日本apa酒店'
			if error_type not in error_count:
				error_count[error_type]=[]
			error_count[error_type].append('key:'+key)
			continue

		if  source not in hotel_source:
			error_sourceid_list.append(source+'\t'+sid+'\t'+cid)
			error_type = '未知的源'
			if error_type not in error_count:
				error_count[error_type]=[]
			error_count[error_type].append('source:'+source+'\tkey:'+key)
			continue

		if ('' != name and "NULL" != name and re.sub(ur"[%s]+" %(punctuation+string.punctuation),"",name.decode("utf-8"))=='') \
				or ('' != name_en and "NULL" != name_en and re.sub(ur"[%s]+" %(punctuation+string.punctuation),"",name_en.decode("utf-8"))==''):
			error_sourceid_list.append(source+'\t'+sid+'\t'+cid)
			error_type = '酒店中文或英文名字全部由标点符号组成'
			if error_type not in error_count:
				error_count[error_type]=[]
			error_count[error_type].append('name:'+name+'\tname_en:'+name_en+'\tkey:'+key)
			continue

		if ('' == name and '' == name_en) or ("NULL" == name and "NULL" == name_en):
			error_sourceid_list.append(source+'\t'+sid+'\t'+cid)
			error_type = '酒店中英文名都为空'
			if error_type not in error_count:
				error_count[error_type]=[]
			error_count[error_type].append('name:'+name+'\tname_en:'+name_en+'\tkey:'+key)
			continue
		if '' != name and '' != name_en and is_contain_ch(name_en):
			if is_full_contain_ch(name_en) and is_contain_ch(name) == False:
				error_sourceid_list.append(source+'\t'+sid+'\t'+cid)
				error_type = "酒店中英文名字相反"
				if error_type not in error_count:
					error_count[error_type]=[]
				error_count[error_type].append('name:'+name+'\tname_en:'+name_en+'\tkey:'+key)
				continue
		if name.strip().lower() != name_en.strip().lower() and \
				is_contain_ch(name) and is_contain_ch(name_en) == False and len(name_en.split(' ')) >= 2 \
				and name_en in name:
			error_sourceid_list.append(source+'\t'+sid+'\t'+cid)
			error_type = "中文名中含有英文名"
			if error_type not in error_count:
				error_count[error_type]=[]
			error_count[error_type].append('name:'+name+'\tname_en:'+name_en+'\tkey:'+key)
			continue

		#if cid not in city_map:
		#	error_sourceid_list.append(source+'\t'+sid+'\t'+cid)
		#	error_type = 'unknown_cid'
		#	if error_type not in error_count:
			#	error_count[error_type]=[]
		#	error_count[error_type].append('cid:'+cid+'\tkey'+key)
		#	continue

		#city_map_info = city_map[cid]

		try:
			lgt = float(map_info.split(',')[0])
			lat = float(map_info.split(',')[1])

		except Exception,e:
			error_sourceid_list.append(source+'\t'+sid+'\t'+cid)
			error_type = 'map_info_error'
			if error_type not in error_count:
				error_count[error_type]=[]
			error_count[error_type].append('map_info:'+map_info+'\tkey:'+key)
			continue

		if lgt == 0 and lat == 0:
			error_sourceid_list.append(source+'\t'+sid+'\t'+cid)
			error_type = '经纬度坐标为0'
			if error_type not in error_count:
				error_count[error_type]=[]
			error_count[error_type].append('map_info:'+map_info+'\tkey:'+key)
			continue

		#cand_dist = getDistByMap(city_map_info,map_info) 

		#if None == cand_dist:
		#	error_sourceid_list.append(source+'\t'+sid+'\t'+cid)
		#	error_type = "dist_None"
		#	if error_type not in error_count:
		#		error_count[error_type]=[]
		#	error_count[error_type].append('map_info:'+map_info+'\tkey:'+key)
		#	continue

		#if cand_dist >= filter_dist and cid not in white_city_list:
		#	error_sourceid_list.append(source+'\t'+sid+'\t'+cid)
		#	error_type = 'dist_filter'
		#	if error_type not in error_count:
		#		error_count[error_type]=[]
		#	error_count[error_type].append('can_dist:'+cand_dist+'\tkey'+key)
		#	continue

		try:
			grade_f = float(grade)
			if grade_f > 10:
				error_sourceid_list.append(source+'\t'+sid+'\t'+cid)
				error_type = '酒店静态评分异常(评分高于10分)'
				if error_type not in error_count:
					error_count[error_type]=[]
				error_count[error_type].append('grade:'+grade+'\tkey:'+key)
				continue
		except:
			pass

		success += 1

	return error_sourceid_list,total,success,error_count

def get_error_sourceid_list():
	#读取城市和国家的对应关系
	cid2countryId = loadCid2CountryInfo()
	print "cid2countryId.size=" + str(len(cid2countryId))

	conn = MySQLdb.connect(host=config.ori_db_ip, user=config.ori_db_user,charset='utf8',passwd=config.ori_db_pwd,db=config.ori_db_name)
	cursor = conn.cursor()
	city_map = get_city_map()

	hotel_source = getHotelSource()

	error_sourceid_list = []
	
	#ori_table_list.append('hotel_static_data_from_parse')
	date = datetime.datetime.now().strftime('%Y%m%d')
	fw_result = open(date +'.log','w')

	for cand_table in ori_table_list:
		start = time.time()
		print 'Begin ' + cand_table
	
		sql = "select count(*) from " + cand_table
		print sql
		cursor.execute(sql)
		dataCnt = cursor.fetchall()[0][0]

		max_thread_count = 4  #进程数，可根据机器情况进行调整
		unit = 500000
		#unit = 100

		start_idx = 0
		pool = Pool(max_thread_count)
		res_list = []
		while start_idx < dataCnt:
			res = pool.apply_async(func=read_mysql_dispose,args=(cand_table,hotel_source,city_map,cid2countryId,start_idx,unit))
			res_list.append(res)
			start_idx += unit
		pool.close()
		pool.join()

		error_count = {}
		total = 0
		success = 0
		for res in res_list:
			results=res.get()
			print results
			error_sourceid_list.extend(results[0])
			total+=results[1]
			success+=results[2]

			#for key in results[3]:
			#	print key, len(results[3][key])
			#print "*"*50
			
			for error_type in results[3]:
				if error_type not in error_count:
					error_count[error_type] = []
				error_count[error_type].extend(results[3][error_type])
	
		fw_result.write(cand_table + '\n')
		printErrorToFile(total,error_count,success,False,fw_result)

		end = time.time()
		print "耗时:" + str(end - start)

	return error_sourceid_list

if __name__ == '__main__':
	config.ori_table_name =	config.ori_table_name.format(tag)
	config.unmerged_table_list[0] =	config.unmerged_table_list[0].format(tag)
	error_sourceid_list = get_error_sourceid_list()
	for error in error_sourceid_list:
		print error