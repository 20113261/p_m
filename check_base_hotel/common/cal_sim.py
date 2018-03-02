#encoding=utf-8
import sys
import traceback
import MySQLdb
import re
import math
import time,datetime
import json
import redis
import hashlib
from LOG import _ERROR, _INFO

stop_word_list = ['hotel', 'hotels', 'and', 'by', ' ', '\t', '\r', '\n','apartments', 'apartment', '\'', \
		'!','of','in','@','#','$','%','&','(',')','_','-','+','=',',','.','?','/','{','}',':',':','"',\
		'！','aparthotel','appartements','with','（','）','room','——','：','；','，','。','？','“','”','‘','’','、','the']

reload(sys)
sys.setdefaultencoding('utf-8')
def get_md5_value(src):
	myMd5 = hashlib.md5()
	myMd5.update(src)
	myMd5_Digest = myMd5.hexdigest()
	return myMd5_Digest

def is_chinese(uchar):
	if uchar >= u'\u4e00' and uchar<=u'\u9fa5':
		return True
	else:
		return False

def is_contain_ch(ustr):
	for ch in ustr:
		if is_chinese(ch):
			return True
	
	return False

def is_full_contain_ch(ustr) :
	for ch in ustr :
		if not is_chinese(ch) :
			return False
	if ustr == u"" :
		return False
	return True

#split string to char
def str2dict(o_str):
	str = o_str.lower().replace('\xc2\xa0', ' ').replace('_', ' ')
	for sw in stop_word_list:
		str = str.replace(sw, ' ')
	
	l = list(str.decode('utf-8'))
	
	d = {}
	s = set([])

	for i in xrange(0, len(l)):
		ch = l[i].encode('utf-8')
		if ch not in s:
			d[ch] = 0
			s.add(ch)
		d[ch] += 1
	return d


def enstr2dict(o_en_str):
	en_str = o_en_str.lower()

	for sw in stop_word_list:
		if ' ' == sw:
			continue
		
		en_str = en_str.replace(sw,' ')
	''' 
	word_list = en_str.strip().split()
	#print json.dumps(word_list, ensure_ascii=False)
	word_dic = {}
	for word in word_list:
		if '' == word:
			continue
		
		if word not in word_dic:
			word_dic[word] = 0

		word_dic[word] += 1
	
	#print str(word_dic)
	''' 
	word_dic = chstr2dict(en_str)
	return word_dic

ch_stop_word = [u'酒店',u'旅馆',u'旅店',u'_',u' ',u'公寓', u'-', u'|', u',', u'(',u')',u'（',u'）',u'&',u'公寓式酒店']
def chstr2dict(src_u_str):
	for s_word in ch_stop_word:
		src_u_str = src_u_str.replace(s_word,u' ')
	
	word_count = {}

	begin_idx = 0
	while begin_idx < len(src_u_str):
		if src_u_str[begin_idx] == ' ' or '\t' == src_u_str[begin_idx]:
			begin_idx += 1
			continue
		word = src_u_str[begin_idx]
		if is_chinese(word):
			if word not in word_count:
				word_count[word] = 0

			word_count[word] += 1
			begin_idx += 1

		elif word.isdigit():
			wordStr = ''
			flag = True
			while(flag):
				if not word.isdigit():
					break
				wordStr += word
				try:
					begin_idx += 1
					word = src_u_str[begin_idx]
				except:
					flag = False
			if wordStr not in word_count:
				word_count[wordStr] = 0
			word_count[wordStr] += 10

		elif word.isalpha():
			wordStr = ''
			flag = True
			while(flag):
				word = word.lower()
				if word == ' ' or not word.isalpha():
					break
				wordStr += word
				try:
					begin_idx += 1
					word = src_u_str[begin_idx]
				except:
					flag = False
			if wordStr not in word_count:
				word_count[wordStr] = 0
			word_count[wordStr] += 1
		elif word.isalpha():
			end_idx = begin_idx + 1
			if end_idx >= len(src_u_str):
				break

			while end_idx < len(src_u_str):
				tmp_word = src_u_str[end_idx]

				if tmp_word == ' ' or is_chinese(tmp_word):
					break
				else:
					end_idx += 1
					continue

			cand_word = src_u_str[begin_idx:end_idx]
			if cand_word not in word_count:
				word_count[cand_word] = 0

			word_count[cand_word] += 1
			
			begin_idx = end_idx
		else:
			begin_idx += 1
				
	return word_count


# 夹角余弦
def cos(d1, d2):
	ret = 0.0000001
	len1 = 0.0000001
	len2 = 0.0000001
	for key in d1.keys():
		len1 += d1[key] * d1[key]
		if key in d2.keys():
			ret += d1[key] * d2[key]
	
	for key in d2.keys():
		len2 += d2[key] * d2[key]
	return ret / pow(len1 * len2, 0.5)


# distance
EARTH_RADIUS = 6378137
PI = 3.1415927

def rad(d):
	return d * PI / 180.0

def getDist(lng1, lat1, lng2, lat2):
	radLat1 = rad(lat1)
	radLat2 = rad(lat2)
	a = radLat1 - radLat2
	b = rad(lng1) - rad(lng2)

	s = 2 * math.asin(math.sqrt(math.pow(math.sin(a/2), 2) + math.cos(radLat1) * math.cos(radLat2) * math.pow(math.sin(b/2),2)))

	s = s * EARTH_RADIUS
	s = round(s * 10000) / 10000

	return int(s)


def getDistSimply(lng1, lat1, lng2, lat2) :
	dx = lng1 - lng2
	dy = lat1- lat2
	b = (lat1 + lat2) / 2.0
	lx = rad(dx) * EARTH_RADIUS * math.cos(rad(b))
	ly = EARTH_RADIUS * rad(dy)
	return int(math.sqrt(lx * lx + ly * ly))

def get_dist_by_map(map_1,map_2):
	try:
		return getDistSimply(float(map_1.split(',')[0]),float(map_1.split(',')[1]),
				float(map_2.split(',')[0]),float(map_2.split(',')[1]))
	
	except Exception,e:
		_INFO('get_dist',['map = ' + map_1 + '\t' + map_2])
		return 100000000000

def cal_sim(source,source_id,cand_cid,map_info,hotel_name_en,hotel_name_ch,\
		sourceidSet,hotel_name_en_dic,hotel_name_ch_dic,map_info_dic,debug_flag = False):
	count = 0
	dist = 0
	esim1 = 0.0
	esim2 = 0.0
	esim3 = 0.0
	repeat_flag = False
	same_flag = False
	#是否存在名字完全一致
	has_same_name_flag =False

	dist_set_count = 0
	sim_1_set_count = 0
	sim_2_set_count = 0
	sim_3_set_count = 0
	
	for cand_hotel in sourceidSet: #cand_hotel is source + '\t' + sid + '\t' + cid
		if cand_hotel == source + '\t' + source_id + '\t' + cand_cid:
			same_flag = True
			#continue
			break

		if debug_flag:
			print 'cand_hotel:' + cand_hotel

		if source == cand_hotel.split('\t')[0]:
			repeat_flag = True
			break

		if cand_hotel not in hotel_name_en_dic:
			#print 'Error, no name! ' + cand_hotel
			continue

		cand_hotel_name_en = hotel_name_en_dic[cand_hotel]

		if cand_hotel not in hotel_name_ch_dic:
			#print 'Error, no ch name'
			continue

		cand_hotel_name_ch = hotel_name_ch_dic[cand_hotel]

		if cand_hotel not in map_info_dic:
			#print 'Error, no map_info! ' + cand_hotel
			continue
		
		cand_map_info = map_info_dic[cand_hotel]
		
		try:
			new_dist = getDistSimply(float(map_info.split(',')[0]),float(map_info.split(',')[1]),\
					float(cand_map_info.split(',')[0]),float(cand_map_info.split(',')[1]))
			if new_dist > 500 :
				return new_dist, -1, -1, -1, False, False,has_same_name_flag
			dist_set_count += 1
		except Exception,e:
			print 'cal dist fail! ' +  map_info + ' ' + cand_map_info + '\t' + cand_hotel
			#sys.exit(1)
			continue
		
		if new_dist <= 100:
			if (hotel_name_ch.strip() != '' and hotel_name_ch.lower() != 'null' \
					and hotel_name_ch == cand_hotel_name_ch)\
					or (hotel_name_en.strip() != '' and hotel_name_en.lower() != 'null' \
					and hotel_name_en == cand_hotel_name_en):
						has_same_name_flag = True

		try:
			"""
			#en char
			new_esim1 = 0.0
			if not is_contain_ch(cand_hotel_name_en.decode('utf-8')) \
					and not is_contain_ch(hotel_name_en.decode('utf-8')) \
					and 'null' != cand_hotel_name_en.lower() and 'null' != hotel_name_en.lower()\
					and '' != cand_hotel_name_en and '' != hotel_name_en:
						new_esim1 = cos(str2dict(cand_hotel_name_en),str2dict(hotel_name_en))
						sim_1_set_count += 1
			"""
			#en name
			new_esim2 = 0.0
			if not is_contain_ch(cand_hotel_name_en.decode('utf-8')) \
					and not is_contain_ch(hotel_name_en.decode('utf-8')) \
					and 'null' != cand_hotel_name_en.lower() and 'null' != hotel_name_en.lower() \
					and '' != cand_hotel_name_en.strip() and '' != hotel_name_en.strip():
						new_esim2 = cos(enstr2dict(cand_hotel_name_en),enstr2dict(hotel_name_en))
						sim_2_set_count += 1
			
			#ch name
			new_esim3 = 0.0
			if is_contain_ch(cand_hotel_name_ch.decode('utf-8')) \
					and is_contain_ch(hotel_name_ch.decode('utf-8')):
						new_esim3 = cos(chstr2dict(cand_hotel_name_ch.decode('utf-8')),chstr2dict(hotel_name_ch.decode('utf-8')))
						sim_3_set_count += 1
			
			if debug_flag:
				print cand_hotel + '\t' + str(new_dist) + '\t' + str(new_esim1) + '\t' + str(new_esim2) + '\t' + str(new_esim3)

			dist += new_dist
			#esim1 += new_esim1
			esim2 += new_esim2
			esim3 += new_esim3
		
		except Exception ,e:
			print 'error occ'
			print 'error code = ' + str(e)
			sys.exit(1)


	if 0 == dist_set_count:
		return 1000000,-1,-1,-1,repeat_flag,False,has_same_name_flag
	
	if dist_set_count > 0:
		dist = dist / dist_set_count
	else:
		dist = 1000000

	if sim_1_set_count > 0:
		esim1 = esim1 / sim_1_set_count
	else:
		esim1 = -1.0

	if sim_2_set_count > 0:
		esim2 = esim2 / sim_2_set_count
	else:
		esim2 = -1.0
	
	if sim_3_set_count > 0:
		esim3 = esim3 / sim_3_set_count
	else:
		esim3 = -1.0
	
	if debug_flag:
		print dist,esim1,esim2,esim3,repeat_flag

	return dist,esim1,esim2,esim3,repeat_flag,same_flag,has_same_name_flag

#esim1	en char
#esim2 en name word
#esim3 ch name word
def isSameHotel(dist,esim1,esim2,esim3,repeat_flag,star, starSet, name, name_en, num):
	'''
	print "%"*100
	print dist,esim1,esim2,esim3,repeat_flag,star, starSet, name, name_en
	print "%"*100
	'''
	if repeat_flag or dist >= 1000 or (esim2 <= 0 and esim3 <= 0):
		return False
		
	if star in starSet or (star.strip() == "-1.0"):
		if ((esim2 > 0.8*num or esim2 < 0) and (esim3 > 0.8*num or esim3 < 0) and dist < 100) \
				or (dist < 30 \
				and (( (esim2 > 0 or esim3 > 0) and  esim2 > 0.8*num or esim3 > 0.8*num ) \
				or (esim2 > 0.85*num and esim3 > 0.75*num) or (esim2 > 0.75*num and esim3 > 0.85*num))):
			return True
	
		'''
		if dist <= 5 and (esim2 > 0.5 or esim3 > 0.65):
			return True

		if dist <= 10 and (esim2 >= 0.6 or esim3 >= 0.6):
			return True

		if dist <= 20 and (esim2 >= 0.65 or esim3 >= 0.65):
			return True
		'''
	
		if dist <= 25 and (esim3 >= 0.7*num or esim2 > 0.7*num):
			return True

		if dist <= 50 and (esim2 >= 0.85*num or esim3 >= 0.85*num):
			return True

		if (dist <= 120 and (esim2 >= 0.85*num or esim3 >= 0.85*num)) \
				or (dist <= 140 and (esim2 >= 0.9*num or esim3 >= 0.9*num)):
					return True
		'''
		if dist <= 1000:
			en_count = len(enstr2dict(name_en.decode("utf-8")))
			ch_count = len(chstr2dict(name.decode("utf-8")))
			if (en_count <= 3 and esim2 >= 0.95*num) or \
				(3 < en_count and en_count <= 5 and esim2 >= 0.9*num) or \
				(5 < en_count and en_count <= 8 and esim2 >= 0.85*num) or \
				(8 < en_count and en_count <= 10 and esim2 >= 0.8*num) or \
				(10 < en_count and esim2 >= 0.75*num) or \
				(ch_count <= 3 and esim3 >= 0.95*num) or \
				(3 < ch_count and ch_count <= 5 and esim3 >= 0.9*num) or \
				(5 < ch_count and ch_count <= 8 and esim3 >= 0.85*num) or \
				(8 < ch_count and ch_count <= 10 and esim3 >= 0.8*num) or \
				(10 < ch_count and esim3 >= 0.8*num):
				return True
		'''
		return False
	else :
		if dist <= 10 and (esim2 > 0.7*num or esim3 > 0.7*num):
			return True
		if dist <= 20 and (esim2 > 0.8*num or esim3 > 0.8*num):
			return True
		if dist <= 50 and (esim2 > 0.88*num or esim3 > 0.88*num):
			return True
		if dist <= 70 and (esim2 > 0.9*num or esim3 > 0.9*num):
			return True
		if dist <= 100 and (esim2 > 0.95*num or esim3 > 0.95*num):
			return True
		#if dist <= 500 and (esim2 >= 0.95):
		#	return True
		'''
		if dist <= 1000:
			en_count = len(enstr2dict(name_en.decode("utf-8")))
			ch_count = len(chstr2dict(name.decode("utf-8")))
			if (en_count <= 3 and esim2 >= 0.95*num) or \
				(3 < en_count and en_count <= 5 and esim2 >= 0.9*num) or \
				(5 < en_count and en_count <= 8 and esim2 >= 0.85*num) or \
				(8 < en_count and en_count <= 10 and esim2 >= 0.8*num) or \
				(10 < en_count and esim2 >= 0.75*num) or \
				(ch_count <= 3 and esim3 >= 0.95*num) or \
				(3 < ch_count and ch_count <= 5 and esim3 >= 0.9*num) or \
				(5 < ch_count and ch_count <= 8 and esim3 >= 0.85*num) or \
				(8 < ch_count and ch_count <= 10 and esim3 >= 0.8*num) or \
				(10 < ch_count and esim3 >= 0.8*num):
				return True
			if (en_count <= 1 and esim2 >= 0.95) or \
				(1 < en_count and en_count <= 3 and esim2 >= 0.9) or \
				(3 < en_count and en_count <= 5 and esim2 >= 0.85) or \
				(5 < en_count and en_count <= 7 and esim2 >= 0.8) or \
				(7 < en_count and esim2 >= 0.75) or \
				(ch_count <= 1 and esim3 >= 0.95) or \
				(1 < ch_count and ch_count <= 3 and esim3 >= 0.9) or \
				(3 < ch_count and ch_count <= 5 and esim3 >= 0.85) or \
				(5 < ch_count and ch_count <= 7 and esim3 >= 0.8) or \
				(7 < ch_count and esim3 >= 0.8):
		'''
		return False


def getDistByMap(map_info_1,map_info_2):
	try:
		lgt1 = float(map_info_1.split(',')[0])
		lat1 = float(map_info_1.split(',')[1])

		lgt2 = float(map_info_2.split(',')[0])
		lat2 = float(map_info_2.split(',')[1])
		return getDistSimply(lgt1,lat1,lgt2,lat2)

	except Exception,e:
		return None

def cal_sim_new(mgetRedisData,source,source_id,cand_cid,map_info,hotel_name_en,hotel_name_ch,sourceidSet,hotel_name_en_dic,hotel_name_ch_dic,map_info_dic,index_flag,cand_star,starSet,debug_flag = False):
	count = 0
	dist = 0
	esim1 = 0.0
	esim2 = 0.0
	esim3 = 0.0
	repeat_flag = False
	same_flag = False
	#是否存在名字完全一致
	has_same_name_flag =False
	
	max_esim1 = 0.0
	min_esim1 = 0.0
	max_esim2 = 0.0
	min_esim2 = 0.0
	max_esim3 = 0.0
	min_esim3 = 0.0
	max_dist = 0
	min_dist = 10000

	dist_set_count = 0
	sim_1_set_count = 0
	sim_2_set_count = 0
	sim_3_set_count = 0
	#print source,source_id,cand_cid,map_info,hotel_name_en,hotel_name_ch,sourceidSet,hotel_name_en_dic,hotel_name_ch_dic,map_info_dic,index_flag,cand_star,starSet
	for cand_hotel in sourceidSet: #cand_hotel is source + '\t' + sid + '\t' + cid
		if cand_hotel == source + '\t' + source_id + '\t' + cand_cid:
			same_flag = True
			break

		enSim = chSim = chAndEnSim = isSame = 0
		distance = 10000
		hotelUnionKey = source + '|||' + source_id + '|||' + cand_cid
		redis_key = "sim__" + "__".join(sorted([cand_hotel, hotelUnionKey])) 
		enSim, chSim, chAndEnSim, distance, isSame = mgetRedisData[redis_key] if mgetRedisData.has_key(redis_key) else [0.0,0.0,0.0,10000,0]
		if not mgetRedisData.has_key(redis_key):
			print "redis_key:",redis_key
		if debug_flag:
			print 'cand_hotel:' + cand_hotel

		#相同源数据不融合，下线
		#if source == cand_hotel.split('\t')[0]:
		#	repeat_flag = True
		#	break

		if cand_hotel not in hotel_name_en_dic:
			#print 'Error, no name! ' + cand_hotel
			continue

		cand_hotel_name_en = hotel_name_en_dic[cand_hotel]

		if cand_hotel not in hotel_name_ch_dic:
			#print 'Error, no ch name'
			continue

		cand_hotel_name_ch = hotel_name_ch_dic[cand_hotel]

		if cand_hotel not in map_info_dic:
			#print 'Error, no map_info! ' + cand_hotel
			continue
		
		cand_map_info = map_info_dic[cand_hotel]
		#start = time.time()
		if distance == 10000:
			new_dist = getDistSimply(float(map_info.split(',')[0]),float(map_info.split(',')[1]),float(cand_map_info.split(',')[0]),float(cand_map_info.split(',')[1]))
		else:
			new_dist = distance
		dist_set_count += 1
		if index_flag == 0:
			try:
				if new_dist <= 1000:
					if distance != 10000 and isSame == 1:
						return new_dist, -1, -1, -1, False, False,True, max_esim1, max_esim2, max_esim3, min_dist, min_esim1, min_esim2, min_esim3, max_dist
					#elif distance != 10000 and isSame == 0:
					#	return new_dist, -1, -1, -1, False, False,False
					if distance != 10000:
						continue
					if	cand_hotel_name_ch == "None" or cand_hotel_name_ch == "NULL":
						cand_hotel_name_ch = ''
					if hotel_name_ch == "None" or hotel_name_ch == "NULL":
						hotel_name_ch = ''
					if	cand_hotel_name_en == "None" or cand_hotel_name_en == "NULL":
						cand_hotel_name_en = ''
					if hotel_name_en == "None" or hotel_name_en == "NULL":
						hotel_name_en = ''
					if hotel_name_ch != hotel_name_en and hotel_name_ch != "None" and hotel_name_en != "None" and hotel_name_ch != "NULL" and hotel_name_en != "NULL":
						ch_en_name = hotel_name_ch + hotel_name_en
					elif hotel_name_ch != "None" and hotel_name_ch != "NULL":
						ch_en_name = hotel_name_ch
					else:
						ch_en_name = hotel_name_en

					if cand_hotel_name_ch != cand_hotel_name_en and cand_hotel_name_en != "None" and cand_hotel_name_ch != "None" and cand_hotel_name_ch != "NULL" and cand_hotel_name_en != "NULL":
						cand_ch_en_name = cand_hotel_name_ch + cand_hotel_name_en
					elif cand_hotel_name_ch != "None" and  cand_hotel_name_ch != "NULL":
						cand_ch_en_name = cand_hotel_name_ch
					else:
						cand_ch_en_name = cand_hotel_name_en

					#print "#"*100
					#print cand_hotel,hotel_name_en,cand_hotel_name_en,hotel_name_ch,cand_hotel_name_ch,ch_en_name,cand_ch_en_name
					#print "#"*100
					#1、中文名：大于等于3个汉字，且名字相同
					#2、英文名：大于等于3个词，且名字相同
					#3、中文名+英文名：大于等于3个词，且名字相同
					#start = time.time()
					#if (enstr2dict(hotel_name_en) == enstr2dict(cand_hotel_name_en) and getChOrEnCount(hotel_name_en))>=2:
					if (enstr2dict(hotel_name_en) == enstr2dict(cand_hotel_name_en) and hotel_name_en !='' and cand_hotel_name_en != ''):
						has_same_name_flag = True
						return new_dist, -1, -1, -1, False, False,has_same_name_flag, max_esim1, max_esim2, max_esim3, min_dist, min_esim1, min_esim2, min_esim3, max_dist
					#if (chstr2dict(hotel_name_ch) == chstr2dict(cand_hotel_name_ch) and getChOrEnCount(hotel_name_ch))>=2:
					if (chstr2dict(hotel_name_ch) == chstr2dict(cand_hotel_name_ch) and hotel_name_ch != '' and cand_hotel_name_ch != ''):
						has_same_name_flag = True
						return new_dist, -1, -1, -1, False, False,has_same_name_flag, max_esim1, max_esim2, max_esim3, min_dist, min_esim1, min_esim2, min_esim3, max_dist
					#if (chstrAndenstr2dict(ch_en_name) == chstrAndenstr2dict(cand_ch_en_name) and getChOrEnCount(cand_ch_en_name))>=2: 
					if (chstrAndenstr2dict(ch_en_name) == chstrAndenstr2dict(cand_ch_en_name) and ch_en_name != '' and cand_ch_en_name != ''): 
						has_same_name_flag = True
						return new_dist, -1, -1, -1, False, False,has_same_name_flag, max_esim1, max_esim2, max_esim3, min_dist, min_esim1, min_esim2, min_esim3, max_dist

				#if new_dist > 500 and has_same_name_flag:
				#print "$"*100
				#print has_same_name_flag
				#print "$"*100
				if has_same_name_flag:
					return new_dist, -1, -1, -1, False, False,has_same_name_flag, max_esim1, max_esim2, max_esim3, min_dist, min_esim1, min_esim2, min_esim3, max_dist
			except Exception,e:
				print "error is:",e
				print 'cal dist fail! ' +  map_info + ' ' + cand_map_info + '\t' + cand_hotel
				continue
		else:
			try:
				'''
				hotelUnionKey = source + '\t' + source_id + '\t' + cand_cid
				redis_key1 = "sim__" + cand_hotel + "__" + hotelUnionKey
				redis_key2 = "sim__" + hotelUnionKey + "__" + cand_hotel
				pikaNum1 = int(get_md5_value(redis_key1)[-2:],16)%len(pikaIpList)
				pikaNum2 = int(get_md5_value(redis_key2)[-2:],16)%len(pikaIpList)
				try:
					r1 = r[pikaNum1]
					r2 = r[pikaNum2]
					simInfo = None
					simInfo = r1.get(redis_key1)
					if simInfo == None:
						simInfo = r2.get(redis_key2)
					if simInfo != None:
						new_esim2_str, new_esim3_str, new_esim1_str = simInfo.split('__')
						new_esim1 = float(new_esim1_str)
						new_esim2 = float(new_esim2_str)
						new_esim3 = float(new_esim3_str)
						sim_1_set_count += 1
						sim_2_set_count += 1
						sim_3_set_count += 1
				except:
					pass
						#and 'null' != cand_hotel_name_en.lower() and 'null' != hotel_name_en.lower() \
						#and 'none' != cand_hotel_name_en.lower() and 'none' != hotel_name_en.lower() \
						#and hotel_name_ch != 'None' \
						#and hotel_name_ch != 'NULL' \
				'''
				if new_dist > 1000:
					break
				
				new_esim1 = 0.0
				new_esim2 = 0.0
				new_esim3 = 0.0
				
				#en name
				if not is_contain_ch(cand_hotel_name_en.decode('utf-8')) \
						and not is_contain_ch(hotel_name_en.decode('utf-8')) \
						and cand_hotel_name_en != "NULL" and cand_hotel_name_en != "None" \
						and '' != cand_hotel_name_en.strip() and '' != hotel_name_en.strip():
							if distance != 10000:
								new_esim2 = enSim
							else:
								new_esim2 = cos(enstr2dict(cand_hotel_name_en),enstr2dict(hotel_name_en))
							sim_2_set_count += 1
			
				#ch name
				if is_contain_ch(cand_hotel_name_ch.decode('utf-8')) \
						and cand_hotel_name_ch != "NULL" and cand_hotel_name_ch != "None" \
						and is_contain_ch(hotel_name_ch.decode('utf-8')):
							if distance != 10000:
								new_esim3 = chSim
							else:
								new_esim3 = cos(chstr2dict(cand_hotel_name_ch.decode('utf-8')),chstr2dict(hotel_name_ch.decode('utf-8')))
							sim_3_set_count += 1
				'''
				if isSame == 1:
					return new_dist, -1, -1, -1, False, False, True, max_esim1, max_esim2, max_esim3, min_dist, min_esim1, min_esim2, min_esim3, max_dist
				'''
				same = isSameHotel(new_dist,new_esim1,new_esim2,new_esim3,repeat_flag,cand_star, starSet, hotel_name_ch, hotel_name_en, 1)
				if same:
					return new_dist, -1, -1, -1, False, False, True, max_esim1, max_esim2, max_esim3, min_dist, min_esim1, min_esim2, min_esim3, max_dist
				'''
				dist += new_dist
				#esim1 += new_esim1
				esim2 += new_esim2
				esim3 += new_esim3
				#print "^"*100
				#print dist,dist_set_count,esim2,sim_2_set_count,esim3,sim_3_set_count
				#print "^"*100

				if new_esim2 > max_esim2:
					max_esim2 = new_esim2
				if new_esim3 > max_esim3:
					max_esim3 = new_esim3
				if new_esim2 < min_esim2:
					min_esim2 = new_esim2
				if new_esim3 < min_esim3:
					min_esim3 = new_esim3
				if new_dist < min_dist:
					min_dist = new_dist
				if new_dist > max_dist:
					max_dist = new_dist
				'''
		
			except Exception ,e:
				traceback.print_exc()
				print 'error occ'
				print 'error code = ' + str(e)
				sys.exit(1)

	return new_dist, -1, -1, -1, False, False, False, max_esim1, max_esim2, max_esim3, min_dist, min_esim1, min_esim2, min_esim3, max_dist
	if 0 == dist_set_count:
		return 1000000,-1,-1,-1,repeat_flag,False,has_same_name_flag, max_esim1, max_esim2, max_esim3, min_dist, min_esim1, min_esim2, min_esim3, max_dist
	
	if dist_set_count > 0:
		dist = dist / dist_set_count
	else:
		dist = 1000000

	if sim_1_set_count > 0:
		esim1 = esim1 / sim_1_set_count
	else:
		esim1 = 0.0

	if sim_2_set_count > 0:
		esim2 = esim2 / sim_2_set_count
	else:
		esim2 = 0.0
	
	if sim_3_set_count > 0:
		esim3 = esim3 / sim_3_set_count
	else:
		esim3 = 0.0
	
	if debug_flag:
		print dist,esim1,esim2,esim3,repeat_flag

	#print "&"*100
	#print dist,dist_set_count,esim1,esim2,sim_2_set_count,esim3,sim_3_set_count,repeat_flag,same_flag,has_same_name_flag
	#print "&"*100
	return dist,esim1,esim2,esim3,repeat_flag,same_flag,has_same_name_flag, max_esim1, max_esim2, max_esim3, min_dist, min_esim1, min_esim2, min_esim3, max_dist

def chstrAndenstr2dict(chstr_enstr):
	#ch_word_dic = enstr2dict(chstr_enstr) 
	#'''
	r_ch = re.compile(ur'[^\u4e00-\u9fa5]')  
	chstr = ''.join(r_ch.split(chstr_enstr))
	ch_word_dic = chstr2dict(chstr)

	r_en = re.compile(ur'[\u4e00-\u9fa5]')
	enstr = ' '.join(r_en.split(chstr_enstr))
	#en_word_dic = enstr2dict(enstr)
	en_word_dic = str2dict(enstr)

	for word in en_word_dic:
		ch_word_dic[word] = en_word_dic[word]
	#'''
	return ch_word_dic

def getChOrEnCount(chstr_or_enstr):
	chstr_or_enstr = chstr_or_enstr.decode('utf-8')
	word_dic = chstrAndenstr2dict(chstr_or_enstr)
	cnt = 0
	
	for word in word_dic:
		cnt += int(word_dic[word])
	#sys.stdout.write("mzyDebug cnt\t" + cand_hotel + "\t" + source + '\t' + source_id + '\t' + cand_cid + "\n")
	#sys.stdout.write("mzyDebug cnt\t" + chstr_or_enstr + "\t" + str(cnt) + "\n")
	#sys.stdout.flush()
	return cnt

def cal_sim_single(hotel_name_ch, hotel_name_en, map_info, cand_hotel_name_ch, cand_hotel_name_en, cand_map_info):
	
	has_same_same_flag =False
	new_esim2 = 0.0 
	new_esim3 = 0.0 
	new_dist=0.0
	try:
		new_dist = getDistSimply(float(map_info.split(',')[0]),float(map_info.split(',')[1]),\
				float(cand_map_info.split(',')[0]),float(cand_map_info.split(',')[1]))
		#if new_dist > 500 :
		#	return new_dist, -1, -1, has_same_same_flag
	except Exception,e:
		print 'cal dist fail! ' + str(e) 
		return new_dist, -1, -1, has_same_same_flag
		
	#if new_dist <= 2000:
	if (hotel_name_ch.strip() != '' and hotel_name_ch.lower() != 'null' \
				and hotel_name_ch == cand_hotel_name_ch)\
				or (hotel_name_en.strip() != '' and hotel_name_en.lower() != 'null' \
				and hotel_name_en == cand_hotel_name_en):
						has_same_name_flag = True

	try:
		new_esim2 = 0.0
		if not is_contain_ch(cand_hotel_name_en.decode('utf-8')) \
					and not is_contain_ch(hotel_name_en.decode('utf-8')) \
					and 'null' != cand_hotel_name_en.lower() and 'null' != hotel_name_en.lower() \
				and '' != cand_hotel_name_en.strip() and '' != hotel_name_en.strip():
					new_esim2 = cos(enstr2dict(cand_hotel_name_en),enstr2dict(hotel_name_en))
			
		new_esim3 = 0.0
		if is_contain_ch(cand_hotel_name_ch.decode('utf-8')) \
				and is_contain_ch(hotel_name_ch.decode('utf-8')):
					new_esim3 = cos(chstr2dict(cand_hotel_name_ch.decode('utf-8')),chstr2dict(hotel_name_ch.decode('utf-8')))
	except Exception, e:
		new_esim2 = -1
		new_esim3 = -1
	return new_dist, new_esim3, new_esim2, has_same_same_flag


if __name__ == '__main__':
	enCh = '巴黎高速列车北站宜必思酒店 Lall Ji Tourist Resort'
	print json.dumps(chstrAndenstr2dict(enCh), ensure_ascii=False)
	exit(0)
	name_en_list = ['Lall Ji Tourist Resort', 'Hotel Route-Inn  ', 'Luxurious Apartments Near City','House of Love @  ']
	for name_en in name_en_list:
		print name_en
		print json.dumps(enstr2dict(name_en), ensure_ascii=False)
	exit(0)
	"""
	name = "东京希尔顿酒店"
	name = name.decode('utf-8')
	print json.dumps(chstrAndenstr2dict(name), ensure_ascii=False)
	print name
	exit(0)
	r_ch = re.compile(ur'[^\u4e00-\u9fa5]')  
	print json.dumps(r_ch.split(name), ensure_ascii=False)

	r_en = re.compile(ur'[\u4e00-\u9fa5]')
	print json.dumps(r_en.split(name), ensure_ascii=False)
	exit(0)
	"""

	"""
	name = "Hôtel R de"
	print name
	print getChOrEnCount(name.decode('utf-8'))
	print json.dumps(enstr2dict(name.decode('utf-8')), ensure_ascii=False)

	"""
	name_en = "12惠灵顿1312酒店"
	print chstr2dict(name_en.encode('utf-8'))
	name1 = "W	 - Downtown121"
	print enstr2dict(name1)
	print json.dumps(chstr2dict(name_en.decode('utf-8')), ensure_ascii=False)
	name1 = "YOTEL - Times Square"
	name2 = "Yotel"
	print cos(enstr2dict(name1), enstr2dict(name2))
	print "*"*100
	print getDistSimply(40.7561871413,-73.9930352569,40.7640773,-73.9845144)
	name_1 = '中央公园酒店'
	#name_en_1 = 'Adagio La Defense Esplanade'

	name_2 = '惠灵顿酒店'
	#name_en_2 = 'Adagio La Defense Esplanade'
	#
	#print cos(str2dict(name_en_1),str2dict(name_en_2))
	#print cos(enstr2dict(name_en_1),enstr2dict(name_en_2))
	print cos(chstr2dict(name_1.decode('utf-8')),chstr2dict(name_2.decode('utf-8')))
	print getChOrEnCount('巴黎高速列车北站宜必思酒店')
	#
	#map_1 = sys.argv[1]
	#map_2 = sys.argv[2]

	#print getDistSimply(float(map_1.split(',')[0]),float(map_1.split(',')[1]),float(map_2.split(',')[0]),float(map_2.split(',')[1]))
