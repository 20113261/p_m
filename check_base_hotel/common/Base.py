#encoding=utf-8
import sys
import json
# import MySQLdb
import pymysql as MySQLdb
MySQLdb.install_as_MySQLdb()
import traceback
import time,datetime
import redis

sys.path.append("/search/zhaowenbo/Fenbushi/new_merge_hotel_lk_opt/")
import config

from LOG import _ERROR, _INFO
from cal_sim import get_dist_by_map

#到城市中心距离超过50km的酒店直接过滤
filter_dist = 50000
#以下城市不受50km的距离限制
white_city_list = ["50033","50061","50066","50067","50846"]

dev_db_ip = config.dev_db_ip
dev_db_user = config.dev_db_user
dev_db_pwd = config.dev_db_pwd
dev_db_name = config.dev_db_name

def getDistWhiteCityList():
    return white_city_list

#获取基础数据中不可用数据
#过滤规则包括:名字为空,坐标格式不对,离市中心太远
def get_confilict_data(db_ip,db_name,db_table):
    cid2map = get_city_map()    
    conn = MySQLdb.connect(host=db_ip, user='root', charset='utf8',passwd='',db=db_name)
    cursor = conn.cursor()

    sql = "select source,source_id,hotel_name,hotel_name_en,city_id,map_info from " + db_table
    cursor.execute(sql)

    sid2cid = {}
    
    datas = cursor.fetchall()
    drop_sid = set()
    name_filter_count = 0
    map_info_filter_count = 0
    dist_filter_count = 0
    except_filter_count = 0
    unknown_cid  = 0
    cid_double = 0
    for data in datas:
        try:
            source = data[0].encode('utf-8')
            sid = data[1].encode('utf-8')
            city_id = data[4].encode('utf-8')
            map_info = data[5].encode('utf-8')
            
            cand_key = source + '\t' + sid + '\t' + city_id
#           if city_id not in cid2map:
#               unknown_cid += 1
                #print "haha cid\t" + cand_key
#               drop_sid.add(cand_key)
#               continue
            try:
                city_map = cid2map[city_id]
            except:
                city_map=""
            
            if data[2] == None or '' == data[2]:
                name = 'NULL'
            else:
                name = data[2].encode('utf-8')

            if data[3] == None or '' == data[3]:
                name_en = 'NULL'
            else:
                name_en = data[3].encode('utf-8')

            if 'NULL' == name and 'NULL' == name_en:
                #print "haha name\t" + cand_key
                drop_sid.add(cand_key)
                name_filter_count += 1
                continue


#           try:
#               map_info_list = map_info.strip().split(',')
#
#               lat = float(map_info_list[0])
#               lgt = float(map_info_list[1])
#
#
###             cand_dist = get_dist_by_map(map_info,city_map)

#               if cand_dist > filter_dist and city_id not in white_city_list:
#                   #print "haha dist\t" + cand_key +'\t' + str(cand_dist)
#                   drop_sid.add(cand_key)
##                  dist_filter_count += 1
#                   continue
#
#           except Exception,e:
#               exstr = traceback.format_exc()
#               print 'MAP_ERROR ' + cand_key
#               #print "haha map\t" + cand_key
#               drop_sid.add(cand_key)
#               except_filter_count += 1
#               continue

            if cand_key not in sid2cid:
                sid2cid[cand_key] = set()

            sid2cid[cand_key].add(city_id)

        except Exception,e:
            exstr = traceback.format_exc()
            print exstr
            sys.exit(1)
            continue

    for cand_key in sid2cid:
        if len(sid2cid[cand_key]) > 1:
            #print "haha sid\t" + cand_key
            drop_sid.add(cand_key)
            cid_double += 1

    print 'Drop sid count: ' + str(len(drop_sid))
    print 'name_filter: ' + str(name_filter_count)
    print 'dist_filter_count: ' + str(dist_filter_count)
    print 'map_info_filter_count:' + str(map_info_filter_count)
    print 'except_filter_count: ' + str(except_filter_count)
    print 'unkone_cid_count: ' + str(unknown_cid)
    print 'cid double: ' + str(cid_double)
    
    return drop_sid

def getHotelSource():
    conn = MySQLdb.connect(host = dev_db_ip,user = dev_db_user,passwd = dev_db_pwd,db = dev_db_name,charset='utf8')
    cursor = conn.cursor()

    sql = "select name from source where type = 'hotel';"
    
    cursor.execute(sql)

    datas = cursor.fetchall()
    
    source_set = set()

    for data in datas:
        if None in data:
            continue

        cand_source = data[0].encode('utf-8')

        source_set.add(cand_source)

    cursor.close()
    conn.close()

    return source_set


def get_city_map():
    conn = MySQLdb.connect(host = dev_db_ip,user = dev_db_user,passwd = dev_db_pwd,db = dev_db_name,charset='utf8')
    cursor = conn.cursor()

    sql = "select id,map_info from city;"

    cursor.execute(sql)

    datas = cursor.fetchall()

    cid2map = {}

    for data in datas:
        if None in data:
            continue

        cid = data[0].encode('utf-8')
        map_info = data[1].encode('utf-8')

        try:
            map_info_list = map_info.strip().split(',')

            lat = float(map_info_list[0])
            lgt = float(map_info_list[1])

        except Exception,e:
            #print 'cid_erro: ' + str(e)
            continue

        cid2map[cid] = map_info

    
    cursor.close()
    conn.close()
    print 'cid size:' + str(len(cid2map))
    return cid2map


def load_city_data(db_ip,db_name,table_name):
    conn = MySQLdb.connect(host = dev_db_ip,user = dev_db_user,passwd = dev_db_pwd,db = dev_db_name,charset='utf8')
    cursor = conn.cursor()

    #sql = "select id,name,country,map_info from " + table_name
    sql = "select %s.id,%s.name,country.name,%s.map_info from %s,country where %s.country_id=country.mid" % \
            (table_name, table_name, table_name, table_name, table_name)
    print sql
    cursor.execute(sql)

    cid2name = {}
    cid2mapInfo = {}

    for data in cursor.fetchall():
        if None in data:
            continue
        
        cid = data[0].encode('utf-8')
        city = data[1].encode('utf-8').strip().lower()
        country = data[2].encode('utf-8').strip().lower()
        map_info = data[3].encode('utf-8').strip()

        try:
            tmp_list = map_info.strip().split(',')
            lng = float(tmp_list[0])
            lat = float(tmp_list[1])
        except:
            continue

        cid2name[cid] = (city,country)
        
        cid2mapInfo[cid] = map_info


    _INFO('Load ready city',['ready city count:' + str(len(cid2name))])

    return cid2name,cid2mapInfo



#get max uid from merged unid table
def get_max_unid(db_ip,db_name,table_name):
    conn = MySQLdb.connect(host=db_ip, user='root', charset='utf8',passwd='',db = db_name)
    cursor = conn.cursor()

    max_uid = -1

    sql = "select uid,mid from " + table_name;
    cursor.execute(sql) 
    datas = cursor.fetchall()
    
    for data in datas:
        cand_uid = int(data[0].encode('utf8')[2:])

        if cand_uid > max_uid:
            max_uid = cand_uid

    return max_uid

#def get_sid2mid():
#   conn = MySQLdb.connect(host='127.0.0.1', user='root', charset='utf8',passwd='',db = 'merge_hotel_20150706')
#   cursor = conn.cursor()
#   
#   sid2mid = {}
#   sql = "select source,sid,mid from hotel_unid;"
#   cursor.execute(sql)
#
#   for data in cursor.fetchall():
#       if None in data:
#           continue


def load_merged_mapInfo_and_hotelName(db_ip,db_name,table_name, hit_cid_set):
    conn = MySQLdb.connect(host=db_ip, user='root', charset='utf8',passwd='',db = db_name)
    cursor = conn.cursor()
    
    sourceid2mapInfo = {}
    sourceid2hotelNameEn = {}
    sourceid2hotelNameCh = {}
    sourceid2grade = {}
    sourceid2star = {}

    countryId2info = {}
    sql = "select source,sid,name,name_en,map_info,mid,grade,star from " + table_name + " where source !='daodao';"
    
    cursor.execute(sql)
    datas = cursor.fetchall()
    
    for data in datas:
        word_list = []
        if data[0] == None:
            continue
        word_list.append(data[0])
        for idx in range(1,len(data)-1):
            word = data[idx]

            if None == word or '' == word:
                word_list.append('NULL')
            else:
                word_list.append(word.encode('utf-8').lower())
        
        if None == data[6]:
            grade = 'NULL'
        
        #int or float
        elif type(data[6]) == type(1) or type(data[6]) == type(1.0):
            grade = str(data[6])
        else:
            try:
                grade = data[6].encode('utf-8')
            except Exception,e:
                grade = '-1'

        if "" == data[7].strip() :
            star = "-1.0"
        else :
            try:
                star_fl = float(data[7].encode("utf-8").strip())
                star = str(star_fl)
            except Exception, e:
                star = "-1.0"

        source = word_list[0]
        source_id = word_list[1]
        map_info = word_list[4]
        city_id = word_list[5]

        if city_id not in hit_cid_set:
            continue

        map_key = source + '\t' + source_id + '\t' + city_id

        name = word_list[2]
        name_en = word_list[3]
        
        if 'NULL' == name and 'NULL' == name_en:
            continue

        sourceid2mapInfo[map_key] = map_info
        sourceid2hotelNameEn[map_key] = name_en
        sourceid2hotelNameCh[map_key] = name
        sourceid2grade[map_key] = grade
        sourceid2star[map_key] = star

    print 'Base data size: ' + str(len(sourceid2hotelNameEn))
    return sourceid2mapInfo,sourceid2hotelNameEn,sourceid2hotelNameCh,sourceid2grade,sourceid2star

def load_merged_mapInfo_and_hotelName_new(db_ip,db_name,table_name, cid2countryId):
    conn = MySQLdb.connect(host=db_ip, user='root', charset='utf8',passwd='',db = db_name)
    cursor = conn.cursor()
    
    sourceid2mapInfo = {}
    sourceid2hotelNameEn = {}
    sourceid2hotelNameCh = {}
    sourceid2grade = {}
    sourceid2star = {}

    countryId2info = {}
    sql = "select source,sid,name,name_en,map_info,mid,grade,star,country_id from " + table_name + " where source !='daodao';"
    
    cursor.execute(sql)
    datas = cursor.fetchall()
    
    for data in datas:
        word_list = []
        if data[0] == None:
            continue
        word_list.append(data[0])
        for idx in range(1,len(data)-1):
            word = data[idx]

            if None == word or '' == word:
                word_list.append('NULL')
            else:
                word_list.append(word.encode('utf-8').lower())
        
        if None == data[6]:
            grade = 'NULL'
        
        #int or float
        elif type(data[6]) == type(1) or type(data[6]) == type(1.0):
            grade = str(data[6])
        else:
            try:
                grade = data[6].encode('utf-8')
            except Exception,e:
                grade = '-1'

        if "" == data[7].strip() :
            star = "-1.0"
        else :
            try:
                star_fl = float(data[7].encode("utf-8").strip())
                star = str(star_fl)
            except Exception, e:
                star = "-1.0"

        source = word_list[0]
        source_id = word_list[1]
        map_info = word_list[4]
        city_id = word_list[5]

        map_key = source + '\t' + source_id + '\t' + city_id

        name = word_list[2]
        name_en = word_list[3]
        
        if 'NULL' == name and 'NULL' == name_en:
            continue

        #if city_id not in cid2countryId:
        #   print "load_merged_mapInfo_and_hotelName error: not find country for cid=" + city_id
        #   continue
        #country_id = cid2countryId[city_id]
        country_id = word_list[8]
        if country_id not in countryId2info:
            countryId2info[country_id] = {}
            countryId2info[country_id]["sourceid2mapInfo"] = {}
            countryId2info[country_id]["sourceid2hotelNameEn"] = {} 
            countryId2info[country_id]["sourceid2hotelNameCh"] = {}
            countryId2info[country_id]["sourceid2grade"] = {}
            countryId2info[country_id]["sourceid2star"] = {}
    
        countryId2info[country_id]["sourceid2mapInfo"][map_key] = map_info
        countryId2info[country_id]["sourceid2hotelNameEn"][map_key] = name_en
        countryId2info[country_id]["sourceid2hotelNameCh"][map_key] = name
        countryId2info[country_id]["sourceid2grade"][map_key] = grade
        countryId2info[country_id]["sourceid2star"][map_key] = star
    
    #print 'Base data size: ' + str(len(sourceid2hotelNameEn))
    return countryId2info


def load_unmerged_data(db_ip,db_name,table_name_list,sourceid2mapInfo,sourceid2hotelNameEn,\
        sourceid2hotelNameCh,sourceid2grade,sourceid2star, hit_cid_set):
    conn = MySQLdb.connect(host=db_ip, user='root', charset='utf8',passwd='',db=db_name)
    cursor = conn.cursor()

    unmerged_data = set()
    for table_name in table_name_list:
        print 'Load ' + table_name

        drop_sid = get_confilict_data(db_ip,db_name,table_name)

        good_data_count = 0

        sql = "select source,source_id,hotel_name,hotel_name_en,map_info,city_id,grade,star from " + table_name + ";"
        
        cursor.execute(sql)

        datas = cursor.fetchall()
        
        print 'ori data count: ' + str(len(datas))

        skip_dic = {}
        
        for data in datas:
            if None == data[0] or None == data[1] or None == data[5]:
                if 'source_sid_null' not in skip_dic:
                    skip_dic['source_sid_null'] = 0

                skip_dic['source_sid_null'] += 1

                continue

            source = data[0].encode('utf-8')
            sid = data[1].encode('utf-8')
            city_id = data[5].encode('utf-8')

            if city_id not in hit_cid_set:
                continue

            cand_key = source + '\t' + sid + '\t' + city_id

            if cand_key in drop_sid:
                if 'confilict_data' not in skip_dic:
                    skip_dic['confilict_data'] = 0

                skip_dic['confilict_data'] += 1
                continue

            if None == data[2]:
                name = 'NULL'
            else:
                name = data[2].encode('utf-8').strip()

            if None == data[3]:
                name_en = 'NULL'
            else:
                name_en = data[3].encode('utf-8').strip()

            if 'NULL' == name and 'NULL' == name_en:
                if 'name_null' not in skip_dic:
                    skip_dic['name_null'] = 0

                skip_dic['name_null'] += 1

                continue

            #if 'NULL' == name or "" == name:
            #   name = name_en

            #if 'NULL' == name_en or "" == name_en:
            #   name_en = name

            if None == data[4]:
                if 'map_null' not in skip_dic:
                    skip_dic['map_null'] = 0

                skip_dic['map_null'] += 1

                continue

            map_info = data[4].encode('utf-8')

            try:
                lat = float(map_info.strip().split(',')[0])
                lgt = float(map_info.strip().split(',')[1])

            except Exception,e:
                if 'map_error' not in skip_dic:
                    skip_dic['map_error'] = 0

                skip_dic['map_error'] += 1

                continue

        
            if None == data[6]:
                grade = 'NULL'
            #int or float
            elif type(data[6]) == type(1) or type(data[6]) == type(1.0):
                grade = str(data[6])
            else:
                try:
                    grade = data[6].encode('utf-8')
                except Exception,e:
                    grade = '-1'

            if "" == data[7]:
                star = "-1.0"
            else :
                try:
                    star_fl = float(data[7].encode("utf-8").strip())
                    star = str(star_fl)
                except Exception, e:
                    star = "-1.0"

            sourceid2mapInfo[cand_key] = map_info
            sourceid2hotelNameCh[cand_key] = name
            sourceid2hotelNameEn[cand_key] = name_en
            sourceid2grade[cand_key] = grade
            sourceid2star[cand_key] = star
            unmerged_data.add(cand_key)

            good_data_count += 1

        print 'Load ' + table_name + ' Done. data_count = ' + str(good_data_count) + '\n'
        
        for cand_error in skip_dic:
            print cand_error + '\t' + str(skip_dic[cand_error])

    print 'unmerged_data: ' + str(len(unmerged_data))


    return unmerged_data

def load_unmerged_data(db_ip,db_name,table_name_list,sourceid2mapInfo,sourceid2hotelNameEn,\
        sourceid2hotelNameCh,sourceid2grade,sourceid2star, hit_cid_set):
    conn = MySQLdb.connect(host=db_ip, user='root', charset='utf8',passwd='',db=db_name)
    cursor = conn.cursor()

    unmerged_data = set()
    for table_name in table_name_list:
        print 'Load ' + table_name

        drop_sid = get_confilict_data(db_ip,db_name,table_name)

        good_data_count = 0

        sql = "select source,source_id,hotel_name,hotel_name_en,map_info,city_id,grade,star from " + table_name + ";"
        
        cursor.execute(sql)

        datas = cursor.fetchall()
        
        print 'ori data count: ' + str(len(datas))

        skip_dic = {}
        
        for data in datas:
            if None == data[0] or None == data[1] or None == data[5]:
                if 'source_sid_null' not in skip_dic:
                    skip_dic['source_sid_null'] = 0

                skip_dic['source_sid_null'] += 1

                continue

            source = data[0].encode('utf-8')
            sid = data[1].encode('utf-8')
            city_id = data[5].encode('utf-8')

            if city_id not in hit_cid_set:
                continue

            cand_key = source + '\t' + sid + '\t' + city_id

            if cand_key in drop_sid:
                if 'confilict_data' not in skip_dic:
                    skip_dic['confilict_data'] = 0

                skip_dic['confilict_data'] += 1
                continue

            if None == data[2]:
                name = 'NULL'
            else:
                name = data[2].encode('utf-8').strip()

            if None == data[3]:
                name_en = 'NULL'
            else:
                name_en = data[3].encode('utf-8').strip()

            if 'NULL' == name and 'NULL' == name_en:
                if 'name_null' not in skip_dic:
                    skip_dic['name_null'] = 0

                skip_dic['name_null'] += 1

                continue

            #if 'NULL' == name or "" == name:
            #   name = name_en

            #if 'NULL' == name_en or "" == name_en:
            #   name_en = name

            if None == data[4]:
                if 'map_null' not in skip_dic:
                    skip_dic['map_null'] = 0

                skip_dic['map_null'] += 1

                continue

            map_info = data[4].encode('utf-8')

            try:
                lat = float(map_info.strip().split(',')[0])
                lgt = float(map_info.strip().split(',')[1])

            except Exception,e:
                if 'map_error' not in skip_dic:
                    skip_dic['map_error'] = 0

                skip_dic['map_error'] += 1

                continue

        
            if None == data[6]:
                grade = 'NULL'
            #int or float
            elif type(data[6]) == type(1) or type(data[6]) == type(1.0):
                grade = str(data[6])
            else:
                try:
                    grade = data[6].encode('utf-8')
                except Exception,e:
                    grade = '-1'

            if "" == data[7]:
                star = "-1.0"
            else :
                try:
                    star_fl = float(data[7].encode("utf-8").strip())
                    star = str(star_fl)
                except Exception, e:
                    star = "-1.0"

            sourceid2mapInfo[cand_key] = map_info
            sourceid2hotelNameCh[cand_key] = name
            sourceid2hotelNameEn[cand_key] = name_en
            sourceid2grade[cand_key] = grade
            sourceid2star[cand_key] = star
            unmerged_data.add(cand_key)

            good_data_count += 1

        print 'Load ' + table_name + ' Done. data_count = ' + str(good_data_count) + '\n'
        
        for cand_error in skip_dic:
            print cand_error + '\t' + str(skip_dic[cand_error])

    print 'unmerged_data: ' + str(len(unmerged_data))


    return unmerged_data

def load_merged_data(db_ip,db_name,db_table,unmerged_data_set, hit_cid_set):
    conn = MySQLdb.connect(host=db_ip, user='root', charset='utf8',passwd='',db=db_name)
    cursor = conn.cursor()

    #sql = "select source,sid,uid,mid,status from " + db_table + " where source !='daodao';"
    sql = "select source,sid,uid,mid,star,status from " + db_table

    cursor.execute(sql)

    datas = cursor.fetchall()

    uid2sid = {}
    uid2cid = {}
    sid2uid = {}
    uid2star = {}
    uid2status_set = {}
    num = 0
    for data in datas:
        num += 1
        word_list = []
        for idx in range(len(data) - 1):
            word_list.append(data[idx].encode('utf-8'))
    
        word_list.append(data[-1])
        source = word_list[0]
        sid = word_list[1]
        uid = word_list[2]
        mid = word_list[3]

        if mid not in hit_cid_set:
            continue
        
        if "" == word_list[4].strip() :
            star = "-1.0"
        else :
            try:
                star_fl = float(word_list[4].encode("utf-8").strip())
                star = str(star_fl)
            except Exception, e:
                star = "-1.0"
        status = word_list[5]
        key = source + '\t' + sid + '\t' + mid
        
        if key in unmerged_data_set:
            continue

        if uid not in uid2sid:
            uid2sid[uid] = set()
        
        if uid not in uid2status_set:
            uid2status_set[uid] = set()

        if uid not in uid2star:
            uid2star[uid] = set()

        uid2status_set[uid].add(status)

        uid2sid[uid].add(key)
        uid2star[uid].add(star)
        uid2cid[uid] = mid
        sid2uid[key] = uid 
    
    uid2status = {}
    for uid in uid2status_set:
        cand_set = uid2status_set[uid]
        if 4 in cand_set:
            uid2status[uid] = 4
        elif 2 in cand_set:
            uid2status[uid] = 2
        elif 3 in cand_set:
            uid2status[uid] = 3
        else:
            uid2status[uid] = 0
    

    _INFO('Load merged data',['merged data count = ' + str(num)])

    return uid2sid,uid2cid,sid2uid,uid2status,uid2star


#def load_unmerged_data():
#   conn = MySQLdb.connect(host='127.0.0.1', user='root', charset='utf8',passwd='',db='merge_hotel_20150706')
#   cursor = conn.cursor()
#   
#
#   uid2sid,uid2cid,sid2uid,uid2status = load_merged_data('127.0.0.1','merge_hotel_20150706','hotel_unid')
#   drop_sid = get_confilict_data('127.0.0.1','merge_hotel_20150706','hotelinfo_expedia')
#   
#   sql = "select source,source_id from hotelinfo_expedia;"
#
#   cursor.execute(sql)
#
#   datas = cursor.fetchall()
#
#   unmerged_sid = set()
#
#   for data in datas:
#       if None in data:
#           continue
#
#       source = data[0].encode('utf-8')
#       sid = data[1].encode('utf-8')
#
#       cand_key = source + '\t' + sid
#
#       if cand_key in sid2uid or cand_key in drop_sid:
#           continue
#
#       unmerged_sid.add(cand_key)
#
#   print 'Unmegred: ' + str(len(unmerged_sid))


def load_merged_mapInfo_and_hotelName_new(db_ip,db_name,table_name, cid2countryId):
    conn = MySQLdb.connect(host=db_ip, user='root', charset='utf8',passwd='',db = db_name)
    cursor = conn.cursor()
    
    sourceid2mapInfo = {}
    sourceid2hotelNameEn = {}
    sourceid2hotelNameCh = {}
    sourceid2grade = {}
    sourceid2star = {}

    countryId2info = {}
    sql = "select source,sid,name,name_en,map_info,mid,grade,star,country_id from " + table_name + " where source !='daodao';"
    
    cursor.execute(sql)
    datas = cursor.fetchall()
    
    for data in datas:
        word_list = []
        if data[0] == None:
            continue
        word_list.append(data[0])
        for idx in range(1,len(data)-1):
            word = data[idx]

            if None == word or '' == word:
                word_list.append('NULL')
            else:
                word_list.append(word.encode('utf-8').lower())
        
        if None == data[6]:
            grade = 'NULL'
        
        #int or float
        elif type(data[6]) == type(1) or type(data[6]) == type(1.0):
            grade = str(data[6])
        else:
            try:
                grade = data[6].encode('utf-8')
            except Exception,e:
                grade = '-1'

        if "" == data[7].strip() :
            star = "-1.0"
        else :
            try:
                star_fl = float(data[7].encode("utf-8").strip())
                star = str(star_fl)
            except Exception, e:
                star = "-1.0"

        source = word_list[0]
        source_id = word_list[1]
        map_info = word_list[4]
        city_id = word_list[5]

        map_key = source + '\t' + source_id + '\t' + city_id

        name = word_list[2]
        name_en = word_list[3]
        
        if 'NULL' == name and 'NULL' == name_en:
            continue
        country_id = data[8]
        #if city_id not in cid2countryId:
        #   print "load_merged_mapInfo_and_hotelName error: not find country for cid=" + city_id
        #   continue
        #country_id = cid2countryId[city_id]

        if country_id not in countryId2info:
            countryId2info[country_id] = {}
            countryId2info[country_id]["sourceid2mapInfo"] = {}
            countryId2info[country_id]["sourceid2hotelNameEn"] = {} 
            countryId2info[country_id]["sourceid2hotelNameCh"] = {}
            countryId2info[country_id]["sourceid2grade"] = {}
            countryId2info[country_id]["sourceid2star"] = {}
        
        countryId2info[country_id]["sourceid2mapInfo"][map_key] = map_info
        countryId2info[country_id]["sourceid2hotelNameEn"][map_key] = name_en
        countryId2info[country_id]["sourceid2hotelNameCh"][map_key] = name
        countryId2info[country_id]["sourceid2grade"][map_key] = grade
        countryId2info[country_id]["sourceid2star"][map_key] = star
    
    #print 'Base data size: ' + str(len(sourceid2hotelNameEn))
    return countryId2info

def load_unmerged_data_new(db_ip,db_name,table_name_list, cid2countryId, countryId2info):
    conn = MySQLdb.connect(host=db_ip, user='root', charset='utf8',passwd='',db=db_name)
    cursor = conn.cursor()

    unmerged_data = set()
    unmerged_sourceid2country={}
    for table_name in table_name_list:
        print 'Load ' + table_name

        drop_sid = get_confilict_data(db_ip,db_name,table_name)

        good_data_count = 0

        sql = "select source,source_id,hotel_name,hotel_name_en,map_info,city_id,grade,star,country_id from " + table_name + ";"
        
        cursor.execute(sql)

        datas = cursor.fetchall()
        
        print 'ori data count: ' + str(len(datas))

        skip_dic = {}
        
        for data in datas:
            if None == data[0] or None == data[1] or None == data[5]:
                if 'source_sid_null' not in skip_dic:
                    skip_dic['source_sid_null'] = 0

                skip_dic['source_sid_null'] += 1

                continue

            source = data[0].encode('utf-8')
            sid = data[1].encode('utf-8')
            city_id = data[5].encode('utf-8')

            cand_key = source + '\t' + sid + '\t' + city_id

            if cand_key in drop_sid:
                if 'confilict_data' not in skip_dic:
                    skip_dic['confilict_data'] = 0

                skip_dic['confilict_data'] += 1
                continue

            if None == data[2]:
                name = 'NULL'
            else:
                name = data[2].encode('utf-8').strip()

            if None == data[3]:
                name_en = 'NULL'
            else:
                name_en = data[3].encode('utf-8').strip()

            if 'NULL' == name and 'NULL' == name_en:
                if 'name_null' not in skip_dic:
                    skip_dic['name_null'] = 0

                skip_dic['name_null'] += 1

                continue

            #if 'NULL' == name or "" == name:
            #   name = name_en

            #if 'NULL' == name_en or "" == name_en:
            #   name_en = name

            if None == data[4]:
                if 'map_null' not in skip_dic:
                    skip_dic['map_null'] = 0

                skip_dic['map_null'] += 1

                continue

            map_info = data[4].encode('utf-8')

            try:
                lat = float(map_info.strip().split(',')[0])
                lgt = float(map_info.strip().split(',')[1])

            except Exception,e:
                if 'map_error' not in skip_dic:
                    skip_dic['map_error'] = 0

                skip_dic['map_error'] += 1

                continue

        
            if None == data[6]:
                grade = 'NULL'
            #int or float
            elif type(data[6]) == type(1) or type(data[6]) == type(1.0):
                grade = str(data[6])
            else:
                try:
                    grade = data[6].encode('utf-8')
                except Exception,e:
                    grade = '-1'

            if "" == data[7]:
                star = "-1.0"
            else :
                try:
                    star_fl = float(data[7].encode("utf-8").strip())
                    star = str(star_fl)
                except Exception, e:
                    star = "-1.0"
            country_id = data[8]
            #if city_id not in cid2countryId:
            #   print "load_merged_mapInfo_and_hotelName error: not find country for cid=" + city_id
            #   continue
            #country_id = cid2countryId[city_id]
            unmerged_sourceid2country[cand_key] = country_id
            if country_id not in countryId2info:
                countryId2info[country_id] = {}
                countryId2info[country_id]["sourceid2mapInfo"] = {}
                countryId2info[country_id]["sourceid2hotelNameEn"] = {} 
                countryId2info[country_id]["sourceid2hotelNameCh"] = {}
                countryId2info[country_id]["sourceid2grade"] = {}
                countryId2info[country_id]["sourceid2star"] = {}
            
            countryId2info[country_id]["sourceid2mapInfo"][cand_key] = map_info
            countryId2info[country_id]["sourceid2hotelNameEn"][cand_key] = name_en
            countryId2info[country_id]["sourceid2hotelNameCh"][cand_key] = name
            countryId2info[country_id]["sourceid2grade"][cand_key] = grade
            countryId2info[country_id]["sourceid2star"][cand_key] = star

            unmerged_data.add(cand_key)

            good_data_count += 1

        print 'Load ' + table_name + ' Done. data_count = ' + str(good_data_count) + '\n'
        
        for cand_error in skip_dic:
            print cand_error + '\t' + str(skip_dic[cand_error])

    print 'unmerged_data: ' + str(len(unmerged_data))

    return unmerged_data, unmerged_sourceid2country

def load_merged_data_new(db_ip,db_name,db_table,unmerged_data_set,cid2countryId):
    conn = MySQLdb.connect(host=db_ip, user='root', charset='utf8',passwd='',db=db_name)
    cursor = conn.cursor()

    #sql = "select source,sid,uid,mid,status from " + db_table + " where source !='daodao';"
    sql = "select source,sid,uid,mid,star,status,country_id from " + db_table

    cursor.execute(sql)

    datas = cursor.fetchall()

    countryId2uidInfo = {}

    num = 0
    for data in datas:
        num += 1
        word_list = []
        for idx in range(len(data) - 2):
            word_list.append(data[idx].encode('utf-8'))
    
        word_list.append(data[-1])
        source = word_list[0]
        sid = word_list[1]
        uid = word_list[2]
        mid = word_list[3]
        
        if "" == word_list[4].strip() :
            star = "-1.0"
        else :
            try:
                star_fl = float(word_list[4].encode("utf-8").strip())
                star = str(star_fl)
            except Exception, e:
                star = "-1.0"
        status = word_list[5]
        key = source + '\t' + sid + '\t' + mid
        
        if key in unmerged_data_set:
            continue
        country_id= data[6]
        #if mid not in cid2countryId:
        #   print "load_merged_data_new error: not find country for cid=" + mid
        #   continue
        #country_id = cid2countryId[mid]

        if country_id not in countryId2uidInfo:
            countryId2uidInfo[country_id] = {}
            countryId2uidInfo[country_id]["uid2sid"] = {}
            countryId2uidInfo[country_id]["uid2cid"] = {}
            countryId2uidInfo[country_id]["sid2uid"] = {}
            countryId2uidInfo[country_id]["uid2star"] = {}
            countryId2uidInfo[country_id]["uid2status_set"] = {}

        uid2sid = countryId2uidInfo[country_id]["uid2sid"]
        uid2cid = countryId2uidInfo[country_id]["uid2cid"]
        sid2uid = countryId2uidInfo[country_id]["sid2uid"]
        uid2star = countryId2uidInfo[country_id]["uid2star"]
        uid2status_set = countryId2uidInfo[country_id]["uid2status_set"]

        if uid not in uid2sid:
            uid2sid[uid] = set()
        
        if uid not in uid2status_set:
            uid2status_set[uid] = set()

        if uid not in uid2star:
            uid2star[uid] = set()

        uid2status_set[uid].add(status)

        uid2sid[uid].add(key)
        uid2star[uid].add(star)
        uid2cid[uid] = mid
        sid2uid[key] = uid 
    

    for country_id in countryId2uidInfo:
        countryId2uidInfo[country_id]["uid2status"] = {}

        uid2status = countryId2uidInfo[country_id]["uid2status"]
        uid2status_set = countryId2uidInfo[country_id]["uid2status_set"]

        for uid in uid2status_set:
            cand_set = uid2status_set[uid]
            if 4 in cand_set:
                uid2status[uid] = 4
            elif 2 in cand_set:
                uid2status[uid] = 2
            elif 3 in cand_set:
                uid2status[uid] = 3
            else:
                uid2status[uid] = 0
    

    _INFO('Load merged data',['merged data count = ' + str(num)])

    #return uid2sid,uid2cid,sid2uid,uid2status,uid2star
    return countryId2uidInfo
def getSourceid(hid_list, r):
    hid2sourceidSet={}   
    sourceidSet=set()
    if len(hid_list) != 0:
        #rds_flag = False
        #while rds_flag == False:
        #   try:
        #       r = redis.Redis(host=config.pika_host, port=config.pika_port, db=0, password=config.pika_pwd)
        #       rds_flag = True
        #   except:
        #       print "connect redis failed"
        #       sleep(3)
        #print config.pika_host,config.pika_port,config.pika_pwd
        values = r.mget(hid_list)
        #print values
        for i in range(len(hid_list)):
            value = values[i]
            hid = hid_list[i]
            if value == None or value == 'Null' or value == '':
                continue
            datas=value.split('\n')
            for data in datas:
                if hid not in hid2sourceidSet:
                    hid2sourceidSet[hid] = set()
                hid2sourceidSet[hid].add(data)
                sourceidSet.add(data)
    return hid2sourceidSet, sourceidSet

def gethid2star(hid_list, r):   
    hid2star={}
    hid2sourceid = {}
    sourceid_set=set()
    if len(hid_list) != 0:
        hid2sourceid, sourceid_set = getSourceid(hid_list, r)
        #rds_flag = False
        #while rds_flag == False:
        #   try:
        #       r = redis.Redis(host=config.pika_host, port=config.pika_port, db=0, password=config.pika_pwd)
        #       rds_flag = True
        #   except:
        #       print "connect rds failed"
        #       sleep(3)
            
        for hid in hid2sourceid:
            values = r.mget(list(hid2sourceid[hid]))
            for value in values:
            #   value = r.get(sid)
                if value == None or value == 'Null' or value == '':
                    continue
                #words = value.split("|||")
                #star = words[6]
                words = json.loads(value)
                star = words['star']
                if hid not in hid2star:
                    hid2star[hid] =set()
                hid2star[hid].add(star)
    return hid2sourceid, sourceid_set, hid2star

def getsourceid2Info(sourceidSet):
    sourceid2mapInfo={}
    sourceid2hotelNameEn={}
    sourceid2hotelNameCh={}
    sourceid2grade={}
    sourceid2star={}
    sourceid2brand_tag={}
    sourceid2address={}
    r = redis.Redis(host=config.pika_host, port=config.pika_port, db=0, password=config.pika_pwd)
    sourceidList = list(sourceidSet)
    values = r.mget(list(sourceidList))
    for i in range(len(sourceidList)):
        data=values[i]
        sid =sourceidList[i]
        if data == None or data == '' or data == 'Null':
            continue
        words = json.loads(data)
        '''
        words = data.split("|||")
        if len(words) != 7:
            sys.stdout.write("cant get Info from redis, sourceid  " + sid)
            continue
        hid = words[0]
        cid = words[1]
        name = words[2].encode("utf-8").strip()
        name_en = words[3].encode("utf-8").strip()
        map_info =words[4]
        grade = words[5]
        star = words[6]
        '''
        cid = words['city_id']
        name = words['name'].encode("utf-8").strip()
        name_en = words['name_en'].encode("utf-8").strip()
        map_info = words['map_info']
        grade = words['grade']
        star = words['star']
        brand_tag = words['brand_tag']
        address = words['address']

        cand_key = sid
        sourceid2mapInfo[cand_key] = map_info
        sourceid2hotelNameCh[cand_key] = name
        sourceid2hotelNameEn[cand_key] = name_en
        sourceid2grade[cand_key] = grade
        sourceid2star[cand_key] = star
        sourceid2brand_tag[cand_key] = brand_tag
        sourceid2address[cand_key] = address
    return sourceid2mapInfo, sourceid2hotelNameEn, sourceid2hotelNameCh, sourceid2grade, sourceid2star

if __name__ == '__main__':

    print get_max_unid('127.0.0.1','merge_hotel_20160606','hotel_unid_20160512')
    
    #db_name = 'hotel_merge_beimei'
    #table_name = 'hotel_ori_data_beimei_20150527'

    #sourceid2mapInfo,sourceid2hotelNameEn,sourceid2hotelNameCh,sourceid2cid = \
    #       load_merged_mapInfo_and_hotelName()

    #unmerged_data = load_unmerged_data(sourceid2mapInfo,sourceid2hotelNameEn,sourceid2hotelNameCh,sourceid2cid)

    #load_merged_data('127.0.0.1','merge_hotel_20150706','hotel_unid')

    #load_unmerge_data()
