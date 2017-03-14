# coding=utf-8
import db_add  # 本地数据库，存basic表
import db_img  # 10.10.189.213上的update_img.attr_bucket_relation表，即图片关系表: 图片名称，图片对应的miaoji_id
import hashlib
import os
import ConfigParser
import is_complete_scale_ok as img_filter  # 图片过滤程序

ids1 = []
ids2 = []

# --------- 需要改动的变量  --------------
IS_RENAME = True
IS_DEBUG = False
unit = 'attr_img'
# ------------ END -----------

config = ConfigParser.ConfigParser()
config.read('rename.ini')
DIR_IMG = config.get(unit, 'dir_img')
TASK_TABLE = config.get(unit, 'task_table')
CATE = config.get(unit, 'category')
BASIC_TABLE = config.get(unit, 'basic_table')
RELATION_TABLE = CATE + '_bucket_relation'
BUCKET_NAME = 'mioji-' + CATE


def get_url_by_md5():
    sql = 'select url,md5_name from ' + TASK_TABLE
    # 从本地数据库读取的图片抓取任务表
    result = db_add.QueryBySQL(sql)
    dic = {}
    for i in result:
        url = i['url']
        md5_name = i['md5_name']
        dic[md5_name] = url
    return dic


def get_mid_by_url():
    img_column = 'image_list'
    sql = 'select * from ' + BASIC_TABLE + ' where ' + img_column + '!=\'\' and ' + img_column + ' is not null'
    result = db_add.QueryBySQL(sql)
    dic = {}
    for i in result:
        # mid = i['miaoji_id']
        mid = i['id']
        urls = i[img_column]
        for u in urls.split('|'):
            url = u.strip().encode('utf-8')
            # 判断是否之前有过该键，如果有过，则跳过，不能覆盖原来的值
            dic[url] = mid
    return dic


URL_MD5_DICT = get_url_by_md5()
MID_URL_DICT = get_mid_by_url()


def get_md5(pic):
    # 生成md5值
    pic_file = open(pic).read()
    m = hashlib.md5()
    m.update(pic_file)
    file_md5 = m.hexdigest().strip().encode('utf-8')
    return file_md5


def get_size(filename):
    flag, y, x = img_filter.is_complete_scale_ok(filename)
    return flag, x, y


def get_new_name(args):
    sql = 'select * from %s where sid = \'%s\'' % args
    result = db_img.QueryBySQL(sql)
    num = len(result)
    if num != 0:
        nums = []
        for i in result:
            name = i['file_name'].strip().encode('utf-8')
            nums.append(int(name.split('_')[1].replace('.jpg', '')))
        num = max(nums)
    sid = args[1]
    new_name = sid + '_' + str(num + 1) + '.jpg'
    return new_name


def rename(md5_name, file_name):
    path1, path2 = DIR_IMG, DIR_IMG
    file_list = os.listdir(path1)
    old_name = md5_name + '.jpg'
    new_name = file_name
    command = 'mv ' + path1 + old_name + '  ' + path2 + new_name
    print command
    try:
        os.system(command)
        return True
    except Exception, e:
        print 'mv failed!!!', str(e)
        return False


def insert_db(args):
    sql = 'insert ignore into ' + args[
        0] + '(file_name,sid,url,bucket_name,pic_size,url_md5,pic_md5,source,`use`,status) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    return db_img.ExecuteSQL(sql, args[1:])


if __name__ in '__main__':

    f = open('/home/hourong/data/log/log_' + unit + '_rename.txt', 'a')
    os.chdir(DIR_IMG)
    file_list = os.listdir(DIR_IMG)
    count = 0
    rename_count = 0
    for img in file_list:
        if len(img) < 22:
            # count += 1
            continue
        print '------------------- count: %s ---------------' % count
        print 'image: %s' % img
        try:
            # 获取图片文件的md5值
            pic_md5 = get_md5(img)
            print 'pic_md5: %s' % pic_md5

            md5_name = img.split('.')[0]
            # 根据抓取任务表获得这张图片的来源URL,即图片URL
            url = URL_MD5_DICT[md5_name]
            print 'img_url: %s' % url
            if MID_URL_DICT.get(url, -1) == -1:
                count += 1
                continue
            # 根据source_id获取miaoji_id
            miaoji_id = MID_URL_DICT[url]
            print 'miaoji_id: %s' % miaoji_id

            # 读取文件，获取图片大小 #注意转换成string否则元组入库报错
            status = get_size(img)
            print 'pic status:', status

            # 只有当status[0]=0时，这张图片才是符合我们条件的,否则都筛选掉
            if status[0] != 0:
                count += 1
                ids2.append(miaoji_id)
                continue
            size = str(status[1:])
            print 'size: %s' % size

            # 获取该图片在线上的应该命名的图片,即miaoji_id_num.jpg
            file_name = get_new_name((RELATION_TABLE, miaoji_id))
            print 'file_name: %s' % file_name

            ids1.append(miaoji_id)

            # 重命名
            if IS_RENAME:
                if rename(md5_name, file_name):
                    rename_count += 1
                    f.write(img + '\t' + file_name + '\n')
                    # 入库,录到189.213的bucket_relation表中
                    data = (RELATION_TABLE, file_name, miaoji_id, url, BUCKET_NAME, size, md5_name, pic_md5, 'machine', '1',
                    'online')
                    print 'data:', data
                    print 'insert', insert_db(data), file_name
        except Exception, e:
            print 'rename error:', img
            print str(e)
        count += 1
        if IS_DEBUG:
            break
    f.close()
    print count, ' over'
    print rename_count, 'rename over'

    ids1 = list(set(ids1))
    ids2 = list(set(ids2))
    ids3 = list(set(ids2).difference(set(ids1)))
    print 'new pic:', len(ids1)
    print 'no pic:', len(ids3)
