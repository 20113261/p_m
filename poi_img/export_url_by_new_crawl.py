import pymysql
import pickle

if __name__ == '__main__':
    conn = pymysql.connect(host='localhost', user='hourong', passwd='hourong', charset='utf8', db='data_prepare')
    url_set = set()
    _count = 0
    with conn as cursor:
        cursor.execute('select image_list from restaurant_tmp')
        for line in cursor.fetchall():
            image_list = line[0]
            for img_url in image_list.strip().split('|'):
                url_set.add(img_url)
                _count += 1
                if _count % 10000 == 0:
                    print(_count)

    pickle.dump(url_set, open('/root/data/task/rest_img_task_170217.pk', 'wb'))
