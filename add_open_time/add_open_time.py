import pymysql

from fix_daodao_time import fix_daodao_open_time, get_open_time
from multi_days_check import multi_days_handling

# attr
OPEN = 'open'
OPEN_DESC = 'open_desc'
TASK_TABLE = 'data_prepare.attraction_tmp'

# shop
# OPEN = 'open'
# OPEN_DESC = 'open_desc'
# TASK_TABLE = 'data_prepare.shopping_tmp'


# rest
# OPEN = 'open_time'
# OPEN_DESC = 'open_time_desc'
# TASK_TABLE = 'data_prepare.restaurant_tmp'


def get_open_time(open_desc):
    if '<SURE>' in open_desc:
        open_time = open_desc
    else:
        try:
            open_time = fix_daodao_open_time(open_desc.replace('。', '').replace('到', '-').strip())
        except:
            try:
                open_time = get_open_time(open_desc.replace('。', '').replace('到', '-').strip())
            except:
                print(open_desc)
                open_time = ''
    if open_time != '':
        try:
            open_time = multi_days_handling(open_time)
        except:
            pass

    return open_time


def get_task():
    conn = pymysql.connect(host='localhost', user='hourong', passwd='hourong', charset='utf8', db='data_prepare')
    with conn as cursor:
        datas = []
        _count = 0
        sql = 'select id,{0} from {1}'.format(OPEN_DESC, TASK_TABLE)
        cursor.execute(sql)
        for line in cursor.fetchall():
            _count += 1
            miaoji_id = line['id']
            open_desc = line[OPEN_DESC]
            open_time = get_open_time(open_desc)
            data = (open_time, miaoji_id)
            datas.append(data)
            if _count % 30000 == 0:
                update_sql = 'update {0} set {1}=%s where id=%s'.format(TASK_TABLE, OPEN)
                print(_count)
                print('Total:', len(datas))
                print('Update', cursor.executemany(update_sql, datas))

        update_sql = 'update {0} set {1}=%s where id=%s'.format(TASK_TABLE, OPEN)
        print(_count)
        print('Total:', len(datas))
        print('Update', cursor.executemany(update_sql, datas))


if __name__ == '__main__':
    print("Updata:", get_task())
