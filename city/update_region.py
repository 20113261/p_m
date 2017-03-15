import pandas
import pymysql


def update_region(file_path):
    try:
        file = pandas.read_csv(file_path)
    except Exception as e:
        print("文件读取错误 " + str(e))
        return None

    conn = pymysql.connect(**SQL_DICT)

    with conn as cursor:
        for i in range(len(file)):
            line = file.irow(i)
            print('#' * 100)
            print(line['region'], line['country'], line['region_cn'], line['region_en'])
            print('Now', i)
            print('Update', cursor.execute("""UPDATE city
SET region_cn = '{region_cn}', region_en = '{region_en}'
WHERE country = '{country}' AND region = '{region}'""".format(**line)))
    conn.close()


if __name__ == '__main__':
    SQL_DICT = {
        'host': '10.10.154.38',
        'user': 'writer',
        'password': 'miaoji1109',
        'charset': 'utf8',
        'db': 'onlinedb'
    }
    file_path = '/tmp/region.csv'
    update_region(file_path)
