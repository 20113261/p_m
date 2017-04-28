import pymysql

SQL_DICT = {
    'host': '10.10.114.35',
    'user': 'hourong',
    'password': 'hourong',
    'charset': 'utf8',
    'db': 'attr_merge'
}

conn = pymysql.connect(**SQL_DICT)
with conn as cursor:
    cursor.execute('select url from chat_attraction')
