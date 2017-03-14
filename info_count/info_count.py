import pymysql
import pandas
from sqlalchemy import create_engine


def get_sql_cols(ignore_column=None):
    if ignore_column is None:
        ignore_column = []

    column_name_sql = '''SELECT column_name
FROM information_schema.columns
WHERE table_schema = "{0}" AND table_name = "{1}"'''.format(SQL_DICT['db'], TABLE)
    conn = pymysql.connect(**SQL_DICT)

    count_sql_list = []
    cols = []
    with conn as cursor:
        cursor.execute(column_name_sql)
        for column_name in cursor.fetchall():
            if column_name in ignore_column:
                continue
            cols.append(column_name[0])
            count_sql_list.append(
                "SUM(CASE WHEN {0} != '' AND {0} != 'NULL' AND {0} IS NOT NULL THEN 1 ELSE 0 END) AS {0}_count".format(
                    column_name[0])
            )
        count_sql_list.append(
            "COUNT(*) AS TOTAL"
        )

        count_sql = '''SELECT {0} FROM {1}'''.format(','.join(count_sql_list), TABLE)
    conn.close()
    return count_sql, cols


def output_file(cols, check_sql):
    engine = create_engine(SQL_STR)
    df = pandas.read_sql(check_sql, engine)
    for col_name in cols:
        df[col_name + '_percent'] = df[col_name + '_count'] / df['TOTAL']

    if FILE_TYPE == 'csv':
        df.to_csv(FILE_PATH)
    elif FILE_TYPE == 'excel':
        df.to_excel(FILE_PATH)
    else:
        raise TypeError('pandas 的 IO 部分，有需要之后增加')


if __name__ == '__main__':
    # 连接数据库的相关信息
    SQL_DICT = {
        'host': '10.10.180.145',
        'user': 'hourong',
        'password': 'hourong',
        'charset': 'utf8',
        'db': 'hotel_adding'
    }
    # 需要产生报告的数据表
    TABLE = 'hotelinfo_static_data'
    # 报告导出的文件夹
    FILE_PATH = '/tmp/test_result.csv'
    # 忽略的 column
    ignore_column = ['continent', 'update_time']
    # 数据库连接字符串
    SQL_STR = 'mysql+pymysql://{user}:{password}@{host}/{db}?charset={charset}'.format(**SQL_DICT)
    # 文件类型
    FILE_TYPE = 'csv'

    check_sql, cols = get_sql_cols(ignore_column=ignore_column)
    output_file(cols=cols, check_sql=check_sql)
