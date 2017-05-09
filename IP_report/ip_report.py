import pymysql
from datetime import datetime
from collections import defaultdict

sql_dict = {
    'host': '10.10.180.145',
    'user': 'hourong',
    'password': 'hourong',
    'charset': 'utf8',
    'db': 'IP'
}

count_dict = defaultdict(list)
int_split = 30
if __name__ == '__main__':
    _count = 0
    conn = pymysql.connect(**sql_dict)
    with conn as cursor:
        # IP
        # cursor.execute('''SELECT
        #   ip_address,
        #   group_concat(u_time ORDER by u_time)
        # FROM ip_used
        # GROUP BY ip_address''')
        # local proxy
        cursor.execute('''SELECT
          local_proxy,
          group_concat(u_time ORDER by u_time)
        FROM ip_used
        GROUP BY local_proxy''')
        for line in cursor.fetchall():
            _count += 1
            if _count % 10000 == 0:
                print(_count)
            ip, times = line
            time_list = times.decode().split(',')
            if len(time_list) == 1:
                count_dict[-1].append(ip)

            else:
                for i in range(len(time_list) - 1):
                    try:
                        last = datetime.strptime(time_list[i + 1], '%Y-%m-%d %X')
                        first = datetime.strptime(time_list[i], '%Y-%m-%d %X')
                    except Exception:
                        continue

                    res = (last - first).seconds // 60

                    count_dict[res // int_split].append(ip)

        # print(count_dict)
        x_data = []
        y_data = []
        for k in sorted(count_dict.keys()):
            v = count_dict[k]
            if k == -1:
                print('1 times', len(v))
                x_data.append('一次')
                y_data.append(len(v))
            else:
                print(str(30 * k) + ' - ' + str(30 + 30 * k), len(v))
                x_data.append(str(30 * k) + ' - ' + str(30 + 30 * k))
                y_data.append(len(v))
    conn.close()
    print(x_data)
    print(y_data)
