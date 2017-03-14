# coding=utf-8
import csv

if __name__ == '__main__':
    csv_file = open('/tmp/outlets.csv')
    sql_file = open('/tmp/outlets.sql', 'w')
    sql_file.write('set names utf8;\n')
    csv_file.readline()
    reader = csv.reader(csv_file)
    for line in reader:
        sql_string = 'update `chat_shopping` set name="%s",norm_tagid="%s",norm_tagid_en="%s" where id="%s";' % (
            str(line[1]), '奥特莱斯', 'Outlet', str(line[-1]))
        print(sql_string)
        sql_file.write(sql_string + '\n')
    sql_file.close()
    csv_file.close()
