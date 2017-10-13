import pymysql
import time
import os
import datetime
import jinja2
from .send_email import send_email

IMG_DICT = dict(host='10.10.228.253', user='mioji_admin', passwd='mioji1109', charset="utf8")
SQL_DICT = dict(host='10.10.189.213', user='hourong', passwd='hourong', charset="utf8")

mail_list = ["hourong@mioji.com", ]


def get_uid_set():
    _set = set()
    conn = pymysql.connect(db='onlinedb', **SQL_DICT)
    with conn as cursor:
        sql = 'select distinct uid from hotel'
        cursor.execute(sql)
        for line in cursor.fetchall():
            _set.add(line[0])
    conn.close()
    print('uid fetch OK')
    return _set


if __name__ == '__main__':
    uid_set = get_uid_set()
    conn = pymysql.connect(db='BaseDataFinal', **IMG_DICT)
    online_conn = pymysql.connect(db='onlinedb', **SQL_DICT)
    _count = 0
    data = []
    for uid in uid_set:
        online_cursor = online_conn.cursor()
        cursor = conn.cursor()
        online_cursor.execute('''SELECT
  source,
  sid
FROM hotel_unid
WHERE uid = %s;''', (uid,))

        start = time.time()
        img_result = []
        for each in online_cursor.fetchall():
            cursor.execute('''SELECT pic_md5
FROM hotel_images
WHERE source = %s AND source_id = %s GROUP BY file_md5;''', each)

            for line in cursor.fetchall():
                img_result.append(line[0])

        img_result = list(set(img_result))
        if img_result:
            data.append(('|'.join(img_result), img_result[0], uid))
        cursor.close()
        online_cursor.close()
        _count += 1
        if _count % 100 == 0:
            online_cursor = online_conn.cursor()
            online_cursor.executemany('update hotel set img_list=%s, first_img=%s where uid=%s', data)
            online_cursor.close()
            data = []
            print(_count)

        if _count % 50000 == 0:
            # 发送进度统计
            send_email("酒店图片状态更新", "当前进度: {0} / {1}".format(_count, len(uid_set)), mail_list=mail_list)

    # 最终入库
    online_cursor = online_conn.cursor()
    online_cursor.executemany('update hotel set img_list=%s, first_img=%s where uid=%s', data)
    online_cursor.close()

    # 最终状态
    send_email("酒店图片状态更新", "当前进度: {0} / {1}".format(_count, len(uid_set)), mail_list=mail_list)

    # 结束后统计
    online_cursor = online_conn.cursor()
    online_cursor.execute('''SELECT
      count(*)        AS total,
      sum(CASE WHEN first_img != ''
        THEN 1
          ELSE 0 END) AS first_img,
      sum(CASE WHEN img_list != ''
        THEN 1
          ELSE 0 END) AS img_list
    FROM hotel;''')
    total, first_img, img_list = online_cursor.fetchone()

    # 结束后成功统计
    send_email("酒店图片状态更新", "Total: {0}, FirstImg: {1}, ImgList: {2}".format(total, first_img, img_list),
               mail_list=mail_list)

    # 生成 table 名称

    table_name = datetime.datetime.now().strftime("hotel_with_img_%Y_%m_%d_%H.sql")

    # 导出酒店表
    os.system(
        '''/usr/bin/mysqldump -uhourong -phourong onlinedb hotel > /search/hourong/hotel/{0}'''.format(table_name))

    rsync_address = 'rsync 10.10.189.213::root/search/hourong/hotel/{0}'.format(table_name)

    # 最终统计结果

    content = "Total: {0}, FirstImg: {1}, ImgList: {2}\n".format(total, first_img, img_list)
    report_list = [
        {'key': 'Total', 'value': total},
        {'key': 'FirstImg', 'value': first_img},
        {'key': 'ImgList', 'value': img_list},
        {'key': 'RsyncAddress', 'value': rsync_address},

    ]
    report_template = jinja2.Template('''<table border="2">
    {% for report in report_list %}
        <tr>
            <td>{{ report.key }}</td>
            <td>{{ report.value }}</td>
        </tr>
    {% endfor %}
</table>''')

    report_html = report_template.render(report_list=report_list)
    send_email("酒店图片状态更新", report_html, mail_list=mail_list, want_send_html=True)

    print(_count)
