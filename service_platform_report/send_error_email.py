#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/20 上午10:41
# @Author  : Hou Rong
# @Site    : 
# @File    : send_error_email.py
# @Software: PyCharm
import jinja2
import datetime
from pymysql.cursors import DictCursor
from service_platform_conn_pool import report_pool
from my_logger import get_logger
from hotel_image.send_email import send_email

logger = get_logger("send_error_info_email")


def send_error_report_email():
    conn = report_pool.connection()
    cursor = conn.cursor(cursor=DictCursor)
    cursor.execute('''SELECT
  `task_name`,
  `total`
                                                                                                AS 'real_total',
  `total` - `21+22+23` - `103` - `105`                                                          AS 'total',
  round((`0` + `29` + `106` + `107` + `109`) / (`total` - `21+22+23` - `103` - `105`) * 100, 2) AS 'right_percent',
  `0`                                                                                           AS 'right_have_data',
  `29` + `106` + `107` + `109`                                                                  AS 'right_none_data'
FROM service_platform_routine_source_type_by_day
WHERE date = '{}'
HAVING total > 1200 AND right_percent < 95;'''.format(datetime.datetime.now().strftime("%Y%m%d")))
    data = []
    for each in cursor.fetchall():
        logger.debug(
            "[error_info][task_name: {task_name}][right_percent: {right_percent}][real_total: {real_total}][total: {total}][right_have_data: {right_have_data}][right_none_data: {right_none_data}]".format(
                **each))
        data.append(each)
    template = jinja2.Template("""<!DOCTYPE html>
<html>
<head>
    <title>成功率小于 95% 的任务</title>
    <style type="text/css">
        .responstable {
            margin: 1em 0;
            width: 100%;
            overflow: hidden;
            background: #FFF;
            color: #024457;
            border-radius: 10px;
            border: 1px solid #167F92;
        }

        .responstable tr {
            border: 1px solid #D9E4E6;
        }

        .responstable tr:nth-child(odd) {
            background-color: #EAF3F3;
        }

        .responstable th {
            display: none;
            border: 1px solid #FFF;
            background-color: #167F92;
            color: #FFF;
            padding: 1em;
        }

        .responstable th:first-child {
            display: table-cell;
            text-align: center;
        }

        .responstable th:nth-child(2) {
            display: table-cell;
        }

        .responstable th:nth-child(2) span {
            display: none;
        }

        .responstable th:nth-child(2):after {
            content: attr(data-th);
        }

        @media (min-width: 480px) {
            .responstable th:nth-child(2) span {
                display: block;
            }

            .responstable th:nth-child(2):after {
                display: none;
            }
        }

        .responstable td {
            display: block;
            word-wrap: break-word;
            max-width: 7em;
        }

        .responstable td:first-child {
            display: table-cell;
            text-align: center;
            border-right: 1px solid #D9E4E6;
        }

        @media (min-width: 480px) {
            .responstable td {
                border: 1px solid #D9E4E6;
            }
        }

        .responstable th, .responstable td {
            text-align: left;
            margin: .5em 1em;
        }

        @media (min-width: 480px) {
            .responstable th, .responstable td {
                display: table-cell;
                padding: 1em;
            }
        }

        body {
            padding: 0 2em;
            font-family: Arial, sans-serif;
            color: #024457;
            background: #f2f2f2;
        }

        h1 {
            font-family: Verdana;
            font-weight: normal;
            color: #024457;
        }

        h1 span {
            color: #167F92;
        }

    </style>
</head>
<body>
<table class="responstable" border="2px">
    <tr>
        <th>任务名</th>
        <th>任务成功百分率</th>
        <th>发出全量任务</th>
        <th>去除未知错误的全量任务</th>
        <th>正确，有数据返回</th>
        <th>正确，无数据返回</th>
    </tr>
    {% for d in data %}
        <tr>
            <td>{{ d.task_name }}</td>
            <td>{{ d.right_percent }}%</td>
            <td>{{ d.real_total }}</td>
            <td>{{ d.total }}</td>
            <td>{{ d.right_have_data }}</td>
            <td>{{ d.right_none_data }}</td>
        </tr>
    {% endfor %}
</table>
</body>
</html>
""")
    html = template.render(data=data)
    send_email("正确率小于 95% 的错误任务统计", html, ["zhangxiaopeng@mioji.com", "luwanning@mioji.com", "cuixiyi@mioji.com"],
               want_send_html=True)
    cursor.close()
    conn.close()


if __name__ == '__main__':
    send_error_report_email()
