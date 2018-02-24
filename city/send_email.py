#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/8/24 上午9:38
# @Author  : Hou Rong
# @Site    :
# @File    : hotel_list_routine_tasks.py
# @Software: PyCharm

import requests
import sys

SEND_TO = ['luwanning@mioji.com', 'cuixiyi@mioji.com']

def send_mail(title, content, mail_list, need_qq=False):
    mailapi = "http://10.10.150.16:9000/sendmail"
    mail_list = ';'.join(mail_list)
    mail_data = {"content": content,
                 "title": title,
                 "mailto": mail_list,
                 "level": 0
                 }
    if need_qq:
        mail_data['qq'] = 1
    mail_r = requests.post(mailapi, data=mail_data)
    r_code = mail_r.status_code
    print("[mail][%s][send alert mail]" % (r_code))


if __name__ == '__main__':
    send_mail("123q", "这是一封测试邮件", ["luwanning@mioji.com"])