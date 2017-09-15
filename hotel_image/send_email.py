#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/9/15 上午9:16
# @Author  : Hou Rong
# @Site    : 
# @File    : send_email.py
# @Software: PyCharm
import sys
import requests


def send_email(title, mail_info, mail_list, want_send_html=False):
    try:
        mail_list = ';'.join(mail_list)
        data = {
            'mailto': mail_list,
            'content': mail_info,
            'title': title,
        }
        if want_send_html:
            data['mail_type'] = 'html'
        requests.post('http://10.10.150.16:9000/sendmail', data=data)
    except Exception as e:
        sys.stderr.write('Error Info:%s\n' % str(e))
        return False
    return True


if __name__ == '__main__':
    print(send_email("酒店图片状态更新", "当前进度", ["hourong@mioji.com", ]))
