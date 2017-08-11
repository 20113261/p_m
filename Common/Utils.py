#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/20 上午11:38
# @Author  : Hou Rong
# @Site    : 
# @File    : Utils.py
# @Software: PyCharm
import unittest
import re
from urllib.parse import urlparse


def is_legal(s):
    if s:
        if isinstance(s, str):
            if s.strip():
                if s.lower() != 'null':
                    return True
        elif isinstance(s, int):
            if s > -1:
                return True

        elif isinstance(s, float):
            if s > -1.0:
                return True
    return False


def modify_url(raw_url: str) -> str:
    if not bool(re.match(r'^https?:/{2}\w.+$', raw_url or '')):
        return ''

    parsed_obj = urlparse(raw_url.strip())

    parsed_link_prefix = '{0}://{1}{2}'.format(
        parsed_obj.scheme.strip(),
        parsed_obj.netloc.strip(),
        parsed_obj.path.strip(),
    )
    if parsed_obj.query:
        parsed_link = "{0}?{1}".format(parsed_link_prefix, parsed_obj.query.strip())
    else:
        parsed_link = parsed_link_prefix
    return parsed_link


class UtilTest(unittest.TestCase):
    def test_is_legal_false(self):
        self.assertEqual(is_legal(None), False)

    def test_is_legal_true(self):
        self.assertEqual(is_legal('test'), True)

    def test_modify_url(self):
        self.assertEqual(
            modify_url(
                'https://ihg.scene7.com/is/image/ihg/transparent_1440?fmt=png-alpha&wid=668&hei=385#asdfasdf#123123'),
            'https://ihg.scene7.com/is/image/ihg/transparent_1440?fmt=png-alpha&wid=668&hei=385')

    def test_modify_url_2(self):
        self.assertEqual(
            modify_url('asdfasdfasdfasdf'), ''
        )


if __name__ == '__main__':
    unittest.main()