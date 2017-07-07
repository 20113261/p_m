#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/7/7 上午9:58
# @Author  : Hou Rong
# @Site    : 
# @File    : logger.py
# @Software: PyCharm
import logging.handlers

handler_multi = logging.handlers.WatchedFileHandler(
    '/tmp/multi_matched_log',
    mode='w'
)

handler_none = logging.handlers.WatchedFileHandler(
    '/tmp/none_matched_log',
    mode='w'
)
stream_handler = logging.StreamHandler()

logger_multi = logging.getLogger("CtripSuggestionMultiMatched")
logger_none = logging.getLogger("CtripSuggestionNoneMatched")

logger_multi.setLevel(level=logging.DEBUG)
logger_none.setLevel(level=logging.DEBUG)

logger_multi.addHandler(handler_multi)
logger_none.addHandler(handler_none)

# logger_multi.addHandler(stream_handler)
# logger_none.addHandler(stream_handler)
