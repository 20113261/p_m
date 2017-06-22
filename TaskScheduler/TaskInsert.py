#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/6/19 下午3:41
# @Author  : Hou Rong
# @Site    : 
# @File    : TaskInsert.py
# @Software: PyCharm

import hashlib
import json
import pymysql
import time

# 当 task 数积攒到每多少时进行一次插入
# 当程序退出后也会执行一次插入，无论当前 task 积攒为多少

INSERT_WHEN = 2000

# 入库语句

INSERT_SQL = 'INSERT IGNORE INTO Task (`id`, `worker`, `args`, `task_name`, `priority`) VALUES (%s, %s, %s, %s, %s)'


def order_and_sorted_dict(d: dict):
    return tuple(sorted([(k, order_and_sorted_dict(v)) if isinstance(v, dict) else(k, v) for k, v in d.items()],
                        key=lambda x: x[0]))


class Task(object):
    def __init__(self, worker, args, task_name, **kwargs):
        self.worker = worker
        self.args = args
        self.task_name = task_name

        if 'priority' in kwargs:
            self.priority = kwargs['priority']
        else:
            self.priority = '0'

        self.id = self.get_task_id()

    def get_task_id(self):
        if isinstance(self.args, dict):
            key_tuple = order_and_sorted_dict(self.args)
            return hashlib.md5((self.worker + str(key_tuple)).encode()).hexdigest()
        else:
            raise TypeError('错误的 args 类型 < {0} >'.format(type(self.args).__name__))


class TaskList(list):
    def append_task(self, task: Task):
        self.append((task.id, task.worker, json.dumps(task.args), task.task_name, task.priority))


class InsertTask(object):
    def __init__(self, worker, **kwargs):
        self.worker = worker

        if 'task_name' in kwargs:
            self.task_name = kwargs['task_name']
        else:
            self.task_name = worker + time.strftime("_%y_%m_%d_%H_%M")

        if 'conn_val' in kwargs:
            self.conn_val = kwargs['conn_val']
        else:
            self.conn_val = {
                'host': 'localhost',
                'user': 'hourong',
                'passwd': 'hourong',
                'db': 'Task',
                'charset': 'utf8'
            }

        self.conn = self.connect()
        self.tasks = TaskList()
        self.count = 0

    def connect(self):
        return pymysql.connect(**self.conn_val)

    def insert_into_database(self):
        cursor = self.conn.cursor()
        res = cursor.executemany(INSERT_SQL, self.tasks)
        cursor.close()
        task_length = len(self.tasks)

        self.count += task_length

        print('本次准备入库任务数：{0}\n实际入库数：{1}\n库中已有任务：{2}\n已完成总数：{3}\n'.format(task_length, res, task_length - res,
                                                                         self.count))

        # 入库完成，清空任务列表
        self.tasks = TaskList()

    def insert_stat(self):
        """
        用于检查当前是否可以准备将任务入到 mysql 中
        :return: bool 是否准备将任务入到 mysql 中
        """
        return len(self.tasks) >= INSERT_WHEN

    def insert_task(self, args):
        if isinstance(args, dict):
            __t = Task(self.worker, args, self.task_name)
            self.tasks.append_task(__t)

            # 如果当前可以入库，则执行入库
            if self.insert_stat():
                self.insert_into_database()
        else:
            raise TypeError('错误的 args 类型 < {0} >'.format(type(args).__name__))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.insert_into_database()


if __name__ == '__main__':
    args = {1: 123123, 3: {2: 232322, 1: 111111, 3: 4343}, 2: 222}
    t = Task(worker='abcTest', args=args, task_name='abcTest')
    print(t.get_task_id())
