#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/13 下午12:33
# @Author  : Hou Rong
# @Site    : 
# @File    : test_aio_mysql.py
# @Software: PyCharm
import asyncio

import aiomysql
from aiomysql.cursors import DictCursor


async def mysql_test(sql=''):
    pool = await aiomysql.create_pool(
        host='10.10.228.253', port=3306, user='mioji_admin',
        password='mioji1109', db='mysql'
    )
    async with pool.acquire() as conn:
        await conn.set_charset('utf8')
        async with conn.cursor(DictCursor) as cur:
            await cur.execute(sql)
            print(cur.rowcount)
            print(await cur.fetchone())
            print(await cur.fetchall())
            # 如果是插入或者修改则需要commit
            # await conn.commit()


test_sql = 'SELECT Host,User FROM user'
loop = asyncio.get_event_loop()
loop.run_until_complete(mysql_test(sql=test_sql))
loop.close()
