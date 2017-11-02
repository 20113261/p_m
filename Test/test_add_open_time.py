#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/11 下午10:13
# @Author  : Hou Rong
# @Site    : 
# @File    : test_add_open_time.py
# @Software: PyCharm
import unittest
from add_open_time.fix_daodao_time import fix_daodao_open_time


class DaodaoOpenTimeFixTest(unittest.TestCase):
    def test_case_1(self):
        self.assertSetEqual(set(fix_daodao_open_time('周日10:00 - 17:00周一 - 周三10:00 - 17:00').split('|')),
                            {'<*><周1-周3><10:00-17:00><SURE>', '<*><周7><10:00-17:00><SURE>'}
                            )

    def test_case_2(self):
        self.assertSetEqual(set(fix_daodao_open_time('周一 - 周六:下午7点00分 - 上午10点00分').split('|')),
                            {'<*><周1><19:00-23:59><SURE>',
                             '<*><周7><00:00-10:00><SURE>',
                             '<*><周2-周6><00:00-10:00,19:00-23:59><SURE>'})

    def test_case_3(self):
        self.assertSetEqual(
            set(fix_daodao_open_time('周二 - 周四:11:00 - 17:00|周五:11:00 - 21:00|周六 - 周日:11:00 - 17:00').split('|')),
            {'<*><周6-周7><11:00-17:00><SURE>',
             '<*><周2-周4><11:00-17:00><SURE>',
             '<*><周5><11:00-21:00><SURE>'})

    def test_case_4(self):
        self.assertSetEqual(
            set(fix_daodao_open_time('周一 - 周日:8:00 - 18:00').split('|')),
            {'<*><周1-周7><08:00-18:00><SURE>'}
        )

    def test_case_5(self):
        self.assertSetEqual(
            set(
                fix_daodao_open_time(
                    '周一 - 周二 11:00 - 14:00| 17:30 - 22:00|周三 5:30 - 22:00| 23:00 - 14:00|'
                    '周四 11:00 - 14:00| 17:30 - 22:00|周五 - 周日 17:30 - 22:30|'
                    '周五 11:30 - 14:00|周六 - 周日 11:30 - 14:30'
                ).split('|')),
            {'<*><周4><00:00-14:00,17:30-22:00><SURE>',
             '<*><周6-周7><11:30-14:30,17:30-22:30><SURE>',
             '<*><周1-周2><11:00-14:00,17:30-22:00><SURE>',
             '<*><周3><05:30-22:00,23:00-23:59><SURE>',
             '<*><周5><11:30-14:00,17:30-22:30><SURE>'}
        )

    def test_case_6(self):
        self.assertSetEqual(
            set(fix_daodao_open_time('周一 - 周四 10:00 - 22:00|周五 10:00 - 2:00|周六 8:00 - 2:00').split('|')),
            {'<*><周1-周4><10:00-22:00><SURE>',
             '<*><周5><10:00-23:59><SURE>',
             '<*><周6><00:00-02:00,08:00-23:59><SURE>',
             '<*><周7><00:00-02:00><SURE>'
             }
        )

    def test_case_7(self):
        self.assertSetEqual(
            set(fix_daodao_open_time('周一 - 周四 7:00 - 2:00|周五 16:00 - 2:00|周六 13:00 - 2:00').split('|')),
            {'<*><周7><00:00-02:00><SURE>',
             '<*><周1><07:00-23:59><SURE>',
             '<*><周6><00:00-02:00,13:00-23:59><SURE>',
             '<*><周2-周4><00:00-02:00,07:00-23:59><SURE>',
             '<*><周5><00:00-02:00,16:00-23:59><SURE>'}
        )

    def test_case_8(self):
        self.assertSetEqual(
            set(fix_daodao_open_time('周一至周日8:00-17:30。').split('|')),
            {'<*><周1-周7><08:00-17:30><SURE>'}
        )

    def test_case_9(self):
        # open_time = '12:00  - 18:00 ，周一歇业'
        pass

    def test_case_10(self):
        self.assertSetEqual(
            set(fix_daodao_open_time('星期日-星期六 10:00-19:00').split('|')),
            {'<*><周1-周7><10:00-19:00><SURE>'}
        )

    def test_case_11(self):
        self.assertSetEqual(
            set(fix_daodao_open_time('周一 - 周三10:00 - 17:00周四 - 周五10:00 - 21:00周六 - 周日10:00 - 17:00').split('|')),
            {'<*><周1-周3><10:00-17:00><SURE>',
             '<*><周4-周5><10:00-21:00><SURE>',
             '<*><周6-周7><10:00-17:00><SURE>'}
        )

    def test_case_12(self):
        self.assertSetEqual(
            set(fix_daodao_open_time(
                '星期日09:00 - 11:3018:00 - 19:00星期一 - 星期二08:00 - 17:00星期三08:00 - 19:30星期四08:00 - 17:00星期五08:00 - 16:30').split(
                '|')),
            {'<*><周7><09:00-11:30,18:00-19:00><SURE>',
             '<*><周3><08:00-19:30><SURE>',
             '<*><周4><08:00-17:00><SURE>',
             '<*><周1-周2><08:00-17:00><SURE>',
             '<*><周5><08:00-16:30><SURE>'}
        )

    def test_case_13(self):
        self.assertSetEqual(
            set(fix_daodao_open_time('星期日12:00 - 17:00星期三 - 星期六10:00 - 17:00').split('|')),
            {'<*><周3-周6><10:00-17:00><SURE>',
             '<*><周7><12:00-17:00><SURE>'}
        )

    def test_case_14(self):
        self.assertSetEqual(
            set(fix_daodao_open_time('星期三12:00 - 18:00星期五12:00 - 18:00').split('|')),
            {'<*><周3,周5><12:00-18:00><SURE>'}
        )

    def test_case_15(self):
        self.assertSetEqual(
            set(fix_daodao_open_time('18:00-23:00').split('|')),
            {'<*><周1-周7><18:00-23:00><SURE>'}
        )

    def test_case_16(self):
        self.assertSetEqual(
            set(fix_daodao_open_time('18:00-23:00，每周三休息').split('|')),
            {''}
        )

    def test_case_17(self):
        self.assertSetEqual(
            set(fix_daodao_open_time('每天13:00-21:00').split('|')),
            {'<*><周1-周7><13:00-21:00><SURE>'}
        )

    def test_case_18(self):
        self.assertSetEqual(
            set(fix_daodao_open_time('周一- 周五 10:00 - 23:00 周六、周日 11:00 - 23：00'
                                     ).split('|')),
            {'<*><周1-周5><10:00-23:00><SURE>',
             '<*><周6-周7><11:00-23:00><SURE>'}
        )

    def test_case_19(self):
        self.assertSetEqual(
            set(fix_daodao_open_time('周一至周六 10:00-23:00，周日14:00-24:00。'
                                     ).split('|')),
            {'<*><周1-周6><10:00-23:00><SURE>',
             '<*><周7><14:00-23:59><SURE>'}
        )

    def test_case_20(self):
        self.assertSetEqual(
            set(fix_daodao_open_time('周一-周日：12:00 - 24:00'
                                     ).split('|')),
            {'<*><周1-周7><12:00-23:59><SURE>'}
        )

    def test_case_21(self):
        self.assertSetEqual(
            set(fix_daodao_open_time('07:00 - 24:30'
                                     ).split('|')),
            {'<*><周1-周7><00:00-00:30,07:00-23:59><SURE>'}
        )

    def test_case_22(self):
        self.assertSetEqual(
            set(fix_daodao_open_time('周一至周五9.30-17:00，周六10:00-16:00'
                                     ).split('|')),
            {'<*><周6><10:00-16:00><SURE>',
             '<*><周1-周5><09:30-17:00><SURE>'}
        )

    def test_case_23(self):
        cases = ['永久', '全日开放', '全年24小时', '24hours', '全年常开', '完全开放', '周一-周日：24小时', '一直开放', '全日全时段', '每天',
                 '无限制',
                 '24小时参观游览。', '24h', '24小时', '整天', '全年开放。', '24hr']
        for each in cases:
            self.assertSetEqual(
                set(fix_daodao_open_time(each
                                         ).split('|')),
                {'<*><*><00:00-23:59><SURE>'}
            )



            # def test_case_23(self):
            #     self.assertSetEqual(
            #         set(fix_daodao_open_time('9:30-17:00，周四延长至21:00'
            #                                  ).split('|')),
            #         {'<*><周6><10:00-16:00><SURE>',
            #          '<*><周1-周5><09:30-17:00><SURE>'}
            #     )
            #
            # def test_case_24(self):
            #     self.assertSetEqual(
            #         set(fix_daodao_open_time('周一~周六：10:00~17:00，周日：12:00–17:00，关门前15分钟禁止入场'
            #                                  ).split('|')),
            #         {'<*><周6><10:00-16:00><SURE>',
            #          '<*><周1-周5><09:30-17:00><SURE>'}
            #     )

    def test_case_24(self):
        self.assertSetEqual(
            set(fix_daodao_open_time('周二至周日：09.30 -12.30h，14.30 -18.00 h。'
                                     ).split('|')),
            {'<*><周2-周7><09:30-12:30,14:30-18:00><SURE>'}
        )

    def test_case_25(self):
        self.assertSetEqual(
            set(fix_daodao_open_time('9.30am-4.30pm'
                                     ).split('|')),
            {'<*><周1-周7><09:30-16:30><SURE>'}
        )

    def test_case_26(self):
        self.assertSetEqual(
            set(fix_daodao_open_time('09：00-17：00'
                                     ).split('|')),
            {'<*><周1-周7><09:00-17:00><SURE>'}
        )

    def test_case_27(self):
        self.assertSetEqual(
            set(fix_daodao_open_time('7am-5pm'
                                     ).split('|')),
            {'<*><周1-周7><07:00-17:00><SURE>'}
        )

    def test_case_28(self):
        self.assertSetEqual(
            set(fix_daodao_open_time('10点-5点'
                                     ).split('|')),
            {'<*><周1-周7><10:00-17:00><SURE>'}
        )

    def test_case_29(self):
        self.assertSetEqual(
            set(fix_daodao_open_time('周二至周六 9am-6.30pm 周日11.30am-6.30pm'
                                     ).split('|')),
            {'<*><周2-周6><09:00-18:30><SURE>',
             '<*><周7><11:30-18:30><SURE>'}
        )

    def test_case_30(self):
        self.assertSetEqual(
            set(fix_daodao_open_time('周二到周天早上10点至晚5点'
                                     ).split('|')),
            {'<*><周2-周7><10:00-17:00><SURE>'}
        )

    def test_case_31(self):
        self.assertSetEqual(
            set(fix_daodao_open_time('8am~11pm'
                                     ).split('|')),
            {'<*><周1-周7><08:00-23:00><SURE>'}
        )

    def test_case_32(self):
        self.assertSetEqual(
            set(fix_daodao_open_time('周一~周六：10:00~17:00，周日：12:00–17:00，关门前15分钟禁止入场'
                                     ).split('|')),
            {'<*><周1-周6><10:00-17:00><SURE>',
             '<*><周7><12:00-17:00><SURE>'}
        )

    def test_case_33(self):
        self.assertSetEqual(
            set(fix_daodao_open_time('9:00至17:00, 365 Days of the year'
                                     ).split('|')),
            {'<*><周1-周7><09:00-17:00><SURE>'}
        )

    def test_case_34(self):
        self.assertSetEqual(
            set(fix_daodao_open_time('6:00-18:00，通常售票处下午5点停止售票'
                                     ).split('|')),
            {'<*><周1-周7><06:00-18:00><SURE>'}
        )

    def test_case_35(self):
        self.assertSetEqual(
            set(fix_daodao_open_time('周一至周日9:00-18:00，冬季提前1小时关门'
                                     ).split('|')),
            {'<*><周1-周7><09:00-18:00><SURE>'}
        )

    def test_case_36(self):
        self.assertSetEqual(
            set(fix_daodao_open_time('08:00-20:00，一周7天运行。'
                                     ).split('|')),
            {'<*><周1-周7><08:00-20:00><SURE>'}
        )

    def test_case_37(self):
        self.assertSetEqual(
            set(fix_daodao_open_time('9:30-17:00，周四延长至21:00'
                                     ).split('|')),
            {'<*><周1-周7><09:30-17:00><SURE>'}
        )


if __name__ == '__main__':
    unittest.main()
