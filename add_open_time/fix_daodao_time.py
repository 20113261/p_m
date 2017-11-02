import datetime
import re
import unittest
from .multi_days_check import multi_days_handling

SKIP_KEY = ['休息', '月', '次日']

S2F = {
    '一': '周一',
    '二': '周二',
    '三': '周三',
    '四': '周四',
    '五': '周五',
    '六': '周六',
    '七': '周七',
    '日': '周七'
}

C2A = {'一': '一',
       '二': '二',
       '三': '三',
       '四': '四',
       '五': '五',
       '六': '六',
       '七': '七',
       '日': '七'}

E2CC = {
    '周一': '周1',
    '周二': '周2',
    '周三': '周3',
    '周四': '周4',
    '周五': '周5',
    '周六': '周6',
    '周七': '周7',
    '周日': '周7',
}

R2S = {'一:': '一',
       '二:': '二',
       '三:': '三',
       '四:': '四',
       '五:': '五',
       '六:': '六',
       '七:': '七',
       '日:': '七',
       '|:': '|'
       }

work_replace_key = {
    '周周': '周',
    ' ': '',
    '，': '',
    '。': '',
    '24:00': '23:59'
}


def process_day(day):
    day = list(day)
    for index, literal in enumerate(day):
        if literal in list(C2A.keys()):
            day[index] = C2A[literal]
    return ''.join(day)


def process_hour(hour, seq):
    if hour.startswith('上午12'):
        hour = list(hour[2:])
        if seq == 1 or (seq == 2 and (hour[-3] != '0' or hour[-2] != '0')):
            hour[0] = '0'
            hour[1] = '0'
        elif seq == 2:
            hour[0] = '2'
            hour[1] = '4'
        for index, elem in enumerate(hour):
            if elem == '点':
                hour[index] = ':'
            elif elem == '分':
                hour[index] = ''
        return ''.join(hour)
    elif hour.startswith('上午'):
        hour = list(hour[2:])
        if hour[0].isdigit() and not hour[1].isdigit():
            for index, elem in enumerate(hour):
                if elem == '点':
                    hour[index] = ':'
                elif elem == '分':
                    hour[index] = ''
            hour = '0' + ''.join(hour)
            return hour
        elif hour[0].isdigit() and hour[1].isdigit():
            for index, elem in enumerate(hour):
                if elem == '点':
                    hour[index] = ':'
                elif elem == '分':
                    hour[index] = ''
            return ''.join(hour)
    elif hour.startswith('下午12'):
        hour = list(hour[2:])
        for index, elem in enumerate(hour):
            if elem == '点':
                hour[index] = ':'
            elif elem == '分':
                hour[index] = ''
        return ''.join(hour)
    elif hour.startswith('下午'):
        hour = list(hour[2:])
        if hour[1].isdigit():
            t = list(str(int(''.join(hour[:2])) + 12))
            hour[0] = t[0]
            hour[1] = t[1]
        elif not hour[1].isdigit():
            t = list(str(int(''.join(hour[:1])) + 12))
            hour[0] = t[1]
            hour.insert(0, t[0])
        for index, elem in enumerate(hour):
            if elem == '点':
                hour[index] = ':'
            elif elem == '分':
                hour[index] = ''
        return ''.join(hour)


def not_str(s):
    return not isinstance(s, str) and not isinstance(s, str)


# get open_time_desc
# 周一 - 周四:上午10点00分 - 上午12点00分|周五 - 周日:上午10点00分 - 上午1点30分
def get_time_range(period):
    if period.lower() == 'null':
        return ""
    elif not period:
        return ""
    time_range_raws = period.split('|')
    t = []
    for time_range_raw in time_range_raws:
        if '周' not in time_range_raw:
            return ""
        time_range_raw = time_range_raw.strip().replace(' - ', '-').replace(' ', '')
        if time_range_raw.count('-') == 2:
            if ':' in time_range_raw:
                days, hours = time_range_raw.split(':')
            else:
                days = time_range_raw[:5]
                hours = time_range_raw[5:]
        elif time_range_raw.count('-') == 1:
            if ':' in time_range_raw:
                days, hours = time_range_raw.split(':')
            else:
                days = time_range_raw[:2]
                hours = time_range_raw[2:]
                #        try:
                #            days, hours = time_range_raw.split(':')
                #        except:
                #            print time_range_raw
                #            return ""
                # process days
                #        print time_range_raw,'\t', days,'\t', hours
        if '-' in days:
            #            print days
            day1, day2 = days.split('-')
            day1 = process_day(day1.strip())
            day2 = process_day(day2.strip())
            if not_str(day1) or not str(day2):
                print(time_range_raw)
                break
            days = '-'.join([day1, day2])
        else:
            days = process_day(days)
        # # hours have '-'
        hour1, hour2 = tuple(hours.split('-'))
        hour1 = process_hour(hour1, 1)
        hour2 = process_hour(hour2, 2)
        if not_str(hour1) or not str(hour2):
            print(time_range_raw)
            break
        hours = '-'.join([hour1, hour2])
        # hours
        time_range_raw = ' '.join([days, hours])
        t.append(time_range_raw)
    return '|'.join(t)


# get open_time from open_time_desc
def get_open_time(open_time_desc):
    # 周一-周二 10:30-6:00|周三 10:30-5:30|周四 10:30-7:00|周五 10:00-7:00|周六 10:30-9:00
    if not open_time_desc:
        return ""
    times = open_time_desc.split('|')
    t = []
    for time in times:
        #        print time
        time = time.replace("  ", " ")
        days, hours = tuple(time.split(' '))
        day = days.split('-')
        if len(day) == 2:
            day[0] = E2CC[day[0]]
            day[1] = E2CC[day[1]]
            days = '-'.join(day)
        elif len(day) == 1:
            days = E2CC[day[0]]
        t.append('<*><' + days + '><' + hours + '><SURE>')
    return '|'.join(t)


def standardized(src: str) -> str:
    for k, v in R2S.items():
        if k in src:
            src = src.replace(k, v)
    return src


def fix_time_digits(source_open_time):
    _res = []
    new_date = []
    new_time = []
    has_output_time = False
    until_output_time = True
    source_open_time = standardized(source_open_time)
    for date_or_time in re.findall("(周一|周四|周三|周六|周五|周七|周二|周日|[\d:]+)", source_open_time):
        if '周' in date_or_time:
            if has_output_time:
                new_date = []
                has_output_time = False
                until_output_time = True
            if len(new_date) == 2 and until_output_time:
                new_date = []
            if date_or_time in E2CC:
                new_date.append(E2CC[date_or_time])
                next_word = len(date_or_time) + source_open_time.find(date_or_time)
                if next_word < len(source_open_time) - 1:
                    if source_open_time[next_word] == '、':
                        until_output_time = False
            elif date_or_time in E2CC.values():
                new_date.append(date_or_time)
                next_word = len(date_or_time) + source_open_time.find(date_or_time)
                if next_word < len(source_open_time) - 1:
                    if source_open_time[next_word] == '、':
                        until_output_time = False
            else:
                raise TypeError('Unknown {}'.format(date_or_time))
        else:
            try:
                hour, minute = re.findall("(\d{1,2}):(\d{2})", date_or_time)[0]
            except Exception:
                continue
            if int(hour) >= 24:
                hour = int(hour) - 24
            date_or_time = "{}:{}".format(hour, minute)
            datetime.datetime.strptime(date_or_time, '%H:%M')
            new_time.append(date_or_time)
            if len(new_time) == 2:
                if until_output_time:
                    if len(new_date) == 2:
                        _res.append(
                            '<*><{0}-{1}><{2}-{3}><SURE>'.format(new_date[0], new_date[1], new_time[0], new_time[1]))
                    elif len(new_date) == 1:
                        _res.append(
                            '<*><{0}><{1}-{2}><SURE>'.format(new_date[0], new_time[0], new_time[1]))
                    has_output_time = True
                    new_time = []
                else:
                    for each_date in new_date:
                        _res.append(
                            '<*><{0}><{1}-{2}><SURE>'.format(each_date, new_time[0], new_time[1]))
                    has_output_time = True
                    new_time = []
    # if len(_res) != len(source_open_time.split('|')):
    #     raise TypeError('Error open time : {}'.format(source_open_time))
    return '|'.join(_res)


def split_sunday(b):
    a = [1, 2, 3, 4, 5, 6, 7]

    if b[0] > b[1]:
        return a[:b[1]], a[b[0] - 1:7]
    return b


def fix_sunday_open_time(src_open_time):
    result = []

    for each_time in src_open_time.split('|'):
        try:
            _, d, t, _ = re.findall("<([\s\S]+?)><([\s\S]+?)><([\s\S]+?)><([\s\S]+?)>", each_time)[0]
            s_key = list(map(lambda x: int(x), re.findall("周(\d)-周(\d)", d)[0]))
            d1, d2 = split_sunday(s_key)

            if isinstance(d1, list):
                if len(d1) == 1:
                    result.append("<*><周{}><{}><SURE>".format(d1[0], t))
                else:
                    result.append("<*><周{}-周{}><{}><SURE>".format(d1[0], d1[-1], t))

                if len(d2) == 1:
                    result.append("<*><周{}><{}><SURE>".format(d2[0], t))
                else:
                    result.append("<*><周{}-周{}><{}><SURE>".format(d2[0], d2[-1], t))
            else:
                result.append("<*><周{}-周{}><{}><SURE>".format(d1, d2, t))
        except Exception:
            result.append(each_time)

    return '|'.join(result)


def fix_daodao_open_time(source_open_time):
    # 休息字符无法解析，跳过
    if any(map(lambda x: x in source_open_time, SKIP_KEY)):
        return ''

    if source_open_time in ['永久', '全日开放', '全年24小时', '24hours', '全年常开', '完全开放', '周一-周日：24小时',
                            '一直开放', '全日全时段', '每天',
                            '无限制',
                            '24小时参观游览。', '24h', '24小时', '整天', '全年开放。', '24hr']:
        return '<*><*><00:00-23:59><SURE>'
    # 周替换
    __source_open_time = source_open_time.replace('星期', '周')

    # 到替换
    for key in ['到', '至']:
        __source_open_time = __source_open_time.replace(key, '-')

    # 中文 ： 转换
    __source_open_time = __source_open_time.replace('：', ':')

    # 换行符替换
    __source_open_time = __source_open_time.replace('\n', '|')
    # 数字替换
    for k, v in S2F.items():
        __source_open_time = __source_open_time.replace(k, v)

    __source_open_time = __source_open_time.replace('周天', '周七')

    for k, v in work_replace_key.items():
        while k in __source_open_time:
            __source_open_time = __source_open_time.replace(k, v)

    if '.' in __source_open_time:
        if __source_open_time.count(':') % 2 == 1:
            if (__source_open_time.count(':') + __source_open_time.count('.')) % 2 == 0:
                __source_open_time = __source_open_time.replace('.', ':')

    # 早上 晚上替换
    for each in re.findall('(早|早上|上午)(\d+)(点?)', __source_open_time):
        __source_open_time = __source_open_time.replace('{}{}{}'.format(*each), '{}:00'.format(each[1]))

    for each in re.findall('(下午|晚|晚上)(\d+)(点?)', __source_open_time):
        __source_open_time = __source_open_time.replace('{}{}{}'.format(*each),
                                                        '{}:00'.format(int(each[1]) + 12))

    # 加 |
    # 时间 星期
    try:
        for t, weekday in re.findall('(\d+:\d+)(周[\s\S])', __source_open_time):
            __source_open_time = __source_open_time.replace('{0}{1}'.format(t, weekday), '{0}|{1}'.format(t, weekday))
    except Exception:
        pass
    # |
    # 时间时间
    try:
        for t1, t2 in re.findall('(\d+:\d{2})(\d+:\d{2})', __source_open_time):
            __source_open_time = __source_open_time.replace('{0}{1}'.format(t1, t2), '{0}|{1}'.format(t1, t2))
    except Exception:
        pass

    # . 最终处理
    if '.' in __source_open_time:
        __source_open_time = __source_open_time.replace('.', ':')

    # h 处理
    if 'h' in __source_open_time:
        __source_open_time.replace('h', '')

    if 'am' in __source_open_time:
        for each in re.findall('(\d+:\d+)am', __source_open_time):
            __source_open_time = __source_open_time.replace('{}am'.format(each), each)
        for each in re.findall('(\d+)am', __source_open_time):
            __source_open_time = __source_open_time.replace('{}am'.format(each), '{}:00'.format(each))

    if 'pm' in __source_open_time:
        for each in re.findall('(\d+):(\d+)pm', __source_open_time):
            __source_open_time = __source_open_time.replace('{0}:{1}pm'.format(*each),
                                                            '{0}:{1}'.format(int(each[0]) + 12, each[1]))
        for each in re.findall('(\d+)pm', __source_open_time):
            __source_open_time = __source_open_time.replace('{}pm'.format(each), '{}:00'.format(int(each) + 12))

    # 单独点替换
    if '点' in __source_open_time and '分' not in __source_open_time:
        if __source_open_time.count('点') % 2 == 0:
            for _index, each in enumerate(re.findall('(\d+)点', __source_open_time)):
                __source_open_time = __source_open_time.replace('{}点'.format(each), '{}:00'.format(
                    int(each) + (0 if _index % 2 == 0 else 12)))

    final_time_desc = []
    # 预解析判定
    for each in __source_open_time.split('|'):
        try:
            if '周' not in each:
                _test_each = '周一-周七 {}'.format(each)
            else:
                _test_each = each

            try:
                open_time_desc = get_time_range(_test_each.strip())
                _res = get_open_time(open_time_desc)
            except:
                _res = fix_time_digits(_test_each.strip())

            if not _res:
                continue
            final_time_desc.append(each)
        except Exception:
            pass

    __source_open_time = '|'.join(final_time_desc)

    if '周' not in __source_open_time:
        __source_open_time = '周一-周七 {}'.format(__source_open_time)

    try:
        open_time_desc = get_time_range(__source_open_time.strip())
        src_open_time = get_open_time(open_time_desc)
    except:
        src_open_time = fix_time_digits(__source_open_time)

    fixed_sunday_open_time = fix_sunday_open_time(src_open_time)

    true_open_time = multi_days_handling(fixed_sunday_open_time)
    return true_open_time


if __name__ == '__main__':
    unittest.main()
    # open_time = '星期一  12:00 - 15:00 | 17:00 - 22:00'
    # open_time = '星期日 13:00 - 20:00星期一 - 星期五 16:00 - 22:00星期六 12:00 - 22:00'

    # open_time = '星期三 - 四13:00 - 20:00星期五13:00 - 22:00星期六12:00 - 18:00'
    # open_time = '星期三 - 四13:00 - 20:00星期五13:00 - 22:00星期六12:00 - 18:00'
    # open_time = '星期日13:00 - 17:00星期二 - 星期五09:00 - 17:00星期六10:00 - 17:00'

    # open_time = '星期二 - 六10:00 - 15:30'
    # open_time = '星期一 - 星期四09:00 - 12:0013:00 - 15:00'
    # open_time = '星期四  6:00 - 19:00'
    # open_time = '星期一  12:00 - 15:00 | 17:00 - 22:00\n星期二  12:00 - 15:00 | 17:00 - 22:00\n星期三  12:00 - 15:00 | 17:00 - 22:00\n星期四  12:00 - 15:00 | 17:00 - 22:00\n星期五  12:00 - 15:00 | 17:00 - 23:00\n星期六  12:00 - 15:00 | 17:00 - 23:00'
    # open_time = '星期日09:00 - 11:3018:00 - 19:00星期一 - 星期二08:00 - 17:00星期三08:00 - 19:30星期四08:00 - 17:00星期五08:00 - 16:30'

    # import pyopening_hours
    # open_time = '开放时间：全天'
    # open_time = '周一 - 周六:上午9点00分 - 下午8点00分'
    # print(fix_time_digits_2(open_time))
    # print('open_time', fix_daodao_open_time(open_time))
    pass
