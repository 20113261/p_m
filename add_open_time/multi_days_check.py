# coding=utf-8
import datetime
import re
from collections import defaultdict

pattern = re.compile('<([\s\S]*?)>')
day_list = ['周1', '周2', '周3', '周4', '周5', '周6', '周7']


def next_day(day):
    if day == '周7':
        return '周1'
    else:
        return day_list[day_list.index(day) + 1]


def to_min(clock):
    h_m = clock.split(':')
    return int(h_m[0]) * 60 + int(h_m[1])


def to_clock(mins):
    return "%02d:%02d" % divmod(mins, 60)


def get_day_time_dict(source):
    """
    将传入的开门时间小于关门时间的opentime字符串转换为星期为key,每日的开关门时间的list为value的字典
    :type source: str
    :param source: 传入时间字符串
    :return: dict
    """
    day_time_dict = {}
    for open_time in source.split('|'):
        info = pattern.findall(open_time)
        if '-' in info[1]:
            days = info[1].split('-')
            for day_index in range(day_list.index(days[0]), day_list.index(days[1]) + 1):
                day = day_list[day_index]
                if day not in day_time_dict:
                    day_time_dict[day] = []
                for raw_hours in info[2].split(','):
                    # raw_hours = info[2].replace('24:00', '12:00')
                    raw_hours = raw_hours.replace('24:00', '23:59')
                    if raw_hours == '':
                        continue
                    hours = raw_hours.split('-')
                    if int(hours[0].split(':')[0]) >= 24:
                        hours[0] = "%02d:%s" % (int(hours[0].split(':')[0]) - 24, hours[0].split(':')[1])
                    if int(hours[1].split(':')[0]) >= 24:
                        hours[1] = "%02d:%s" % (int(hours[1].split(':')[0]) - 24, hours[1].split(':')[1])
                    if datetime.datetime.strptime(hours[0], '%H:%M') == datetime.datetime.strptime(hours[1], '%H:%M'):
                        continue
                    if datetime.datetime.strptime(hours[0], '%H:%M') > datetime.datetime.strptime(hours[1], '%H:%M'):
                        if next_day(day) not in day_time_dict:
                            day_time_dict[next_day(day)] = []
                        day_time_dict[day].append(hours[0] + '-23:59')
                        day_time_dict[next_day(day)].append('00:00-' + hours[1])
                    else:
                        day_time_dict[day].append(raw_hours)
        else:
            day = info[1]
            if day not in day_time_dict:
                day_time_dict[day] = []
            for raw_hours in info[2].split(','):
                # raw_hours = info[2].replace('24:00', '12:00')
                raw_hours = raw_hours.replace('24:00', '23:59')
                if raw_hours == '':
                    continue
                hours = raw_hours.split('-')
                if int(hours[0].split(':')[0]) >= 24:
                    hours[0] = "%02d:%s" % (int(hours[0].split(':')[0]) - 24, hours[0].split(':')[1])
                if int(hours[1].split(':')[0]) >= 24:
                    hours[1] = "%02d:%s" % (int(hours[1].split(':')[0]) - 24, hours[1].split(':')[1])
                if datetime.datetime.strptime(hours[0], '%H:%M') == datetime.datetime.strptime(hours[1], '%H:%M'):
                    continue
                if datetime.datetime.strptime(hours[0], '%H:%M') > datetime.datetime.strptime(hours[1], '%H:%M'):
                    if next_day(day) not in day_time_dict:
                        day_time_dict[next_day(day)] = []
                    day_time_dict[day].append(hours[0] + '-23:59')
                    day_time_dict[next_day(day)].append('00:00-' + hours[1])
                else:
                    day_time_dict[day].append(raw_hours)
    return day_time_dict


def times_merge(times):
    """
    将传入的时间片段合并
    :type times: list
    :param times: 传入的时间片段,开关门时间,为int
    :return: 可迭代的tuple时间对象对象
    """
    # 给最小值为初值
    try:
        saved = list(sorted(times, key=lambda x: x[0])[0])
    except:
        saved = [0, 0]
    for start, end in sorted(times, key=lambda x: x[0])[1:]:
        # 若saved包含新增时间则扩展长度
        if start <= saved[1] and end >= saved[0]:
            saved[1] = max(saved[1], end)
        # 否则输出当前值,并初始化saved
        else:
            yield tuple(saved)
            saved[0] = start
            saved[1] = end
    yield tuple(saved)


def hours_list_merge(hours_list):
    """
    将有交叉的时间片段合成为完整的时间
    :type hours_list: list[str]
    :param hours_list: 传入的时间列表
    :return: list
    """
    return list([to_clock(a[0]) + "-" + to_clock(a[1]) for a in
                 times_merge([([to_min(y) for y in x.split('-')]) for x in hours_list])])


def hours_merge(day_time_dict):
    """
    合并传入的dict中的连续时间
    :type day_time_dict: dict{str:list}
    :param day_time_dict: key为星期,value为时间list的字典
    :return: dict
    """
    new_day_time_dict = {}
    for (k, v) in list(day_time_dict.items()):
        new_day_time_dict[k] = hours_list_merge(v)
    return new_day_time_dict


def merged_key(keys):
    tmp = []
    result = []
    for i in sorted(keys):
        if len(tmp) > 0:
            if i - tmp[-1] == 1:
                tmp.append(i)
            else:
                result.append(tmp)
                tmp = [i]
        else:
            tmp.append(i)
    result.append(tmp)

    per_key = []
    for i in result:
        if len(i) == 1:
            per_key.append(i[0])
        else:
            res = "周{}-周{}".format(i[0], i[-1])
            if res:
                yield res
    res = ','.join(map(lambda x: '周' + str(x), per_key))
    if res:
        yield res


def output_str(day_time_dict):
    """
    将day_time_dict字典转换为open_time字符串
    :type day_time_dict: dict
    :param day_time_dict: 传入字典
    :return: open_time字符串
    """
    res_list = []
    for k, v in list(day_time_dict.items()):
        res_list.append("<*><%s><%s><SURE>" % (k, ','.join(v)))
    return '|'.join(res_list)


def day_merge(day_time_dict: dict):
    merged_dey = defaultdict(list)

    for k, v in day_time_dict.items():
        merged_dey[tuple(v)].append(int(k[1]))

    result = {}
    for v, key in merged_dey.items():
        for new_key in merged_key(key):
            result[new_key] = list(v)
    return result


def multi_days_handling(source, closed):
    # 分割为每天时间
    day_time_dict = get_day_time_dict(source)
    # 关闭日期
    day_time_dict = {k: v for k, v in day_time_dict.items() if k not in closed}
    # 合并小时
    day_time_dict = hours_merge(day_time_dict)
    # 合并日期
    day_time_dict = day_merge(day_time_dict)
    return output_str(day_time_dict)


if __name__ == '__main__':
    case_1 = '<*><周2-周6><08:30-01:00><SURE>'
    case_2 = '<*><周2-周4><23:00-01:00><SURE>|<*><周5-周6><23:00-02:00><SURE>'
    case_3 = '<*><周1-周4><14:00-23:00><SURE>|<*><周5><12:00-23:00><SURE>|<*><周6><12:00-01:00><SURE>'
    case_4 = '<*><周1-周4><18:00-22:30><SURE>|<*><周1-周5><11:30-15:00><SURE>|<*><周5><18:00-23:30><SURE>|<*><周6-周7><12:00-11:30><SURE>'
    case_5 = '<*><周1><17:00-23:30><SURE>|<*><周2><17:00-23:00><SURE>|<*><周3><12:00-23:00><SURE>|<*><周4-周5><12:00-01:00><SURE>|<*><周6><14:30-01:00><SURE>'
    case_6 = '<*><周2><00:00-22:30,23:59-23:59><SURE>|<*><周3><00:00-22:30,23:59-23:59><SURE>|<*><周1><00:00-22:30,23:59-23:59><SURE>|<*><周6><00:00-23:30,23:59-23:59><SURE>|<*><周7><00:00-23:30><SURE>|<*><周4><00:00-22:30,23:59-23:59><SURE>|<*><周5><00:00-22:30,23:59-23:59><SURE>|<*><周7><23:59-23:59><SURE>'
    case_7 = '<*><周2-周6><16:00-04:00><SURE>|<*><周7><><SURE>'
    case_8 = '<*><周2><11:00-23:30><SURE>|<*><周3><11:00-23:30><SURE>|<*><周1><11:00-23:30><SURE>|<*><周6><24:30-12:30><SURE>|<*><周7><24:30-23:30><SURE>|<*><周4><11:00-23:30><SURE>|<*><周5><11:00-12:30><SURE>'
    case_9 = '<*><周1><04:00-01:00><SURE>|<*><周2-周4><04:00-02:00><SURE>|<*><周5><><SURE>'
    case_10 = '<*><周2><11:30-14:30,17:00-22:00><SURE>|<*><周3><11:30-14:30,17:00-22:00><SURE>|<*><周6><11:30-15:00,17:30-23:00><SURE>|<*><周7><24:30-22:00><SURE>|<*><周4><11:30-14:30,17:00-22:00><SURE>|<*><周5><11:30-15:00,17:30-23:00><SURE>'
    case_11 = '<*><周2><11:30-14:30,17:00-22:00><SURE>|<*><周3><11:30-14:30,17:00-22:00><SURE>|<*><周6><11:30-15:00,17:30-23:00><SURE>|<*><周7><24:30-22:00><SURE>|<*><周4><11:30-14:30,17:00-22:00><SURE>|<*><周5><11:30-15:00,17:30-23:00><SURE>'
    # day_time_dict = get_day_time_dict(case_4)
    # # print output_str(day_time_dict)
    # # for k, v in day_time_dict.items():
    # #     print k, '--->', sorted(v)
    # day_time_dict = hours_merge(day_time_dict)
    # # print
    # print output_str(day_time_dict)
    # for k, v in day_time_dict.items():
    #     print k, '--->', sorted(v)

    print(multi_days_handling(case_10, []))
