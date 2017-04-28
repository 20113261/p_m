from datetime import datetime


def test_case_1():
    test = b'2017-04-24 23:56:17,2017-04-25 01:15:04,2017-04-25 02:19:41,2017-04-25 02:56:49'
    print(
        (datetime.strptime(test.decode().split(',')[1], '%Y-%m-%d %X') - datetime.strptime(test.decode().split(',')[0],
                                                                                           '%Y-%m-%d %X')).seconds // 60)


def test_case_2():
    test = b'2017-04-24 23:56:17,2017-04-25 01:15:04,2017-04-25 02:19:41,2017-04-25 02:56:49'
    time_list = test.decode().split(',')
    for i in range(len(time_list) - 1):
        last = datetime.strptime(time_list[i + 1], '%Y-%m-%d %X')
        first = datetime.strptime(time_list[i], '%Y-%m-%d %X')

        res = (last - first).seconds // 60
        print(res, res // 30)


if __name__ == '__main__':
    test_case_2()
