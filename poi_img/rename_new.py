import os

import redis

import is_complete_scale_ok

redis_dict = redis.Redis(host='10.10.180.145', port=6379, db=10)
if __name__ == '__main__':
    path = '/search/image/img_url_1101_celery/'
    for file_name in os.listdir(path):
        flag, y, x = is_complete_scale_ok.is_complete_scale_ok(path + file_name)
        redis_dict.set(file_name + '_flag', '###'.join([str(flag), str(y), str(x)]))
        print flag, y, x
        # url_name = redis_dict.get(file_name)
        # source_id = redis_dict.get(url_name)
        # mid = redis_dict.get(source_id)
        # pic_count = redis_dict.get(mid)
        # if not pic_count:
        #     redis_dict.set(mid, '1')
        #     result_file_name = mid + '_1.jpg'
        # else:
        #     new_count = int(pic_count) + 1
        #     redis_dict.set(mid, str(new_count))
        #     result_file_name = mid + '_' + str(new_count) + '.jpg'
        # print file_name, result_file_name
