import redis
import datetime
import dataset
from collections import defaultdict

if __name__ == '__main__':
    db = dataset.connect('mysql+pymysql://hourong:hourong@10.10.180.145/Task?charset=utf8')
    table = db['serviceplatform_task_summary']

    r = redis.Redis(host='10.10.180.145', db=3)
    dt = datetime.datetime.now()
    fail_count = defaultdict(int)
    success_count = defaultdict(int)
    key_set = set()
    for key in r.keys():
        task_type = 'NULL'
        task_source = 'NULL'
        key_list = key.decode().split('|_||_|')
        if len(key_list) not in (3, 5):
            continue
        if len(key_list) == 3:
            worker, local_ip, result = key_list
        elif len(key_list) == 5:
            worker, local_ip, task_source, task_type, result = key_list
        else:
            continue

        count = r.get(key)

        if result == 'failure':
            fail_count[(worker, local_ip, task_source, task_type)] = int(count)
        elif result == 'success':
            success_count[(worker, local_ip, task_source, task_type)] = int(count)

        key_set.add((worker, local_ip, task_source, task_type))

    for key in key_set:
        worker, local_ip, task_source, task_type = key
        data = {
            'worker_name': worker,
            'slave_ip': local_ip,
            'source': task_source,
            'type': task_type,
            'assigned': fail_count[key] + success_count[key],
            'completed': success_count[key],
            'date': datetime.datetime.strftime(dt, '%Y%m%d'),
            'hour': datetime.datetime.strftime(dt, '%H'),
            'datetime': datetime.datetime.strftime(dt, '%Y%m%d%H00')
        }

        try:
            table.insert(data, ensure=None)
        except Exception:
            pass

        print(worker, local_ip, task_source, task_type, fail_count[key] + success_count[key], success_count[key],
              datetime.datetime.strftime(dt, '%Y%m%d'),
              datetime.datetime.strftime(dt, '%H'), datetime.datetime.strftime(dt, '%Y%m%d%H00'))
        r.flushdb()
