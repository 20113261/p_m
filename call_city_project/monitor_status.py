#coding:utf-8

from apscheduler.schedulers.blocking import BlockingScheduler
import pymysql
import datetime
import traceback
import pymongo

from city.config import data_config, OpCity_config, base_path, config as temp_config
from call_city_project.step_status import modify_status, getStepStatus
from my_logger import get_logger
from call_city_project.report import make_poi_and_hotel_report, make_image_content_report, get_file
from call_city_project.step_status import modify_status
try:
    from call_city_project.city_step_seven import check_POI_data, update_mapinfo, analysis_result, success_report, dumps_sql, send_email_format
except:pass
from city.find_hotel_opi_city import from_ota_get_city

scheduler = BlockingScheduler()
logger = get_logger('monitor', base_path)


def update_step_report(csv_path,param,step_front,step_after,step_num):
    conn = pymysql.connect(**OpCity_config)
    cursor = conn.cursor()
    update_sql_front = "update city_order set report"+str(step_num)+"=%s,step"+str(step_num)+"=%s where id=%s"
    update_sql_after = "update city_order set step"+str(step_num+1)+"=%s where id=%s"
    try:
       cursor.execute(update_sql_front,(csv_path,step_front,param))
       cursor.execute(update_sql_after,(step_after,param))
       conn.commit()
    except Exception as e:
        conn.rollback()
    finally:
        conn.close()

def from_tag_get_tasks_status(name, flag=False):
    conn = pymysql.connect(**data_config)
    cursor = conn.cursor()
    sql_step_report = "select * from service_platform_product_mongo_report where tag=%s"
    sql_step_summary = "select * from serviceplatform_product_mongo_split_task_summary where task_name=%s"
    sql = sql_step_report if flag else sql_step_summary
    try:
        cursor.execute(sql, (name,))
        result = cursor.fetchall()
    finally:
        conn.close()
    return result

def step7_detection(tag):
    try:
        update_mapinfo(tag)
        qyer_report_result, _1, daodao_report_result, _2 = check_POI_data(tag)
        print(qyer_report_result)
        print(daodao_report_result)
        logger.info('[{0}]  qyer {1}'.format(tag, qyer_report_result))
        logger.info('[{0}]  daodao {1}'.format(tag, daodao_report_result))
        qyer_flag, qyer_report = analysis_result(qyer_report_result, 'qyer')
        daodao_flag, daodao_report = analysis_result(daodao_report_result, 'daodao')

        report = success_report(tag)
        check_report = '数据检测结果：\n' + qyer_report + '\n' + daodao_report + '\n\n' + report
        rsync_paths = []
        if qyer_flag and daodao_flag:
            for source in ['total', 'attr']:
                rsync_path = dumps_sql(tag, source)
            send_email_format(check_report, rsync_path)
        else:
            for source in ['total', 'attr']:
                rsync_paths.append(dumps_sql(tag, source))
            send_email_format(check_report, rsync_paths)
    except Exception as e:
        logger.error('================= ' + tag + ' ================= {}'.format(traceback.format_exc()))


def monitor_task_summary(step):
    stepa = 'step'+step
    logger.info('================= ' + stepa + ' ================= 开始')
    csvpath = ''
    tasks = getStepStatus(stepa)
    for param, values in tasks.items():
        if len(values) == 0: return
        if type(values[0]) is list:
            task_naems = list(zip(*values))[1]
        else:
            task_naems = [values[1]]
        the_progress_of = 0
        for task_name in task_naems:
            logger.info('{}, {}'.format(stepa, task_name))
            tasks_status = from_tag_get_tasks_status(task_name)
            logger.info('{}, {}'.format(stepa, tasks_status))
            line = tasks_status[0]
            t_all, t_done, t_failed = line[3], line[4], line[5]
            if t_all == t_done + t_failed:
                the_progress_of += 1

            if the_progress_of==len(task_naems):
                if step=='7':
                    tag = task_name.rsplit('_')[-1]
                    step7_detection(tag)
                    # if not get_file(param, 'poireport.csv'):
                elif step=='8':
                    if not make_image_content_report(t_all, t_done, t_failed, param):return
                    csvpath = '{}/merge_image_and_content.txt'.format(param)
                if step in ('4', '9'):
                    update_step_report(csvpath, param, 4, 0, int(step))
                elif step not in ('6', '7'):
                    update_step_report(csvpath, param, 1, 0, int(step))
                modify_status(stepa, param, flag=False)
                logger.info('================= ' + stepa + ' ================= 完成')
        logger.info('================= ' + stepa + ' ================= 1')

def monitor_report(step):
    stepa = 'step' + step
    logger.info('================= ' + stepa + ' ================= 开始')
    tasks = getStepStatus(stepa)
    for param, values in tasks.items():
        if len(values)==0:continue
        task_names = zip(*values).__next__()
        logger.info('{}, {}'.format(stepa, task_names))
        tag = str(task_names[0].rsplit('_', 1)[-1])
        logger.info('{}, {}'.format(stepa, tag))
        tasks_status = from_tag_get_tasks_status(tag, True)
        finaled_date = max(a[-1] for a in tasks_status)
        all_finaled_data = [a for a in tasks_status if a[-1]==finaled_date]
        logger.info('{}, {}'.format(stepa, tasks_status))
        if len(tasks_status) < len(task_names):
            logger.info('{}, {}'.format(stepa, '任务状态数 小于 任务已发任务数'))
            continue
        status_list_len = 0
        for (_0, source, _2, l_done, l_failed, _5, l_all, d_done, d_failed, d_all, i_done, i_failed, i_all, _13) in all_finaled_data:
            #规则 1 完成任务数 + 失败任务数 = 任务总数
            #规则 2 失败任务数 == 任务总数 发邮件报警
            if not (l_done+l_failed==l_all and d_done+d_failed==d_all and i_done+i_failed==i_all) or l_failed==l_all or d_failed==d_all:
                if source in ('Qyer', 'Daodao'):continue
                logger.info('{}, {}: {}'.format(stepa, '未完成', source))
                break
            else:
                logger.info('{}, {}: {}'.format(stepa, '已完成', source))
                status_list_len+=1
        if status_list_len>=len(task_names):
            modify_status(stepa, param, flag=False)
            logger.info('{}, 开始生成报表'.format(stepa))
            csv_file = make_poi_and_hotel_report(all_finaled_data, param)
            update_step_report(csv_file, param, 1, 0, int(step))
            logger.info('================= ' + stepa + ' ================= 完成')

    logger.info('================= ' + stepa + ' ================= 1')

def get_total_count(collection_name):
    client = pymongo.MongoClient(host='10.10.231.105')
    collection = client['MongoTask'][collection_name]
    total_count = collection.find({}).count()
    return total_count


def monitor_step3(stepa):
    step = 'step'+stepa
    tasks = getStepStatus(step)
    if len(tasks) == 0:return
    for param, collection_names in tasks.items():
        collection_name, task_name = collection_names
        total_count = get_total_count(collection_name)
        if int(total_count) == 0:
            return '0%'

        client = pymongo.MongoClient(host='10.10.231.105')
        collection = client['MongoTask'][collection_name]

        success_results = collection.find({
            'finished': 1,
            'used_times': {'$lt': 7},
            'task_name': task_name
        }, hint=[('task_name', 1), ('finished', 1), ('used_times', 1)])
        success_finish_num = success_results.count()

        failed_results = collection.find({
            'finished': 0,
            'used_times': 7,
            'task_name': task_name
        }, hint=[('task_name', 1), ('finished', 1), ('used_times', 1)])
        failed_finish_num = failed_results.count()

        logger.info('{0}, collections: {1}  total: {2}  success: {3}  failed: {4}'.format(step, collection_name, total_count, success_finish_num, failed_finish_num))
        if failed_finish_num>0 and failed_finish_num+success_finish_num==total_count:
            logger.info('{0}, {1} 失败'.format(step, collection_name))

        if success_finish_num == total_count:
            from_ota_get_city(temp_config, param)
            modify_status(step, param, flag=False)
            logger.info('{0}, {1} 成功'.format(step, collection_name))

    return format(success_finish_num/total_count, '.0%')


def local_jobs():
    # scheduler.add_job(monitor_report, 'date', args=('5',), next_run_time=datetime.datetime.now() + datetime.timedelta(seconds=2), id='test')
    # scheduler.add_job(monitor_step3,'cron',args=('3',),second='*/300',id='step3')
    scheduler.add_job(monitor_task_summary, 'cron', args=('4',), second='*/300', next_run_time=datetime.datetime.now() + datetime.timedelta(seconds=83), id='step4')
    scheduler.add_job(monitor_task_summary, 'cron', args=('9',), second='*/300', next_run_time=datetime.datetime.now() + datetime.timedelta(seconds=23), id='step9')
    scheduler.add_job(monitor_report, 'cron', args=('5',), second='*/300', id='step5')
    scheduler.add_job(monitor_task_summary, 'cron', args=('8',), second='*/300', id='step8')
    scheduler.add_job(monitor_task_summary, 'cron', args=('7',), second='*/300', id='step7')


if __name__ == '__main__':
    local_jobs()
    scheduler.start()