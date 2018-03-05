#!/usr/bin/env python
# -*- coding: utf-8 -*-
from werkzeug.utils import secure_filename
from flask import Flask, request, jsonify, send_from_directory, render_template, abort
import os
import base64
import time
import zipfile
import pymysql
import subprocess

from city.add_city import read_file
from city.config import OpCity_config, base_path, config as temp_config
from MongoTask.crawl_all_source_suggest import create_task
from call_city_project.step_status import modify_status
from my_logger import get_logger
from city.find_hotel_opi_city import from_ota_get_city

basedir = os.path.abspath(os.path.dirname(__file__))
print(basedir)
app = Flask(__name__, static_folder=basedir)
UPLOAD_FOLDER='/search/service/nginx/html/MioaPyApi/store/upload'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
logger = get_logger('log', UPLOAD_FOLDER)

@app.route('/')
def main():
    return render_template('makePoihotelconfig.html')

@app.route('/upload', methods=['POST'], strict_slashes=False)
def upload_file():
    param = request.form.get('param')
    param = str(get_param())
    if not param:
        return jsonify({'status': False, 'message': '任务批次号为空'})
    file_dir = os.path.join(basedir, app.config['UPLOAD_FOLDER'], param)
    if not os.path.exists(file_dir):
        os.makedirs(file_dir)
    f = request.files['myfile']  # 从表单的file字段获取文件，myfile为该表单的name值
    if f and f.filename.endswith('.zip'):  # 判断是否是允许上传的文件类型
        fname = secure_filename(f.filename)
        logger.info('[upload][%s] tasks: %s' % (param, fname))
        ext = fname.rsplit('.', 1)[1]  # 获取文件后缀
        unix_time = int(time.time())
        new_filename = str(unix_time) + '.' + ext  # 修改了上传的文件名
        # zip_file_name = os.path.join(file_dir, new_filename)
        zip_file_name = os.path.join(file_dir, fname)
        f.save(zip_file_name)  # 保存文件到upload目录
        token = base64.b64encode(new_filename.encode('utf8'))
        logger.info('[upload][%s] token: %s' % (param, token))
        zip_path = os.path.join(file_dir, fname)

        zipfiles = zipfile.ZipFile(zip_path)
        zipfile_status = subprocess.Popen('unzip '+ fname, stdout=subprocess.PIPE, shell=True, cwd=file_dir)

        if zipfile_status.returncode:
            return jsonify({'status': False, 'message': '解压文件失败'})

        city_file = ''
        zip_files_list = zipfiles.filelist
        logger.info('[upload][%s] zipfiles: %s' % (param, zip_files_list))
        for child_file in zip_files_list:
            real_file_name = child_file.filename.encode('cp437').decode('utf8')
            if '新增城市' in real_file_name:
                city_file = os.path.join(file_dir, real_file_name)
                break
        if not city_file:
            return jsonify({'status': False, 'message': '无新增城市配置文件'})

        database_name = ''.join(['add_city_', param])
        temp_config['db'] = database_name
        collection_name, task_name = create_task(city_file, file_dir, database_name, param)
        tasks = modify_status('step3', param, [collection_name, task_name])
        from_ota_get_city(temp_config, param)
        logger.info('[upload][%s] tasks: %s' % (param, str(tasks)))

        return jsonify({"errno": 0, "errmsg": "上传成功", "token": token})
    else:
        return jsonify({"errno": 1001, "errmsg": "上传失败"})


@app.route('/download/<filename>', methods=['GET'])
def download_file(filename):
    if request.method == "GET":
        if os.path.isfile(os.path.join(UPLOAD_FOLDER+'/684/', filename+'.csv')):
            return send_from_directory(UPLOAD_FOLDER+'/684/', filename+'.csv', as_attachment=True)
        abort(404)

def get_param():
    conn = pymysql.connect(**OpCity_config)
    cursor = conn.cursor()
    sql = 'select max(id) from OpCity.city_order;'
    cursor.execute(sql, ())
    param = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return param+1

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=7698, debug=True)