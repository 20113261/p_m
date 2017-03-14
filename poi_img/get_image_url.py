import hashlib
import os

url_list_file = ['attr_url_list_bak',
                 'rest_url_list_bak',
                 'shop_url_list_bak'
                 ]
for file_name in url_list_file:
    already_download_img = set(os.listdir('/search/image/' + file_name.replace('_bak', '') + '_celery'))
    url_set = set()
    for url in open('/tmp/' + file_name):
        url_set.add(url)
    f = open('/tmp/' + file_name.replace('_bak', ''), 'w')
    for url in url_set:
        if hashlib.md5(url.strip()).hexdigest() + '.jpg' not in already_download_img:
            f.write(url)
