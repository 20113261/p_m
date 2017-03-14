# coding=utf-8
from __future__ import absolute_import

import json
import re

import db_localhost
from lxml import html


def image_paser(content, url):
    dom = html.fromstring(content)
    result = ''
    if len(dom.find_class('flexible_photo_wrap')) != 0:
        try:
            raw_text = dom.find_class('flexible_photo_album_link')[0].find_class('count')[0].xpath('./text()')[0]
            image_num = re.findall('\((\d+)\)', raw_text)[0]
            pattern = re.compile('var lazyImgs = ([\s\S]+?);')
            google_list = []
            img_url_list = []
            if len(pattern.findall(content)) != 0:
                for each_img in json.loads((pattern.findall(content)[0]).replace('\n', '')):
                    img_url = each_img[u'data']
                    if 'ditu.google.cn' in img_url:
                        google_list.append(img_url)
                    elif 'photo-' in img_url:
                        img_url_list.append(img_url.replace('photo-l', 'photo-s').replace('photo-f', 'photo-s'))

            result = '|'.join(img_url_list[:int(image_num)])
            print 'image_urls: ', result
        except:
            try:
                image_num = len(dom.find_class('flexible_photo_wrap'))
                pattern = re.compile('var lazyImgs = ([\s\S]+?);')
                google_list = []
                img_url_list = []
                if len(pattern.findall(content)) != 0:
                    for each_img in json.loads((pattern.findall(content)[0]).replace('\n', '')):
                        img_url = each_img[u'data']
                        if 'ditu.google.cn' in img_url:
                            google_list.append(img_url)
                        elif 'photo-' in img_url:
                            img_url_list.append(img_url.replace('photo-l', 'photo-s').replace('photo-f', 'photo-s'))
                result = '|'.join(img_url_list[:image_num])
            except:
                pass
    return result


def insert_db(args):
    sql = 'insert into image_recrawl (`mid`,`url`,`img_list`) VALUES (%s,%s,%s)'
    return db_localhost.ExecuteSQL(sql, args)


if __name__ == '__main__':
    # import requests
    # import codecs
    #
    mid = 'vaaaaa'
    target_url = 'http://www.tripadvisor.cn/Attraction_Review-g298171-d1310935-Reviews-Gora_Park-Hakone_machi_Ashigarashimo_gun_Kanagawa_Prefecture_Kanto.html'
    # headers = {
    #     'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36'
    # }
    # page = requests.get(target_url, headers=headers)
    # codecs.open('page.html', 'w', encoding='utf8').write(page.text)

    content = open('page.html').read()
    img_list = image_paser(content, target_url)
    print insert_db((mid, target_url, img_list))
