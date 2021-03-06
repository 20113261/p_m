import urllib.request, urllib.parse, urllib.error

language_list = ["tr_TR", "ca_ES ", "th_TH", "sh_YU", "tr", "hu_HU", "lt", "th", "es_AR", "pt_PT", "ar_EG", "is_IS",
                 "zh_HK", "hr_HR", "pt_BR", "de", "cs_CZ", "sk_SK", "mk_MK", "lv_LV ", "et_EE", "zh_CN", "hr ", "el",
                 "en", "fr_FR", "en_CA", "fi_FI", "en_UK", "no_NO", "nl_NL", "en_US", "pl ", "et", "es", "ro_RO",
                 "iw_IL", "es_CL", "de_AT", "ro", "ru_RU", "be", "bg", "da_DK", "sq_AL", "ja", "ca", "fi", "cs",
                 "be_BY", "de_CH", "lv", "pt", "zh ", "ja_JP", "lt_LT", "sl_SI", "es_ES", "sl ", "bg_BG", "hu", "zh_TW",
                 "ar_AE", "es_BO", "mk ", "ar_DZ ", "it_CH", "uk", "is", "it_IT", "it", "iw", "nl_BE", "uk_UA",
                 "sv_SE ", "ar", "el_GR", "en_HK", "fr_BE", "nl", "no", "fr", "ru", "ko_KR", "de_LU", "fr_CH", "sr_YU",
                 "da", "fr_CA", "ar_BH ", "sr", "sq", "ko", "sv", "sk", "sh", "pl_PL"
                 ]
host_prefix = ['www']
host_postfix = ['com', 'cn']
similar_page_name = ['index.html', 'index.htm', 'index.shtml', 'default.html']


def get_host_rest(url):
    if url == '' or url is None:
        return ''
    proto, rest = urllib.parse.splittype(url)
    host, rest = urllib.parse.splithost(rest)
    if host is None:
        return get_host_rest('//' + rest)
    return host, rest


def get_modify_url(url):
    if url == '' or url is None:
        return ''
    host, rest = get_host_rest(url)
    if rest.endswith('#_=_'):
        rest.replace('#_=_', '')
    if rest.split('/')[-1] in similar_page_name:
        rest = '/'.join(rest.split('/')[:-1])
    if rest.endswith('/'):
        rest = rest[:-1]
    try:
        if rest.split('/')[1] in language_list:
            rest = '/'.join([''] + rest.split('/')[2:])
    except:
        pass
    if host.split('.')[0] in host_prefix:
        host = '.'.join(host.split('.')[1:])
    return host + rest


if __name__ == '__main__':
    tp_site_url = open('/tmp/tp_site_file').readlines()
    qyer_site_url = open('/tmp/qyer_site_file').readlines()
    tp_dict = {}
    qyer_dict = {}
    count = 0
    modify_url_set = set()
    for tp_url in tp_site_url:
        url = tp_url.strip()
        tp_dict[get_modify_url(url)] = url
    for qyer_url in qyer_site_url:
        url = qyer_url.strip()
        modify_url = get_modify_url(url)
        if modify_url in tp_dict:
            print(url)
            print(tp_dict[modify_url])
            print('\n')
            modify_url_set.add(modify_url)
            count += 1

    print(count)
    print(len(modify_url_set))
