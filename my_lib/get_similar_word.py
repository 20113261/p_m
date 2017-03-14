# coding=utf-8
import re


def get_similar_word(source):
    similar_words = {
        'é': 'e',
        'ñ': 'n',
        '\'': '',
        ' ': '',
        '­': '',
        '’': '',
        '\"': '',
        '&': '',
        ')': '',
        '(': '',
        ',': '',
        '/': '',
        '.': '',
        ':': '',
        'É': '',
        'ß': 'B',
        'à': 'a',
        'â': 'a',
        'ä': 'a',
        'ç': 'c',
        'è': 'e',
        'ë': 'e',
        'ê': 'e',
        'î': 'i',
        'ô': 'o',
        'ö': 'o',
        'ü': 'u',
        '+': '',
        '-': ''
    }
    for k in similar_words:
        source = source.replace(k, similar_words[k])
    useless_words = re.findall('\(([\s\S]+?)\)', source)
    for each in useless_words:
        source = source.replace('(' + each + ')', '')
    return source.lower().strip()


if __name__ == '__main__':
    case = 'Chiesa di Santa Maria dei Greci'
    A = get_similar_word(case)
    case = 'Chiesa di Santa Maria dei Greci'
    B = get_similar_word(case)
    print(A)
    print(B)
    print(A == B)

    city_id = '10263'
    case = city_id + '|_|_|' + 'Tempio di Giunone'
    A = get_similar_word(case)
    case = 'Tempio di Giunone'
    B = city_id + '|_|_|' + get_similar_word(case)
    print(A)
    print(B)
    print(A == B)
