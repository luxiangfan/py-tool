#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: luxiangfan

import sys
import urllib2
import json

compare_result = True
error_info = []
diff_info = {}


def compare(json_a, json_b):
    if type(json_a) != type(json_b):
        return False

    if type(json_a) == list:
        if len(json_a) != len(json_b):
            return False
        for i in range(len(json_a)):
            if not compare(json_a[i], json_b[i]):
                err_str = 'json_a:%s \njson_b: %s' % (json_a[i], json_b[i])
                error_info.append(err_str)
                return False
        return True

    elif type(json_a) == dict:
        for key in json_a:
            if not compare(json_a[key], json_b[key]):
                err_str = 'key1:%s value1:%s \nkey2:%s value2:%s' % (key, json_a[key], key, json_b[key])
                error_info.append(err_str)
                return False
        return True

    else:
        return json_a == json_b


# 改变json内容, 测试用
def change(rest):
    rest[0]['restaurant']['name'] = 'aaa'
    rest[0]['restaurant']['delivery_mode']['text'] = 'bbb'
    rest[1]['restaurant']['piecewise_agent_fee']['description'] = 'ccc'
    rest[2]['restaurant']['piecewise_agent_fee']['rules'][0]['price'] = 'ddd'


# 预处理餐厅json数据
def deal_rest_data(data_a, data_b):
    rest_map_a = dict()
    rest_map_b = dict()
    for i in range(len(data_a)):
        rest_a = data_a[i]['restaurant']
        rest_map_a[rest_a['id']] = rest_a

        rest_b = data_b[i]['restaurant']
        rest_map_b[rest_b['id']] = rest_b

    return rest_map_a, rest_map_b


# 对json中的数组元素进行递归排序
def sort_json(obj):
    if isinstance(obj, dict):
        for i, j in obj.iteritems():
            sort_json(j)

    if isinstance(obj, (tuple, list)):
        obj.sort()
        for i in obj:
            sort_json(i)


# 餐厅json比较入口
def compare_restaurant(data_a, data_b):
    rest_map_a, rest_map_b = deal_rest_data(data_a, data_b)

    flag = True
    not_cmp_rest = []
    for rest_id in rest_map_a:
        if rest_id not in rest_map_b.keys():
            not_cmp_rest.append(rest_id)
            continue

        if not do_compare(rest_map_a[rest_id], rest_map_b[rest_id], 'restaurant', rest_id):
            flag = False

    return flag, not_cmp_rest


# 递归比较餐厅内容
def do_compare(rest_a, rest_b, label, rest_id):
    if type(rest_a) != type(rest_b):
        return False

    flag = True

    if type(rest_a) == list:
        n = min(len(rest_a), len(rest_b))
        for i in range(n):
            current_label = '%s|%s' % (label, i)
            if not do_compare(rest_a[i], rest_b[i], current_label, rest_id):
                flag = False

    elif type(rest_a) == dict:
        for k in rest_a:
            current_label = '%s|%s' % (label, k)
            if not do_compare(rest_a[k], rest_b[k], current_label, rest_id):
                if type(rest_a[k]) != list and type(rest_a[k]) != dict:
                    err_str = '%s: %s \n%s: %s' % (current_label, rest_a[k], current_label, rest_b[k])
                    if rest_id not in diff_info.keys():
                        diff_info[rest_id] = []
                    err_list = diff_info[rest_id]
                    err_list.append(err_str)

                flag = False

    else:
        if rest_a != rest_b:
            flag = False

    return flag


def start_diff_restaurant(latitude, longitude):
    offset = 0
    limit = 5
    tag_id = -1

    url_a = 'http://mainsite-restapi.ele.me/shopping/v1/guess/likes?latitude=%s&longitude=%s&offset=%s&limit=%s&tag_id=%s' % (
        latitude, longitude, offset, limit, tag_id)
    url_b = 'http://dtopen.ele.me/hotfood/v1/guess/likes?latitude=%s&longitude=%s' % (latitude, longitude)

    data_a = json.loads(urllib2.urlopen(url_a).read())
    data_b = json.loads(urllib2.urlopen(url_b).read())

    # 对json里的list元素排序
    sort_json(data_a)
    sort_json(data_b)

    # 开始比较
    result, not_cmp_rest = compare_restaurant(data_a, data_b)

    print 'cmp_result:', result
    print 'not_cmp_rest:', not_cmp_rest
    if not result:
        for key in diff_info:
            print '***********************************'
            print 'rest_id:', key
            for info in diff_info[key]:
                print info, '\n'


if __name__ == '__main__':
    latitude = 31.2328
    longitude = 121.38164

    if len(sys.argv) >= 3:
        latitude = sys.argv[1]
        longitude = sys.argv[2]

    start_diff_restaurant(latitude, longitude)
