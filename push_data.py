#!/usr/bin/python
# coding=utf-8

import os
import sys
import urllib2
import subprocess
import logging
import json
import csv
import xlrd
import redis

#####################################################################
#  使用方式：python push_es.py ${data_path} ${feature_group_name}     #
#  默认的feature_key为数据文件的第一行的第一列的列名                       #
#  若要指定feature_key, 可以加在最后一个参数                             #
#####################################################################

__author__ = 'luxiaolang'

CONVERT_TO_CSV = True
ZOOTOPIA_URL = "http://alta1-bdi-zootopia-mgr-1.vm.elenet.me:8080"
ES_PORT = "9200"
KEY_NAME = "feature_key"
REDIS_PIPE_BATCH_SIZE = 10000
REDIS_DEFAULT_TTL = 891200


def excel_to_csv(excel_file):
    csv_file_name = os.path.basename(excel_file).split('.', 1)[0] + '.csv'
    csv_file_path = os.path.join(os.path.dirname(excel_file), csv_file_name)
    csv_file = open(csv_file_path, 'wb')

    wb = xlrd.open_workbook(excel_file)
    # 取第一张表, QUOTE_NONE表示对任何字段都不加引号
    sh = wb.sheet_by_index(0)
    wr = csv.writer(csv_file, quoting=csv.QUOTE_NONE)

    row = []
    print "Converting excel file to csv..."
    for row_index in xrange(sh.nrows):
        # xlrd会把excel的int数据读成float
        row[:] = [int(i) if type(i) == float and i == int(i) else i for i in sh.row_values(row_index)]
        wr.writerow(row)

    csv_file.close()
    print "Converting done."
    return csv_file_path


def http_get(url):
    data = urllib2.urlopen(url).read()
    return data


def get_feature_group_info(name):
    data = http_get(ZOOTOPIA_URL + "/featureGroup/name/" + str(name))
    fg = json.loads(data)
    extra = json.loads(fg["extra"])
    fg["extra"] = extra
    return fg


def get_datasource_info(id):
    data = http_get(ZOOTOPIA_URL + "/dataSource/" + str(id))
    ret = json.loads(data)
    result = []
    type = ret["type"]
    if ret["status"] == 1:
        if type is not None and type == "mixed":
            extra = json.loads(ret["extra"])
            for ds in extra:
                if "status" in ds and ds["status"] == 1:
                    sub_ds = get_datasource_info(ds["id"])
                    result.extend(sub_ds)
        else:
            result.append(ret)
    return result


def get_columns(data_path):
    # 数据文件第一行为表的列
    return open(data_path).readline().strip().split(",")


def get_feature_key_index(columns, key_name):
    for i, col in enumerate(columns):
        if col == key_name:
            return i
    raise RuntimeError("Cannot find feature_key named " + key_name)


def gen_redis_field_value_mapping(columns, feature_values):
    return {columns[i]: feature_values[i] for i, col in enumerate(columns)}


def gen_es_config(data_path, columns, index, type, key_name, host, port):
    config = """
        input {{
            file {{
                path => '{0}'
                start_position => 'beginning'
                sincedb_path => '/dev/null'
            }}
        }}
        filter {{
            csv {{
                separator => ','
                columns => {1}
            }}
            if [{2}] == '{3}' {{
                drop {{}}
            }}
            mutate {{
                remove_field => ['path', 'host', 'message', '@timestamp', '@version']
            }}
        }}
        output {{
            elasticsearch {{
                index => '{4}'
                document_type => '{5}'
                document_id => '%{{{6}}}'
                hosts => '{7}:{8}'
            }}
            stdout {{}}
        }}
    """
    return config.format(data_path, str(columns), columns[0], key_name, index, type, key_name, host, port)


def push_data(data_path, feature_group_name, columns, key_name):
    feature_group = get_feature_group_info(feature_group_name)
    source_info = get_datasource_info(feature_group["sourceId"])
    for data_source in source_info:
        source_type = data_source["type"]
        namespace = feature_group["extra"]["index"]
        sub_namespace = feature_group["extra"]["type"]
        ttl = feature_group["extra"].get("ttl", REDIS_DEFAULT_TTL)
        url = data_source["url"].split(":")
        host = url[0]
        port = url[1]

        if source_type == "elasticsearch":
            do_push_es(data_path, columns, namespace, sub_namespace, key_name, host, ES_PORT)
        elif source_type == "redis":
            do_push_redis(data_path, columns, namespace, sub_namespace, key_name, host, port, ttl)


def do_push_es(data_path, columns, namespace, sub_namespace, key_name, host, port):
    logstash_config = gen_es_config(data_path, columns, namespace, sub_namespace, key_name, host, port)
    sh_cmd = 'logstash -e "%s"' % logstash_config
    print sh_cmd

    print "Push data to es start..."
    code = subprocess.call(sh_cmd, shell=True)
    if code != 0:
        logging.info("Error while executing shell command " + sh_cmd)
        raise RuntimeError("Error while executing shell command" + sh_cmd)
    else:
        print "Push data to es done!\n"


def do_push_redis(data_path, columns, namespace, sub_namespace, key_name, host, port, ttl=REDIS_DEFAULT_TTL):
    conn = redis.Redis(host=host, port=port)
    pipe = conn.pipeline()
    redis_key_prefix = namespace + ":" + sub_namespace + ":"
    key_index = get_feature_key_index(columns, key_name)

    print "Push data to redis start..."
    with open(data_path, 'r') as input_file:
        # skip header columns
        input_file.readline()
        count = 0
        for line in input_file:
            feature_values = line.strip().split(',')
            redis_key = redis_key_prefix + feature_values[key_index]
            redis_value = gen_redis_field_value_mapping(columns, feature_values)
            pipe.hmset(redis_key, redis_value)
            pipe.expire(redis_key, ttl)
            count += 1
            if not count % REDIS_PIPE_BATCH_SIZE:
                pipe.execute()
        # send the last data
        pipe.execute()
        print "Push data to redis done!\n"


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("data_file_path and feature_group_name is needed")
        exit(1)

    data_path = sys.argv[1]
    if CONVERT_TO_CSV:
        data_path = excel_to_csv(data_path)
    feature_group = sys.argv[2]
    columns = get_columns(data_path)
    key_name = columns[0]
    if len(sys.argv) > 3:
        key_name = sys.argv[3]

    push_data(data_path, feature_group, columns, key_name)
