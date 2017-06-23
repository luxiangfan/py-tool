#!/usr/bin/python
# coding=utf-8
import os

import xlrd as xlrd
import csv
import logging
import subprocess

__author__ = 'luxiaolang'

import sys
import json
import urllib2

CONVERT_TO_CSV = True
ZOOTOPIA_URL = "http://vpca-bdi-zootopia-mgr-1.vm.elenet.me:8080"
ES_PORT = "9200"
KEY_NAME = "feature_key"


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
    return open(data_path).readline().strip("\n").split(",")


def generate_es_config(data_path, columns, index, type, key_name, host, port):
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
            mutate {{
                remove_field => ['path', 'host', 'message', '@timestamp', '@version']
            }}
        }}
        output {{
            elasticsearch {{
                index => '{2}'
                document_type => '{3}'
                document_id => '%{{{4}}}'
                hosts => '{5}:{6}'
            }}
            stdout {{}}
        }}
    """
    return config.format(data_path, str(columns), index, type, key_name, host, port)


def push_data(data_path, feature_group_name, columns, key_name):
    feature_group = get_feature_group_info(feature_group_name)
    datasource_info = get_datasource_info(feature_group["sourceId"])
    for datasource in datasource_info:
        source_type = datasource["type"]
        url = datasource["url"].split(":")
        host = url[0]

        if source_type == "elasticsearch":
            es_index = feature_group["extra"]["index"]
            es_type = feature_group["extra"]["type"]
            es_config = generate_es_config(data_path, columns, es_index, es_type, key_name, host, ES_PORT)

            print "starting push data to es..."
            exec_cmd(es_config)
        elif source_type == "redis":
            print "datasource is redis, skip"


def exec_cmd(cmd):
    # cmd = cmd.replace("\"", "")
    cmd = 'logstash -e "%s"' % cmd
    print cmd
    code = subprocess.call(cmd, shell=True)
    if code != 0:
        logging.info("Error while executing shell command " + cmd)
        raise RuntimeError("Error while executing shell command" + cmd)
    else:
        logging.info("push data succeed!")


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
