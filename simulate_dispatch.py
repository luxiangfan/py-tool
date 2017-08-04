#!/usr/bin/python
# coding=utf-8

import json
import sys
import urllib
import urllib2
import time

__author__ = 'luxiaolang'

URL_DICT = {"alpha": "http://adca-lpd-ai-eye-1.vm.elenet.me:8080",
            "alta": "http://alta1-lpd-ai-eye-1.vm.elenet.me:8080",
            "prod": "http://wg-lpd-aieye-1.elenet.me:8080"}

CURRENT_ENV = "alpha"
DEFAULT_URL = URL_DICT.get("alpha")

TASK_STATUS_DONE = '200'
TASK_STATUS_FAILED = '401'
QUERY_TASK_RESULT_INTERVAL = 1


# 模拟站点分单
def simulate_dispatch(team_id, start, end, version):
    request_id = add_team_simu_task(team_id, start, end, version)

    print 'Simulation task has been submitted, requestId: %s, start querying result...' % request_id
    time.sleep(10)

    while True:
        response = query_team_simu_task(request_id)
        if response['code'] == TASK_STATUS_DONE:
            print response['data']
            break
        elif response['code'] == TASK_STATUS_FAILED:
            print "Task failed, requestId: %s, failed reason: %s" % (request_id, response['data'])
            break
        else:
            print "Task is running, requestId: %s" % request_id
            time.sleep(QUERY_TASK_RESULT_INTERVAL)


# 请求成功会返回本次请求的request_id, 用于后续查询模拟器的结果
def add_team_simu_task(team_id, start, end, version):
    params = {"teamId": team_id, "start": start, "end": end, "modelVersion": version}
    try:
        response = http_post(get_current_env_url() + "/simu/addTeamSimuTask", params)
        json_result = json.loads(response)
        if json_result['code'] == '200':
            return json_result['data']
        else:
            print "service addTeamSimuTask return failure result"
            exit(1)
    except urllib2.HTTPError, e:
        print "HTTP Error: %s, URL: %s" % (e.code, e.url)
        exit(1)


# 查询task结果
def query_team_simu_task(request_id):
    params = {"requestId": request_id}
    try:
        response = http_post(get_current_env_url() + "/simu/queryTeamSimuTask", params)
        json_result = json.loads(response)
        return json_result
    except urllib2.HTTPError, e:
        msg = "HTTP Error: %s, URL: %s" % (e.code, e.url)
        raise RuntimeError(msg)


def get_current_env_url():
    return URL_DICT.get(CURRENT_ENV, DEFAULT_URL)


def http_get(url):
    return urllib2.urlopen(url).read()


def http_post(url, params):
    return urllib2.urlopen(url, urllib.urlencode(params)).read()


def help_message():
    return "usage: python simulate_dispatch.py team_id start_time end_time model_version [env]\n" \
           "参数说明:\n" \
           "\tteam_id\t\t站点id\t\trequired\n" \
           "\tstart_time\t开始时间\trequired\n" \
           "\tend_time\t结束时间\trequired\n" \
           "\tmodel_version\t模型版本\trequired\n" \
           "\tenv\t\t当前环境\toptional[alpha, alta, prod]\n"


if __name__ == "__main__":
    if sys.argv[1] == '-h' or sys.argv[1] == '-H':
        print help_message()
        exit(1)

    if len(sys.argv) < 5:
        print("arguments invalid")
        exit(1)

    if len(sys.argv) > 5:
        CURRENT_ENV = sys.argv[5].strip().lower()

    simulate_dispatch(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])

    # simulate_dispatch("3434", "2017-07-27 08:41:00", "2017-07-27 08:59:00", "1.9")
    # query_team_simu_task('QQHGt5gKFhQ4JQs94Lad')
