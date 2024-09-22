import threading

from auto_test.tapdata.login import login_with_access_code
from tapshell.tapdata_cli.request import set_req, req
import os

threads_num = 1
# 如果环境变量存在 thread_num, 则使用环境变量的值
if "thread_num" in os.environ:
    threads_num = int(os.environ["thread_num"])

threads_num_config = threads_num

Global_Performance_Rate = 1

global_lock = threading.Lock()
global_load_schema_q = {}

# global client cache
client_cache = {
    "tables": {
    },
    "apis": {
        "name_index": {}
    },
    "apiserver": {
        "name_index": {}
    },
    "connectors": {}
}

# system server conf, will become readonly after login
system_server_conf = {
    "api": "",
    "access_code": "",
    "token": "",
    "user_id": "",
    "username": "",
    "cookies": {},
    "ws_uri": "",
    "auth_param": ""
}

class LogMinerMode:
    manually = "manually"
    automatically = "automatically"
