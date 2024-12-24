from datetime import datetime
import getpass
import shlex
import os, sys
from os.path import expanduser

from tapflow.lib.backend_apis.common import MdbInstanceAssignedApi
from tapflow.lib.commands.api_command import ApiCommand
from tapflow.lib.commands.op_object_command import OpObjectCommand
from tapflow.lib.configuration.config import get_configuration_path, ConfigParser

# 获取当前脚本文件所在的目录
current_dir = os.path.dirname(os.path.abspath(__file__))

# 获取 lib 目录的路径
lib_path = os.path.join(current_dir, '..')

# 将 lib 目录加入到 Python 搜索路径中
sys.path.append(lib_path)

from IPython.terminal.interactiveshell import TerminalInteractiveShell
from platform import python_version
from tapflow.lib.utils.log import logger
from tapflow.lib.request import req
from tapflow.lib.cache import client_cache
from tapflow.lib.data_pipeline.nodes.sink import Sink
from tapflow.lib.data_pipeline.nodes.source import Source
from tapflow.lib.op_object import *
from tapflow.lib.commands.show_command import ShowCommand
from tapflow.lib.data_pipeline.pipeline import Pipeline, MView, Flow, _flows
from tapflow.lib.data_pipeline.data_source import DataSource
from tapflow.lib.data_pipeline.base_node import WriteMode, upsert, update, SyncType, DropType, no_drop, drop_data, drop_schema, FilterMode, FilterType
from tapflow.lib.data_pipeline.nodes.row_filter import RowFilterType


if not python_version().startswith("3"):
    print("python version must be 3.x, please install python3 before using tapdata cli")
    sys.exit(-1)


os.environ['PYTHONSTARTUP'] = '>>>'
os.environ["PROJECT_PATH"] = os.sep.join([os.path.dirname(os.path.abspath(__file__)), ".."])


def get_default_sink():
    mdb_instance_assigned_api = MdbInstanceAssignedApi(req)
    connection_id = mdb_instance_assigned_api.get_mdb_instance_assigned()
    if not connection_id:
        connection_id = mdb_instance_assigned_api.create_mdb_instance_assigned()
        if not connection_id:
            logger.fwarn("{}", "Failed to create default sink")
            return
        show_connections(quiet=True)
    try:
        default_connection_name = client_cache["connections"]["id_index"][connection_id]["name"]
        global DEFAULT_SINK
        DEFAULT_SINK = QuickDataSourceMigrateJob()
        DEFAULT_SINK.__db__ = default_connection_name
        client_cache["default_sink"] = DEFAULT_SINK
    except KeyError:
        # logger.fwarn("{}", "Failed to get default connection name")
        pass


def init(config_path=None):
    """命令行模式初始化
    
    Args:
        config_path (str, optional): 配置文件路径. 如果为None, 则使用默认路径
    """
    config_file = config_path if config_path else get_configuration_path()
    ConfigParser(config_file, interactive=False).init()
    globals().update(show_connections(quiet=True))
    show_connectors(quiet=True)
    show_jobs(quiet=True)
    if req.mode == "cloud":
        get_default_sink()

def main():
    """交互式模式"""
    # ipython settings
    ip = TerminalInteractiveShell.instance()
    ip.register_magics(ShowCommand)
    ip.register_magics(OpObjectCommand)
    ip.register_magics(ApiCommand)
    ConfigParser(get_configuration_path(), interactive=True).init()
    globals().update(show_connections(quiet=True))
    show_connectors(quiet=True)
    show_jobs(quiet=True)
    if req.mode == "cloud":
        get_default_sink()


if __name__ == "__main__":
    main()
