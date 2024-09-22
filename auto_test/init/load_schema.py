import os, sys

project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(project_root)
# from env import *
from suffix import get_suffix
from sources import get_sources
from auto_test.tapdata.connections.connection import Connection


# create datasource in tapdata server
def l():
    datasources = get_sources()
    for datasource in datasources:
        try:
            print("start loading {} schema", datasource)
            Connection(name="%s" % (datasource)).load_schema(quiet=False)
        except Exception as e:
            print("load {} schema failed, err is: {}".format(datasource, e))


if __name__ == "__main__":
    l()
