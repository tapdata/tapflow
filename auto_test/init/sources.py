#!/usr/bin/env python
import os
import json

# from auto_test.init.env import *
from auto_test.init.env import data_path
from auto_test.utils.factory import newDB
from auto_test.utils.config import TestConfig
from auto_test.init.suffix import get_suffix


# parse datasource from config.yaml
def get_sources():
    datasources = {}
    tables = []
    suffix = get_suffix()
    for f in os.listdir(data_path):
        if not f.endswith(".py"):
            continue
        tables.append(f.split(".")[0])

    for source, config in TestConfig().sources_to_use.items():
        source_name = source + suffix
        datasources[source_name] = {
            "tables": tables,
            "config": config,
        }
        db_client = newDB(source_name)
        datasources[source_name]["__has_data"] = False
        if db_client is None:
            continue
        # only datasource with __has_data label can work as job source
        if hasattr(db_client, "load"):
            datasources[source_name]["__has_data"] = True
    return datasources


if __name__ == "__main__":
    print(json.dumps(get_sources(), indent=4))
