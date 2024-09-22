import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(project_root)
from auto_test.init.sources import get_sources
from auto_test.utils.log import logger
from auto_test.tapdata.data_pipeline.data_source import DataSource
from auto_test.tapdata.connections.connection_list import ConnectionList
from auto_test.utils.config import TestConfig

# create datasource in tapdata server
def create_datasource():
    datasources = get_sources()
    for datasource, datasource_value in datasources.items():
        create_single_datasource(datasource, datasource_value)


def create_single_datasource(datasource, source_value):
    try:
        config = source_value["config"]
        connector = config.get("connector")
        if connector is None:
            return

        tapdata_datasource = DataSource(connector, datasource)
        config_changed = False
        for c in config:
            if c.startswith("__"):  # ignore __ start config, as it's useless for server config
                continue
            if c == "connector":  # ignore connector config, as it's useless for server config
                continue
            if c in ["shareCdcEnable", "shareCDCExternalStorageId"]:
                if tapdata_datasource.setting.get("config", {}).get(c) != config[c]:
                    print(f"config {c} is changed, old: {tapdata_datasource.setting.get('config', {}).get(c)}, new: {config[c]}")
                    config_changed = True
                tapdata_datasource.set({c: config[c]})
            if tapdata_datasource.setting.get("heartbeatEnable") != True:
                print(f"config heartbeatEnable is changed, old: {tapdata_datasource.setting.get('heartbeatEnable')}, new: {True}")
                config_changed = True
            tapdata_datasource.set({"heartbeatEnable": True})

            # here we need to run function,such as tapdata_datasource.uri(uri), or tapdata_datasource.host()
            # but dynamic getattr is not possible because function is dynamic, we copy its logic here
            if c == "uri":
                if tapdata_datasource.setting.get("config", {}).get(c) != config[c]:
                    if "******" in tapdata_datasource.setting.get("config", {}).get(c, ""):
                        parts = tapdata_datasource.setting.get("config", {}).get(c).split("******")
                        for part in parts:
                            if part not in config[c]:
                                print(f"config {c} is changed, old: {tapdata_datasource.setting.get('config', {}).get(c)}, new: {config[c]}")
                                config_changed = True
                tapdata_datasource.pdk_setting.update({"isUri": True, c: config[c]})
            elif c == "table_fields":
                tapdata_datasource.pdk_setting.update({c: expand_fields(config.get(c))})
            else:
                if tapdata_datasource.setting.get("config", {}).get(c) != config[c]:
                    if (tapdata_datasource.setting.get("config", {}).get(c) is None and (config[c] is None or config[c] == "")) or c == "password":
                        pass
                    else:
                        print(f"config {c} is changed, old: {tapdata_datasource.setting.get('config', {}).get(c)}, new: {config[c]}")
                        config_changed = True
                tapdata_datasource.pdk_setting.update({c: config[c]})
        tapdata_datasource.config_changed = config_changed
        if config_changed:
            logger.info("creating datasource {}, connector is: {}", datasource, connector)
            tapdata_datasource.save()
            logger.info("create datasource {}, connector is: {}, success\n==================================================\n", datasource, connector)
    except Exception as e:
        print(f"create data source connection {datasource} error:\n{e}")


def create_datasource_ci():
    table_suffix_cache_file = os.path.join(project_root, "auto_test", "init", ".table_suffix_cache_file")
    table_suffix = TestConfig().ci_table_suffix
    # Open the file in write mode and write the table_suffix
    with open(table_suffix_cache_file, "w") as file:
        file.write(table_suffix)
    connections = ConnectionList()
    created_connection_names = connections.connection_names
    datasources = get_sources()
    uncreated_datasources = {conn_name: datasources[conn_name] for conn_name in datasources if
                             conn_name not in created_connection_names}
    for datasource_name, datasource_value in uncreated_datasources.items():
        create_single_datasource(datasource_name, datasource_value)


def expand_fields(fields):
    expanded_fields = []
    for field in fields:
        field: dict
        if "replicas" in field:
            replicas_num = field["replicas"]
            for i in range(1, replicas_num+1):
                new_field = {k: v for k, v in field.items() if k != "replicas"}
                new_field["name"] = field["name"] + "_" + str(i)
                expanded_fields.append(new_field)
        else:
            expanded_fields.append(field)
    return expanded_fields


if __name__ == "__main__":
    config_file_name = TestConfig().config_file_name
    if "ci.config.yaml" == config_file_name:
        print("creating datasource on CI")
        create_datasource_ci()
    else:
        create_datasource()
