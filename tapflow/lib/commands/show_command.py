from IPython.core.magic import Magics, magics_class, line_magic

from tapflow.lib.data_pipeline.pipeline import Pipeline, _flows, Flow
from tapflow.lib.help_decorator import pad
from tapflow.lib.utils.datasource_field_map import datasource_field_map
from tapflow.lib.utils.log import logger
from tapflow.lib.cache import client_cache
from tapflow.lib.request import req
from tapflow.lib.op_object import get_signature_v, get_index_type, match_line, show_dbs, show_tables, show_connections
from tapflow.lib.op_object import *  # 不可注释/删除，主要用于ShowCommand.show()中的eval("show_" + line + "(quiet=False)")


def get_like_query(line):
    if len(line.split("like")) > 1:
        return " ".join([a.strip() for a in line.split("like")[1:]])
    return None

@magics_class
class ShowCommand(Magics):
    @line_magic
    def show(self, line):
        if not line:
            pass
        if line in _flows:
            print(_flows[line].show())
            return
        for name, value in globals().items():
            if name == line and isinstance(value, Flow):
                print(value.show())
                return
        try:
            if "dbs" == line:
                globals().update(show_dbs(quiet=False))
                return
            elif line.split(" ")[0] in ["jobs", "flows"]:
                query = get_like_query(line)
                if query is not None:
                    show_jobs(query=query)
                else:
                    show_jobs()
                return
            elif line.split(" ")[0] == "tables":
                query = get_like_query(line)
                if query is not None:
                    show_tables(query=query)
                else:
                    show_tables()
                return
            else:
                eval("show_" + line + "(quiet=False)")
        except Exception:
            # by default show pipeline
            obj = get_signature_v("pipeline", line)
            if obj is None:
                logger.warn("no pipeline {} found", line)
                return
            obj_id = obj['id']
            name = obj['name']
            pipeline = Pipeline(name=name, id=obj_id)
            print(pipeline.show())

    @line_magic
    def preview(self, line):
        if line in _flows:
            print(_flows[line].preview())
            return
        for name, value in globals().items():
            if name == line and isinstance(value, Flow):
                print(value.preview())

    @line_magic
    def use(self, line):
        if line == "":
            logger.warn("no use datasource found")
            return
        connection = get_signature_v("datasource", line)
        if connection is None:
            logger.warn("connection {} not found", line)
            return

        connection_id = connection["id"]
        connection_name = connection["name"]
        client_cache["connection"] = connection_id
        logger.info("datasource switch to: {}", connection_name)

    @line_magic
    def h(self, line):
        self.help(line)

    @line_magic
    def help(self, line):
        def h_command():
            logger.notice("{}", "- show datasource/table")
            logger.info("    1. {}: show datasource list", pad("show dbs", 20))
            logger.info("    2. {}: switch to datasource", pad("use $db_name", 20))
            logger.info("    3. {}: after use $db_name, used to show tables", pad("show tables", 20))
            logger.info("    4. {}: describe table schema", pad("desc $table_name", 20))
            logger.info("    5. {}: peek some records from table", pad("peek $table_name", 20))
            logger.notice("{}", "- jobs command")
            logger.info("    1. {}: show all jobs", pad("show jobs", 20))
            logger.info("    2. {}: start a job", pad("start $job_name", 20))
            logger.info("    3. {}: stop a job", pad("stop $job_name", 20))
            logger.info("    4. {}: status a job", pad("status $job_name", 20))
            logger.info("    5. {}: show metrics", pad("stats $job_name", 20))
            logger.info("    6. {}: delete a job", pad("delete $job_name", 20))
            logger.notice("{}", "- create a datasource")
            logger.info("    1. {}", "x = DataSource('mysql', 'my-mysql').host('localhost').port(3306).username('root').password('<PASSWORD>')")
            logger.info("    2. {}", "x.save()")
            logger.notice("{}", "- create a simple flow")
            logger.info("    1. {}", "x = Flow('name')")
            logger.info("    2. {}", "x.read_from($ds.$source_table)")
            logger.notice("{}", "- add nodes in a flow")
            logger.info("    1. {}", "x = Flow('name')")
            logger.info("    2. {}: x.filter('id > 2 and sex=male')", "filter records")
            logger.info("    3. {}: x.filter_columns(['id', 'name'], 'keep')", "filter columns")
            logger.info("    4. {}: x.rename_fields(dict: $old_name -> $new_name)", "rename fields")
            logger.info("    5. {}: x.func($func), support js/python code", "add func")
            logger.notice("{}", "- create a lookup flow")
            logger.info("    1. {}", "x = Flow('name')")
            logger.info("    2. {}", "table = x.read_from($ds.$source_table)")
            logger.info("    3. {}", "table.lookup($ds.$table1, path='user', type=dict, relation=[['user_id', 'user_id']], filter='user_id > 1', fields=['user_id', 'user_name'])")
            logger.notice("{}", "- store flow to database")
            logger.info("    1. {}", "x = Flow('name')")
            logger.info("    2. {}", "x.read_from($ds.$source_table)")
            logger.info("    3. {}", "x.write_to($ds.$sink_table)")
            logger.info("    4. {}", "x.start()")

        def h_datasource(l):
            properties = client_cache["connectors"][l].get("properties", {})
            
            def colored_print(text, color="34"):  # 34 是蓝色，33 是黄色
                """打印带颜色的文本，不带换行"""
                print(f"\033[1;{color}m{text}\033[0m", end="")

            def parse_condition(condition):
                """解析不同格式的条件字符串"""
                if "!==" in condition:
                    parts = condition.split("!==")
                    if len(parts) == 2:
                        value = parts[1].strip().strip("'").strip("}}")
                        # 简化布尔值的显示
                        if value.lower() == "false":
                            return "True"
                        elif value.lower() == "true":
                            return "False"
                        return "not " + value
                elif "===" in condition:
                    parts = condition.split("===")
                    if len(parts) == 2:
                        value = parts[1].strip().strip("'").strip("}}")
                        # 首字母大写的布尔值
                        if value.lower() == "true":
                            return "True"
                        elif value.lower() == "false":
                            return "False"
                        return value
                elif "==" in condition:
                    parts = condition.split("==")
                    if len(parts) == 2:
                        value = parts[1].strip().strip("'").strip("}}")
                        return value.capitalize() if value.lower() in ["true", "false"] else value
                elif "$deps[" in condition:
                    # 处理 MongoDB 格式的条件
                    value = condition.split("?")[0].strip()
                    if "true" in value.lower():
                        return "True"
                    elif "false" in value.lower():
                        return "False"
                return None

            def format_bool_value(value, as_string=False):
                """格式化布尔值"""
                str_value = str(value).lower()
                if not as_string:
                    if str_value == "true":
                        colored_print("True", "34")  # 蓝色
                    elif str_value == "false":
                        colored_print("False", "33")  # 黄色
                    return
                return str_value.capitalize() if str_value in ["true", "false"] else str_value

            def print_field_info(fields, field_type):
                if len(fields) > 0:
                    print(f"{field_type} config:")
                    for r in fields:
                        if isinstance(r, (tuple, list)):
                            field_name, field_info = r
                        else:
                            field_name = r.get("name", "")
                            field_info = r
                        
                        # 后端提供的字段名存在不合适展示的情况，在这里做一个映射
                        if datasource_field_map.get(field_name):
                            field_name = datasource_field_map.get(field_name)
                        if datasource_field_map.get(field_info.get('apiServerKey', '')):
                            field_info['apiServerKey'] = datasource_field_map.get(field_info.get('apiServerKey', ''))
                        
                        # 将数据类型信息附加在字段描述后面
                        desc = f": {field_info.get('apiServerKey', '')} (Type: {field_info.get('type', '')})"
                        logger.info("    {}" + desc, field_name)
                        
                        has_enum = "enum" in field_info
                        has_reactions = "x-reactions" in field_info
                        
                        if has_enum or has_reactions:
                            if has_enum:
                                enum_values = field_info["enum"]
                                display_values = [item['value'] for item in enum_values if isinstance(item['value'], str) and item['value'].strip()]
                                if field_name == "timezone" and len(display_values) > 3:
                                    display_values = display_values[:3] + ["..."]
                                
                                print("        Enum values: ", end="")
                                for i, v in enumerate(display_values):
                                    if i > 0:
                                        print(", ", end="")
                                    colored_print(v, "34")  # 为枚举值添加颜色
                                print()
                            
                            if has_reactions:
                                x_reactions = field_info["x-reactions"]
                                if isinstance(x_reactions, list):
                                    print("        Dependencies:")
                                    for reaction in x_reactions:
                                        if isinstance(reaction, dict) and "fulfill" in reaction and "state" in reaction["fulfill"]:
                                            visible_condition = reaction["fulfill"]["state"].get("visible", "")
                                            if visible_condition:
                                                value = parse_condition(visible_condition)
                                                if value:
                                                    targets = reaction["target"].strip("*()").split(",")
                                                    targets = [t.strip() for t in targets]
                                                    print("            When value is ", end="")
                                                    colored_print(value.strip("'"), "34")  # 去掉多余的单引号
                                                    print(", requires: ", end="")
                                                    colored_print(", ".join(targets), "34")
                                                    print()
                                elif isinstance(x_reactions, dict):
                                    if "fulfill" in x_reactions and "dependencies" in x_reactions:
                                        deps = x_reactions["dependencies"]
                                        print("        Depends on: ", end="")
                                        colored_print(", ".join(deps), "34")
                                        print()

                        # 处理数组类型的字段
                        if field_info.get("type") == "array":
                            items = field_info.get("items", {})
                            if "properties" in items:
                                def process_nested_properties(props):
                                    result = []
                                    for prop_key, prop_value in props.items():
                                        if prop_value.get("type") == "void":
                                            if "properties" in prop_value:
                                                result.extend(process_nested_properties(prop_value["properties"]))
                                        else:
                                            result.append(f"{prop_key}: {prop_value.get('type', '')}")
                                    return result

                                nested_fields = []
                                for space_key, space_value in items["properties"].items():
                                    if "properties" in space_value:
                                        nested_fields.extend(process_nested_properties(space_value["properties"]))
                                
                                if nested_fields:
                                    print(f"        Array[{', '.join(nested_fields)}]")

            # 初始化 requires 和 optionals 列表
            requires = []
            optionals = []

            # 处理所有字段，检查是否有条件显示的字段
            for key, value in properties.items():
                if key == "OPTIONAL_FIELDS":
                    # 处理 OPTIONAL_FIELDS 中的可选字段
                    optional_fields = value.get("properties", {})
                    for opt_key, opt_value in optional_fields.items():
                        optionals.append((opt_key, opt_value))
                    continue  # 跳过 OPTIONAL_FIELDS 作为单个字段
                if value.get("required"):
                    requires.append((key, value))
                else:
                    optionals.append((key, value))

            # 打印必需和可选字段
            print_field_info(requires, "required")
            print_field_info(optionals, "optional")

        if line == "":
            h_command()
        if line.lower() in client_cache["connectors"]:
            h_datasource(line.lower())

    def _get_table(self, line):
        connection_id = client_cache.get("connection")
        if connection_id is None:
            logger.warn("no datasource set, please use 'use $datasource_name' to set a valid datasource")
            return None, None
        if client_cache.get("connections") is None:
            show_connections(quiet=True)
        table = line
        if "." in line:
            db = line.split(".")[0]
            table = line.split(".")[1]
            connection = get_signature_v("datasource", db)
            connection_id = connection["id"]
        table_id = ""
        index_type = get_index_type(line)
        if index_type == "short_id_index":
            line = match_line(client_cache["tables"]["id_index"], line)
            index_type = "id_index"
        if index_type == "id_index":
            table_id = line
        if client_cache["tables"].get(connection_id) is None:
            show_tables(source=connection_id, quiet=True)
        table = client_cache["tables"][connection_id][index_type][table]
        connection = get_signature_v("datasource", connection_id)
        return table, connection
    
    def _peek(self, line, connection, table):
        connection_id = client_cache.get("connection")
        capabilities = connection.get("capabilities", [])
        support_peek = False
        for capability in capabilities:
            if capability.get("id") == "query_by_advance_filter_function":
                support_peek = True
                break

        if not support_peek:
            logger.warn("datasource {} not support peek", line)
            return False
        table_name = table["original_name"]

        return TaskApi(req).preview_task(connection_id, table_name)


    @line_magic
    def peek(self, line):
        """
        peek some records from table, support -n to specify the number of records to peek
        """
        connection_id = client_cache.get("connection")
        if line == "":
            logger.warn("no peek datasource found")
            return
        elif connection_id is None:
            logger.warn("no datasource set, please use 'use $datasource_name' to set a valid datasource")
            return
        # parse line
        try:
            if "-n" in line:
                line_split = line.split(" ")
                limit = int(line_split[line_split.index("-n") + 1])
                line = "".join(set(line_split) - set([str(limit), "-n"]))
            else:
                limit = 5
        except Exception as e:
            limit = 5
        table, connection = self._get_table(line)
        table_name = table["original_name"]
        res = self._peek(line, connection, table)
        if res is False:
            return
        try:
            count = res["data"].get("tableInfo", {}).get("numOfRows", 0)
            logger.info("table {} has {} records", table_name, count)
            sample_data = res["data"]["sampleData"]
            x = 0
            for i in sample_data:
                x += 1
                if x > limit:
                    break
                print(json.dumps(i, indent=2))
        except Exception as e:
            pass

    @line_magic
    def count(self, line):
        connection_id = client_cache.get("connection")
        if line == "":
            logger.warn("no peek datasource found")
            return
        elif connection_id is None:
            logger.warn("no datasource set, please use 'use $datasource_name' to set a valid datasource")
            return
        table, connection = self._get_table(line)
        res = self._peek(line, connection, table)
        if res is False:
            return
        try:
            count = res["data"].get("tableInfo", {}).get("numOfRows", 0)
            logger.info("table {} has {} records", table["original_name"], count)
        except Exception as e:
            pass