from IPython.core.magic import Magics, magics_class, line_magic
from tapflow.lib.utils.log import logger
from tapflow.lib.cache import client_cache
from tapflow.lib.data_pipeline.pipeline import Pipeline, Flow, _flows
from tapflow.lib.op_object import get_signature_v, show_dbs
from tapflow.lib.op_object import *  # 不要注释这一行，为了导入 show_xxx 函数，不然会有问题

@magics_class
class show_command(Magics):
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
                globals().update(eval("show_dbs(quiet=False)"))
                return
            eval("show_" + line + "(quiet=False)")
        except Exception as e:
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
                                                    colored_print(value.strip("'"), "34")
                                                    print(", fields ", end="")
                                                    for i, target in enumerate(targets):
                                                        if i > 0:
                                                            print(", ", end="")
                                                        colored_print(target, "33")
                                                    print(" will be visible")

            def process_nested_properties(props):
                required_fields = []
                optional_fields = []
                for field_name, field_info in props.items():
                    if field_info.get("required", False):
                        required_fields.append((field_name, field_info))
                    else:
                        optional_fields.append((field_name, field_info))
                return required_fields, optional_fields

            required_fields, optional_fields = process_nested_properties(properties)
            print_field_info(required_fields, "Required")
            print_field_info(optional_fields, "Optional")

        if line == "":
            h_command()
            return
        h_datasource(line)
