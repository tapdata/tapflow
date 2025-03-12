from IPython.core.magic import Magics, magics_class, line_magic
import shlex

from tapsh.lib.connections.connection import desc_table
from tapsh.lib.data_pipeline.project.project import Project
from tapsh.lib.op_object import op_object_command_class, get_obj, Job
from tapsh.lib.data_pipeline.pipeline import Pipeline
from tapsh.lib.utils.log import logger


import ast

def parse_string_to_tuple(input_str):
    # 初始化返回值
    leading_array = []
    result_dict = {}
    
    # 按空格分割字符串
    parts = input_str.split()
    
    # 当前的键名和值，用于字典部分
    current_key = None
    current_value = ""
    
    # 标志位，表示是否已经进入键值对部分
    in_dict_mode = False
    
    for part in parts:
        # 如果还没有进入键值对模式，且当前部分不含等号，认为是数组元素
        if not in_dict_mode and '=' not in part:
            try:
                parsed_value = ast.literal_eval(part)
                leading_array.append(parsed_value)
            except (ValueError, SyntaxError):
                leading_array.append(part)
        # 遇到等号，切换到字典模式
        elif '=' in part and not (part.startswith('{') or part.startswith('[')):
            in_dict_mode = True
            key, value = part.split('=', 1)  # 只分割第一个等号
            current_key = key
            current_value = value
        # 处理字典值的后续部分
        elif in_dict_mode:
            current_value += " " + part
        
        # 如果值已经完整（字典部分），解析它
        if in_dict_mode and current_key and (current_value.endswith('}') or current_value.endswith(']') or not (current_value.count('{') > current_value.count('}') or current_value.count('[') > current_value.count(']'))):
            value_str = current_value.strip()
            try:
                # 尝试直接解析
                parsed_value = ast.literal_eval(value_str)
                result_dict[current_key] = parsed_value
            except (ValueError, SyntaxError):
                # 如果直接解析失败，尝试修复无引号的键或值
                try:
                    # 假设是字典或列表，手动加引号并重试
                    fixed_value = fix_unquoted_string(value_str)
                    parsed_value = ast.literal_eval(fixed_value)
                    result_dict[current_key] = parsed_value
                except (ValueError, SyntaxError):
                    # 如果仍然失败，作为字符串保留
                    result_dict[current_key] = value_str
            # 重置当前键和值
            current_key = None
            current_value = ""
    
    return leading_array, result_dict

def fix_unquoted_string(value_str):
    """
    修复无引号的键或值，例如 {"x": y} -> {"x": "y"}
    """
    if value_str.startswith('{') and value_str.endswith('}'):
        # 处理字典
        content = value_str[1:-1]  # 去掉外层大括号
        pairs = []
        for pair in content.split(','):
            if ':' in pair:
                key, val = pair.split(':', 1)
                key = key.strip()
                val = val.strip()
                # 如果键或值不是字符串包裹且不是数字/列表/字典，添加引号
                if not (key.startswith('"') or key.startswith("'")) and not key.isdigit():
                    key = f'"{key}"'
                if not (val.startswith('"') or val.startswith("'") or val.startswith('{') or val.startswith('[')) and not val.isdigit():
                    val = f'"{val}"'
                pairs.append(f"{key}: {val}")
            else:
                pairs.append(pair)
        return '{' + ', '.join(pairs) + '}'
    return value_str  # 如果不是字典，直接返回原字符串


@magics_class
# global command for object
class OpObjectCommand(Magics):
    types = op_object_command_class.keys()
    def __common_op(self, op, line):
        args_i = 2
        try:
            object_type, signature = shlex.split(line)[0], shlex.split(line)[1]
            if object_type not in self.types:
                object_type = "job"
                signature = shlex.split(line)[0]
                args_i = 1

        except Exception as e:
            object_type = "job"
            signature = shlex.split(line)[0]
        args = []
        kwargs = {}
        if len(shlex.split(line)) > args_i:
            args, kwargs = parse_string_to_tuple(" ".join(shlex.split(line)[args_i:]))

        if object_type == "project":
            obj = Project(signature)
        else:
            obj = get_obj(object_type, signature)
        if obj is None:
            return
        if op in dir(obj):
            import inspect
            method_args = inspect.getfullargspec(getattr(obj, op)).args
            if "quiet" in method_args:
                kwargs["quiet"] = False
            if "force" in method_args:
                kwargs["force"] = True
            if op == "delete":
                confirm = input(f"Are you sure you want to delete {object_type if object_type != 'job' else 'flow'} {signature} (y/[n]): ")
                if confirm != "y":
                    return
            if isinstance(obj, Job) and op == "preview":
                obj = Pipeline(name=obj.name, id=obj.id)
                kwargs["quiet"] = False
            getattr(obj, op)(*args, **kwargs)

    @line_magic
    def stop(self, line):
        return self.__common_op("stop", line)

    @line_magic
    def status(self, line):
        return self.__common_op("stats", line)

    @line_magic
    def monitor(self, line):
        return self.__common_op("monitor", line)
    
    @line_magic
    def copy(self, line):
        return self.__common_op("copy", line)

    @line_magic
    def start(self, line):
        return self.__common_op("start", line)

    @line_magic
    def reset(self, line):
        return self.__common_op("reset", line)

    @line_magic
    def delete(self, line):
        return self.__common_op("delete", line)

    @line_magic
    def validate(self, line):
        return self.__common_op("validate", line)

    @line_magic
    def logs(self, line):
        return self.__common_op("logs", line)

    @line_magic
    def stats(self, line):
        return self.__common_op("stats", line)
    
    @line_magic
    def preview(self, line):
        return self.__common_op("preview", line)

    @line_magic
    def desc(self, line):
        if line == "":
            logger.warn("no desc datasource found")
            return
        if " " not in line or line.split(" ")[0] == "table":
            if " " in line:
                line = line.split(" ")[1]
            return desc_table(line, quiet=False)
        return self.__common_op("desc", line)
    
    @line_magic
    def tap(self, line):

        def run_file(path):
            with open(path, "r") as f:
                exec(f.read())

        def run_dir(path):

            def help_info():
                logger.info("use {} to init project", "tap -d --init $path")
                logger.info("use {} to save project", "tap -d [--save] $path")
                logger.info("use {} to start project", "tap -d --start $path")
                logger.info("use {} to stop project", "tap -d --stop $path")
                logger.info("use {} to delete project", "tap -d --delete $path")

            if len(path.split(" ")) > 1:
                func = path.split(" ")[0]
                path = path.split(" ")[1]
            else:
                if path == "--help" or path == "-h":
                    help_info()
                    return
                func = "--start"
                path = path.split(" ")[0]

            f = {
                "--start": lambda: Project(path=path).start(),
                "--save": lambda: Project(path=path).save(),
                "--init": lambda: Project(path=path).init(),
                "--stop": lambda: Project(path=path).stop(),
                "--delete": lambda: Project(path=path).delete(),
                "--help": lambda: help_info(),
            }
            if func in f:
                f[func]()
            else:
                logger.warn("unknown command: {}", func)
                help_info()

        operate = {
            "-f": run_file,
            "-d": run_dir,
        }

        if len(line.split(" ")) < 2:
            with open(line, "r") as f:
                exec(f.read())
            return
        ope, path = line.split(" ")[0], line.split(" ")[1:]
        operate[ope](" ".join(path))
