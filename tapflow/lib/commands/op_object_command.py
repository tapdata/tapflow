from IPython.core.magic import Magics, magics_class, line_magic
import shlex

from tapflow.lib.connections.connection import desc_table
from tapflow.lib.data_pipeline.project.project import Project
from tapflow.lib.op_object import op_object_command_class, get_obj, Job
from tapflow.lib.data_pipeline.pipeline import Pipeline
from tapflow.lib.utils.log import logger

@magics_class
# global command for object
class OpObjectCommand(Magics):
    types = op_object_command_class.keys()
    def __common_op(self, op, line):
        try:
            object_type, signature = shlex.split(line)[0], shlex.split(line)[1]
            if object_type not in self.types:
                object_type = "job"
                signature = shlex.split(line)[0]

        except Exception as e:
            object_type = "job"
            signature = shlex.split(line)[0]
        args = []
        kwargs = {}
        if len(shlex.split(line)) > 2:
            for kv in shlex.split(line)[2:]:
                if "=" not in kv:
                    args.append(kv)
                else:
                    v = kv.split("=")[1]
                    try:
                        v = int(v)
                        kwargs[kv.split("=")[0]] = v
                    except Exception as e:
                        if v.lower() == "true":
                            kwargs[kv.split("=")[0]] = True
                            continue
                        if v.lower() == "false":
                            kwargs[kv.split("=")[0]] = False
                            continue
                        kwargs[kv.split("=")[0]] = v
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