from IPython.core.magic import Magics, magics_class, line_magic
import shlex
import inspect

from tapflow.lib.utils.log import logger
from tapflow.lib.op_object import (get_obj, get_signature_v, get_index_type, match_line,
                                 op_object_command_class)
from tapflow.lib.data_pipeline.project.project import Project

@magics_class
class op_object_command(Magics):
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
            method_args = inspect.getfullargspec(getattr(obj, op)).args
            if "quiet" in method_args:
                kwargs["quiet"] = False
            if "force" in method_args:
                kwargs["force"] = True
            if len(args) > 0:
                getattr(obj, op)(*args, **kwargs)
            else:
                getattr(obj, op)(**kwargs)

    @line_magic
    def stop(self, line):
        """
        stop job/pipeline/project
        """
        self.__common_op("stop", line)

    @line_magic
    def status(self, line):
        """
        get job/pipeline/project status
        """
        self.__common_op("status", line)

    @line_magic
    def monitor(self, line):
        """
        monitor job/pipeline/project
        """
        self.__common_op("monitor", line)

    @line_magic
    def copy(self, line):
        """
        copy job/pipeline/project
        """
        self.__common_op("copy", line)

    @line_magic
    def start(self, line):
        """
        start job/pipeline/project
        """
        self.__common_op("start", line)

    @line_magic
    def reset(self, line):
        """
        reset job/pipeline/project
        """
        self.__common_op("reset", line)

    @line_magic
    def delete(self, line):
        """
        delete job/pipeline/project
        """
        self.__common_op("delete", line)

    @line_magic
    def validate(self, line):
        """
        validate job/pipeline/project
        """
        self.__common_op("validate", line)

    @line_magic
    def logs(self, line):
        """
        get job/pipeline/project logs
        """
        self.__common_op("logs", line)

    @line_magic
    def stats(self, line):
        """
        get job/pipeline/project stats
        """
        self.__common_op("stats", line)

    @line_magic
    def preview(self, line):
        """
        preview job/pipeline/project
        """
        self.__common_op("preview", line)

    @line_magic
    def desc(self, line):
        """
        describe job/pipeline/project
        """
        self.__common_op("desc", line)

    @line_magic
    def tap(self, line):
        """
        tap job/pipeline/project
        """
        self.__common_op("tap", line)
