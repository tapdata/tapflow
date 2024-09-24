import functools
from tapflow.lib.utils.log import logger
import copy
import re


command_help_list = {}
lib_help_list = []
lib_methods_list = {}


def help_decorate(h, args=None, res=None):
    def decorator(obj):
        h2 = h
        if 'Enum' in h2:
            h2 = pad(h2 + ", valid value is: ", 45)
            h2 = h2 + '`'
            for i in list(obj.__dict__.keys()):
                if not i.startswith("__"):
                    h2 = h2 + i + ", "
            h2 = h2.rstrip(", ")
            h2 = h2 + "`"
        context = []
        if h2.startswith("[") and "]" in h2:
            context = h2[1:h2.index("]")].split(",")
            h2 = h2[h2.index("]") + 1:]
        help_obj = Help(
            name=obj.__name__,
            desc=h2,
            args=args,
            res=res,
        )
        obj_print = " ".join(map(str, (obj,)))
        source = obj_print.split(" ")[0][1:]
        if source == "function":
            try:
                full_method = obj_print.split(" ")[1]
                class_name = full_method.split(".")[0]
                # method_name = full_method.split(".")[1]
                if class_name[0].isupper():
                    if lib_methods_list.get(class_name) is None:
                        lib_methods_list[class_name] = []
                    lib_methods_list[class_name].append(help_obj)
                else:
                    if not context:
                        if command_help_list.get("default", None) is None:
                            command_help_list["default"] = []
                        command_help_list["default"].append(help_obj)
                    else:
                        for c in context:
                            if command_help_list.get(c, None) is None:
                                command_help_list[c] = []
                            desc = help_obj.desc.replace("object", c.lower())
                            args2 = help_obj.args.replace("object", c.lower())
                            help_obj_copy = copy.deepcopy(help_obj)
                            help_obj_copy.desc = desc
                            help_obj_copy.args = args2
                            command_help_list[c].append(help_obj_copy)
            except Exception as e:
                print(__file__, e)
                pass
        else:
            lib_help_list.append(help_obj)

        @functools.wraps(obj)
        def func_wrapper(*args, **kargs):
            return obj(*args, **kargs)

        if source == "function":
            return func_wrapper
        else:
            return obj

    return decorator


def class_help_decorate(h):
    def decorator(cls):
        return cls

    return decorator


# pad a string to a certain length
def pad(string, length):
    string = str(string)

    def len_zh(data):
        temp = re.findall('[^_\-a-zA-Z$0-9. #()\',\\\\/]+', data)
        count = 0
        for i in temp:
            count += len(i)
        return count

    zh = len_zh(string)
    if len(string) >= length:
        return string
    return string + " " * (length - len(string) - zh)


# Helper class, used to provide operation tips
class Help:
    def __init__(self, name, desc, args=None, res=None):
        self.name = name
        self.desc = desc
        self.args = args
        self.res = res


# operation tips when type h
def show_help(t):
    if t == "signature":
        # logger.info(
        #    "signature is used to name an object, it can be: {}, object type includes; {}",
        #    "id, short id, name",
        #    "datasource, job, api"
        # )
        return

    s = None
    if "." in t:
        s = "."
    if " " in t:
        s = " "
    if s is not None:
        t = t.split(s)[-1]

    l = None
    if t == "command":
        logger.notice(
            "{} is used to name a object, it can be: {}, object type includes; {}\n",
            "signature",
            "id, short id, name",
            "datasource, job, api"
        )
        l = command_help_list
    if t == "lib":
        logger.notice("type {} get detail help, for example: h lib Pipeline\n", "h lib $name")
        l = lib_help_list
    if l is None:
        if lib_methods_list.get(t) is not None:
            l = lib_methods_list.get(t)

    if l is None:
        logger.warn("no help info for {}", t)
        return

    logger.log("{} {} {}", pad("{} name".format(t), 25), pad("desc", 70), "example usage", "info", "info", "info")
    logger.notice("{}", "-" * 120)
    enums = []
    relations = []
    if type(l) == type({}):
        for context, commands in l.items():
            if not commands:
                continue
            logger.notice("{} commands:\n", context)
            for command in commands:
                logger.log(
                    "{} {} {}",
                    pad(command.name, 15), pad(command.desc, 50), command.args,
                    "notice", "debug", "info"
                )
            logger.notice("{}", "-" * 120)
        return

    for i in range(len(l)):
        h = l[i]
        if "Enum" in h.desc:
            enums.append(h)
            continue
        if "Relation" in h.desc:
            relations.append(h)
            continue

        if h.args is None:
            logger.log("{} {}", pad(h.name, 25), pad(h.desc, 70), "notice", "debug")
        else:
            logger.log("{} {} {}", pad(h.name, 25), pad(h.desc, 70), h.args, "notice", "debug", "info")

    if len(relations) > 0:
        # logger.info("")
        # logger.info("{}", "below is relation object, it used to describe how source and sink linked")
        for h in relations:
            logger.log("{}: {}", pad(h.name, 15), h.desc, "notice", "debug")
    if len(enums) > 0:
        # logger.info("")
        # logger.info("{}", "below is enum object")
        for h in enums:
            logger.log("{} {}", pad(h.name, 15), pad(h.desc, 50), "notice", "debug")
