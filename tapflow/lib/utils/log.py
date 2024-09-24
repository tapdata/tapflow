import time
import logging

# 定义一个全局锁
from threading import Lock
global_log_lock = Lock()
import os

# Global Logger Utils
def get_log_level(level):
    levels = {
        "debug": 0,
        "info": 10,
        "notice": 20,
        "warn": 30,
        "error": 40,
    }
    if level.lower() in levels:
        return levels[level.lower()]
    return 100


class Logger:
    def __init__(self, name="", fname="taptest.log"):
        self.name = name
        self.max_len = 0
        self.color_map = {
            "info": "32",
            "notice": "34",
            "debug": "2",
            "warn": "33",
            "error": "31"
        }
        self.fname = fname
        self.fname_fd = None
        self.fname_no_color_fd = None

    def set_log_file(self, filename):
        filename_no_color_fd = filename+".no_color"
        self.fname_fd = open(filename, mode='a+')
        self.fname_no_color_fd = open(filename_no_color_fd, mode='a+')


    def _header(self, logger_header=False):
        if logger_header:
            return "\033[1;34m" + self.name + "" + "\033[0m" + \
                   time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + ": "
        else:
            return ""

    def _print(self, msg, wrap=True, logger_header=False):
        locked = False
        try:
            locked = global_log_lock.acquire(timeout=10)
            end = "\r"
            if wrap:
                end = "\n"
            l = len(self._header(logger_header) + msg)
            tail = ""
            if l > self.max_len:
                self.max_len = l
            if self.max_len > 180:
                self.max_len = 180
            if l < self.max_len:
                tail = " " * (self.max_len - l)
            print(self._header(logger_header) + msg + tail, end=end)
        except Exception as e:
            print(e)
        finally:
            if locked:
                try:
                    global_log_lock.release()
                except Exception as e:
                    pass
            pass

    def _fprint(self, msg, wrap=True, logger_header=True):
        return

        locked = False
        try:
            locked = global_log_lock.acquire(timeout=10)
            end = "\r"
            if wrap:
                end = "\n"
            l = len(self._header(logger_header) + msg)
            tail = ""
            if l > self.max_len:
                self.max_len = l
            if self.max_len > 180:
                self.max_len = 180
            if l < self.max_len:
                tail = " " * (self.max_len - l)
            if self.fname_fd is not None:
                self.fname_fd.write(self._header(logger_header) + msg + tail + "\n")
                self.fname_fd.flush()
        except Exception as e:
            if self.fname_fd is not None:
                self.fname_fd.write(str(e) + "\n")
                self.fname_fd.flush()
        finally:
            if locked:
                try:
                    global_log_lock.release()
                except Exception as e:
                    pass
            pass
    def _format(self, *args, color=32):
        import inspect
        stack = inspect.stack()
        caller_filename = "run.py"
        for i in range(0, len(stack)):
            fname = stack[i][0].f_code.co_filename.split("/")[-1]
            if fname.startswith("item_"):
                caller_filename = fname
                break
        try:
            if "{}" not in args[0]:
                msg = str(args)
                msg_no_color = "file: " + str(args)
            else:
                msg_module = str(args[0]).replace("{}", "\033[1;{}m{}\033[0m".format(color, "{}"))
                msg = msg_module.format(*args[1:])
                msg_no_color = str(args[0]).format(*args[1:])
                extra_msg = args[msg_module.count("{}")+1:]
                if caller_filename != "run.py":
                    msg = "file: " + caller_filename + " " + msg
                    msg_no_color = "file: " + caller_filename + " " + msg_no_color
                if len(extra_msg) > 0:
                    msg += " " + str(extra_msg)
                    msg_no_color += " " + str(extra_msg)
            if self.fname_no_color_fd is not None:
                self.fname_no_color_fd.write(msg_no_color + "\n")
                self.fname_no_color_fd.flush()
            return msg
        except Exception as e:
            return str(args)

    def info(self, *args, **kargs):
        msg = self._format(*args, color=32)
        self._print(msg, **kargs)
        self._fprint(msg, **kargs)

    def finfo(self, *args, **kargs):
        msg = self._format(*args, color=32)
        self._fprint(msg, **kargs)

    def debug(self, *args, **kargs):
        msg = self._format(*args, color=2)
        self._print(msg, **kargs)
        self._fprint(msg, **kargs)

    def fdebug(self, *args, **kargs):
        msg = self._format(*args, color=2)
        self._fprint(msg, **kargs)

    def d(self, *args, **kargs):
        msg = self._format(*args, color=2)
        self._print(msg, **kargs)
        self._fprint(msg, **kargs)

    def notice(self, *args, **kargs):
        msg = self._format(*args, color=34)
        self._print(msg, **kargs)
        self._fprint(msg, **kargs)

    def fnotice(self, *args, **kargs):
        msg = self._format(*args, color=34)
        self._fprint(msg, **kargs)

    def warn(self, *args, **kargs):
        msg = self._format(*args, color=33)
        self._print(msg, **kargs)
        self._fprint(msg, **kargs)

    def fwarn(self, *args, **kargs):
        msg = self._format(*args, color=33)
        self._fprint(msg, **kargs)

    def error(self, *args, **kargs):
        msg = self._format(*args, color=31)
        self._print(msg, **kargs)
        self._fprint(msg, **kargs)

    def ferror(self, *args, **kargs):
        msg = self._format(*args, color=31)
        self._fprint(msg, **kargs)

    def fatal(self, *args, **kargs):
        msg = self._format(*args, color=31)
        self._print(msg, **kargs)
        self._fprint(msg, **kargs)

    def ffatal(self, *args, **kargs):
        msg = self._format(*args, color=31)
        self._fprint(msg, **kargs)

    def log(self, *args, **kargs):
        msg = args[0]
        color_n = msg.count("{}")
        if len(args) != 1 + color_n * 2:
            self._print(
                "log error, \033[1;32m{}\033[0m args expected, \033[1;32m{}\033[0m got, print all args: {}" \
                    .format(1 + color_n * 2, len(args), args), **kargs
            )
            return

        params = list(args[1:1 + color_n])
        for i in args[1 + color_n:]:
            msg = msg.replace(
                "{}", "\033[1;" + self.color_map.get(i) +
                      "m__24FA49F1-7C36-4481-ACF7-BF2146EA4719__\033[0m", 1
            )
        msg = msg.replace("__24FA49F1-7C36-4481-ACF7-BF2146EA4719__", "{}")
        for i in range(len(params)):
            p = str(params[i])
            for j in range(int(p.count("`") / 2)):
                p = p.replace("`", "\033[1;34m", 1)
                p = p.replace("`", "\033[0m", 1)
            params[i] = p
        self._print(msg.format(*params), **kargs)
        self._fprint(msg.format(*params), **kargs)

    def flog(self, *args, **kargs):
        msg = args[0]
        color_n = msg.count("{}")
        if len(args) != 1 + color_n * 2:
            self._fprint(
                "log error, \033[1;32m{}\033[0m args expected, \033[1;32m{}\033[0m got, print all args: {}" \
                    .format(1 + color_n * 2, len(args), args), **kargs
            )
            return

        params = list(args[1:1 + color_n])
        for i in args[1 + color_n:]:
            msg = msg.replace(
                "{}", "\033[1;" + self.color_map.get(i) +
                      "m__24FA49F1-7C36-4481-ACF7-BF2146EA4719__\033[0m", 1
            )
        msg = msg.replace("__24FA49F1-7C36-4481-ACF7-BF2146EA4719__", "{}")
        for i in range(len(params)):
            p = str(params[i])
            for j in range(int(p.count("`") / 2)):
                p = p.replace("`", "\033[1;34m", 1)
                p = p.replace("`", "\033[0m", 1)
            params[i] = p
        self._fprint(msg.format(*params), **kargs)


logger = Logger("")
# make requests and urllib3 quiet
logging.getLogger("requests").setLevel(get_log_level("WARN"))
logging.getLogger("urllib3").setLevel(get_log_level("WARN"))
