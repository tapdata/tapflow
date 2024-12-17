from .cli.cli import init


try:
    get_ipython
except NameError:
    init()

