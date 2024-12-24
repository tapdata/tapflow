from .cli.cli import init
import sys


try:
    if sys.argv[0].endswith("tap") and len(sys.argv) > 1:
        init()
    elif sys.argv[0].endswith("tap"):
        pass
    else:
        get_ipython
except NameError:
    init()
