import re
import random

from tapflow.lib.login import Credentials


def gen_ws_uuid():
    def replace_function(c):
        r = random.randint(0, 15)  # Generate a random number between 0 and 15
        v = r if c == 'x' else (r & 0x3) | 0x8
        return format(v, 'x')  # Convert the number to a hexadecimal string

    uuid_template = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'
    return re.sub(r'[xy]', lambda match: replace_function(match.group(0)), uuid_template)


# will use the ws_uri in system_server_conf by default, also can specify
def gen_ws_uri_with_id(ws_uri=None):
    if ws_uri is None:
        ws_uri = Credentials().ws_uri
    return ws_uri + "&id=" + gen_ws_uuid()
