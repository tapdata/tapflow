from datetime import datetime
import requests

from tapflow.lib.request import set_req
from tapflow.lib.utils.log import logger
from tapflow.lib.cache import system_server_conf

def login_with_access_code(server, access_code):
    print(f"{datetime.now().strftime('%a %b %d %H:%M:%S CST %Y')} \033[36m connecting remote server: {server} \033[0m")
    print(f"{datetime.now().strftime('%a %b %d %H:%M:%S CST %Y')} \033[36m Welcome to TapData Live Data Platform, Enjoy Your Data Trip ! \033[0m")
    req = set_req(server)
    api = "http://" + server + "/api"
    res = req.post("/users/generatetoken", json={"accesscode": access_code})
    if res.status_code != 200:
        logger.fwarn("init get token request fail, err is: {}", res)
        return False
    data = res.json()["data"]
    token = data["id"]
    user_id = data["userId"]
    req.params = {"access_token": token}
    res = req.get("/users")
    if res.status_code != 200:
        logger.fwarn("get user info by token fail, err is: {}", res.json())
        return False
    username = None
    users = res.json()["data"]["items"]
    for user in users:
        if user["id"] == user_id:
            username = user.get("username", "")
            break
    if token is None:
        return False
    cookies = {"user_id": user_id}
    req.cookies = requests.cookies.cookiejar_from_dict(cookies)
    ws_uri = "ws://" + server + "/ws/agent?access_token=" + token
    conf = {
        "api": api,
        "access_code": access_code,
        "token": token,
        "user_id": user_id,
        "username": username,
        "cookies": cookies,
        "ws_uri": ws_uri,
        "auth_param": "?access_token=" + token
    }
    system_server_conf.update(conf)
    return True

def login_with_ak_sk(ak, sk, server=None):
    global req
    try:
        if not server or server == "127.0.0.1:3030":
            server = "https://cloud.tapdata.net"
    except NameError:
        server = "https://cloud.tapdata.net"
    print(f"{datetime.now().strftime('%a %b %d %H:%M:%S CST %Y')} \033[36m connecting remote server: {server} \033[0m")
    print(f"{datetime.now().strftime('%a %b %d %H:%M:%S CST %Y')} \033[36m Welcome to TapData Live Data Platform, Enjoy Your Data Trip ! \033[0m")
    req = set_req(server)
    req.set_ak_sk(ak, sk)
    return True

def login_with_username(username, password):
    pass


class Credentials:
    __ws_uri = None
    __api = None
    __access_code = None
    __token = None
    __user_id = None
    __username = None
    __cookies = None
    __auth_param = None

    def __init__(self):
        from tapflow.lib.cache import system_server_conf
        # use user_id to judge login or not, and only set the value once when login.
        if self.__user_id is None:
            self.__ws_uri = system_server_conf.get("ws_uri")
            self.__api = system_server_conf.get("api")
            self.__access_code = system_server_conf.get("access_code")
            self.__token = system_server_conf.get("token")
            self.__user_id = system_server_conf.get("user_id")
            self.__username = system_server_conf.get("username")
            self.__cookies = system_server_conf.get("cookies")
            self.__auth_param = system_server_conf.get("auth_param")

    # make the attributes readonly
    @property
    def ws_uri(self):
        return self.__ws_uri

    @property
    def api(self):
        return self.__api

    @property
    def access_code(self):
        return self.__access_code

    @property
    def token(self):
        return self.__token

    @property
    def user_id(self):
        return self.__user_id

    @property
    def username(self):
        return self.__username

    @property
    def cookies(self):
        return self.__cookies

    @property
    def auth_param(self):
        return self.__auth_param

