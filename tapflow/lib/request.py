import os
from tapflow.lib.utils.log import logger

import hmac
import hashlib
import base64
import uuid
import time
import requests
import urllib.parse

class RequestSession(requests.Session):
    def sign(self, string_to_sign, access_key_secret):
        """
        签名方法
        :param string_to_sign: 需要签名的字符串
        :param access_key_secret: 密钥
        :return: 签名后的字符串
        """
        encoding = 'utf-8'

        # 创建 HMAC 对象，并使用密钥初始化
        mac = hmac.new(access_key_secret.encode(encoding), string_to_sign.encode(encoding), hashlib.sha1)

        # 完成签名计算
        sign_data = mac.digest()

        # 使用 Base64 编码返回结果
        return base64.b64encode(sign_data).decode(encoding)

    def generate_signed_params(self, request: requests.Request):
        import json
        params = request.params
        data = ""
        if request.json is not None:
            data = json.dumps(request.json)
        access_key = self.ak
        access_key_secret = self.sk
        params.update({
            "ts": str(int(time.time() * 1000)),  # 当前时间戳（毫秒）
            "nonce": str(uuid.uuid4()),  # 随机数
            "signVersion": "1.0",
            "accessKey": access_key
        })

        encoded_params = {}
        for k, v in params.items():
            encoded_params[urllib.parse.quote_plus(k)] = urllib.parse.quote_plus(str(v))

        # 按键名排序
        sorted_params = sorted(encoded_params.items())
        string_to_sign = str(request.method).upper() + ":" + "&".join([f"{k}={v}" for k, v in sorted_params]) + ":"

        string_to_sign = string_to_sign.replace("*", "%2A")
        string_to_sign = string_to_sign.replace("%27", "%22")
        string_to_sign = string_to_sign.replace("+", "%20")
        string_to_sign = string_to_sign.replace("%7E", "~")

        string_to_sign += data
        params['sign'] = self.sign(string_to_sign, access_key_secret)
        return params


    def sign_request(self, request: requests.Request) -> requests.Request:
        request.url = self.base_url + request.url
        params = self.generate_signed_params(request)
        request.params = params
        return request

    def __init__(self, server: str):
        self.server = server
        if "https" in server:
            self.base_url = f"{server}"
        else:
            self.base_url = f"http://{server}/api"
        self.params = {}
        super(RequestSession, self).__init__()
        self.mode = "op"

    def prepare_request(self, request: requests.Request) -> requests.PreparedRequest:
        url_map = {
            "/agent": "/api/tcm",
            "/mdb-instance-assigned": "/api/tcm",
            "/mdb-instance-assigned/connection": "/api/tcm",
            "/agent/agentCount": "/api/tcm",
        }
        if self.mode == "cloud":
            self.base_url = self.server + url_map.get(request.url, "/tm/api")
            request = self.sign_request(request)
        else:
            request.url = self.base_url + request.url
        return super(RequestSession, self).prepare_request(request)
    
    def authentication_check(self, response: requests.Response):
        res = response.json()
        if self.mode == "cloud":
            if res.get("code") == "NotFoundAccessKey":
                logger.error("{}", "Access key not found. Please verify your AK & SK in the configuration file.")
                os._exit(1)
            else:
                return True
        else:
            if res.get("code") == "AccessCode.No.User":
                logger.error("{}", "Access code not found. Please verify your access code in the configuration file.")
                os._exit(1)
            else:
                return True
    
    def request(self, method, url, *args, **kwargs):
        response = super().request(method, url, *args, **kwargs)
        if not self.authentication_check(response):
            os._exit(1)
        return response

    def set_ak_sk(self, ak, sk):
        self.ak = ak
        self.sk = sk
        self.mode = "cloud"


req = RequestSession("127.0.0.1:3030")
def set_req(server):
    global req
    if req is None:
        req = RequestSession(server)
    else:
        req.__init__(server)
    return req


class Api:

    def response_json(self, res: requests.Response):
        if res.status_code < 200 or res.status_code >= 300:
            logger.warn("{}, {}", res.status_code, res.text)
            logger.warn("request failed url: {}", res.url)
            return False
        else:
            return res.json()

    def get(self, id, **kwargs):
        res = req.get(self.url + f"/{id}", **kwargs)
        data = self.response_json(res)
        return data

    def put(self, id, **kwargs):
        res = req.put(self.url + f"/{id}", **kwargs)
        data = self.response_json(res)
        return data

    def post(self, data: dict, url_after=None, **kwargs):
        if url_after is not None:
            url = self.url + url_after
        else:
            url = self.url
        res = req.post(url, json=data, **kwargs)
        data = self.response_json(res)
        return data

    def delete(self, id, data):
        res = req.delete(self.url + f"/{id}", json=data)
        data = self.response_json(res)
        return data

    def list(self, **kwargs):
        res = req.get(self.url, **kwargs)
        data = self.response_json(res)
        return data

    def patch(self, data: dict, url_after=None, **kwargs):
        if url_after is not None:
            url = self.url + url_after
        else:
            url = self.url
        res = req.patch(url, json=data, **kwargs)
        data = self.response_json(res)
        return data


class InspectApi(Api):

    url = "/task/auto-inspect-totals"

