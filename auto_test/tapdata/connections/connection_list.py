import json

import requests
from auto_test.utils.config import TestConfig
from auto_test.tapdata.login import Credentials
from auto_test.init.global_vars import req


class ConnectionList:
    _config = TestConfig()
    _credentials = Credentials()
    _base_url = f"http://{_config.server}/api/Connections"
    _filter = {"limit": 10000}
    _params = {"access_token": _credentials.token}
    _params.update({"filter": json.dumps(_filter)})

    def __init__(self):
        # print(self._params)
        self._response = self._get_connections()
        self._data = None
        self.connections = None
        self.connection_number = None
        self.connection_names = None
        self._parse_json()

    def _get_connections(self):
        # send the request to get response
        return requests.get(self._base_url, params=self._params, cookies=self._credentials.cookies)

    def _parse_json(self):
        res_json = self._response.json()
        if self._response.status_code == 200 and res_json.get("code") == "ok":
            self._data = res_json.get("data")
        if self._data is not None:
            self.connections = self._data.get("items")
            self.connection_number = self._data.get("total")
        if self.connections is not None:
            self.connection_names = [connection['name'] for connection in self.connections]

    def refresh_connections(self):
        self._response = self._get_connections()

