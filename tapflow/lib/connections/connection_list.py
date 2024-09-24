import json

from tapflow.lib.request import req

class ConnectionList:
    def __init__(self):
        self._response = self._get_connections()
        self._data = None
        self.connections = None
        self.connection_number = None
        self.connection_names = None
        self._parse_json()

    def _get_connections(self):
        # send the request to get response
        return req.get("/Connections", params=json.dumps({"filter":{"limit": 10000}}))

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