import json

from tapflow.lib.backend_apis.connections import ConnectionsApi
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
        return ConnectionsApi(req).get_connections(limit=10000)

    def _parse_json(self):
        if self._response:
            self.connections = self._response
            self.connection_number = len(self._response)
        if self.connections:
            self.connection_names = [connection['name'] for connection in self.connections]

    def refresh_connections(self):
        self._response = self._get_connections()