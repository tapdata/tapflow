


import json
from tapflow.lib.backend_apis.common import BaseBackendApi


class ConnectionsApi(BaseBackendApi):

    def get_connections(self, limit: int = 10000, skip: int = 0) -> list:
        """
        Get connections
        :param limit: int, default 10000
        :param skip: int, default 0
        :return: list, connections
        """
        res = self.req.get("/Connections", params={"filter": json.dumps({"limit": limit, "skip": skip, "order":"last_updated DESC","noSchema":1,"where":{"createType":{"$ne":"System"}}})})
        return res.json().get("data", {}).get("items", [])
    
    def save_connection(self, connection: dict):
        """
        Save connection
        :param connection: dict, connection
        :return: requests.Response
        """
        res = self.req.post("/Connections", json=connection)
        return res
    
    def delete_connection(self, connection_id: str):
        """
        Delete connection
        :param connection_id: str, connection id
        :return: requests.Response
        """
        res = self.req.delete(f"/Connections/{connection_id}")
        return res
    
    def list_connections(self) -> dict:
        """
        List connections
        :return: dict, connections
        """
        res = self.req.get("/Connections")
        return res.json().get("data", {})
    
    def get_connection(self, connection_id: str=None, connection_name: str=None) -> dict:
        """
        Get connection
        :param connection_id: str, connection id
        :param connection_name: str, connection name
        :return: dict, connection
        """
        if connection_id:
            payload = {
                "where": {
                    "id": connection_id,
                }
            }
        else:
            payload = { 
                "where": {
                    "name": connection_name,
                }
            }
        res = self.req.get("/Connections", params={"filter": json.dumps(payload)})
        items = res.json().get("data", {}).get("items", [])
        if len(items) == 0:
            return None
        return items[0]
    
    def get_connection_by_id(self, connection_id: str) -> dict:
        """
        Get connection by id
        :param connection_id: str, connection id
        :return: dict, connection
        """
        return self.req.get("/Connections/" + connection_id).json()
