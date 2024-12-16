import json
from typing import Tuple
from tapflow.lib.request import RequestSession
from tapflow.lib.utils.log import logger

class BaseBackendApi:

    def __init__(self, req: RequestSession):
        self.req = req


class LoginResult:  
    token: str
    user_id: str


class UserInfo:
    username: str


class LoginApi(BaseBackendApi):

    def login(self, access_code: str) -> LoginResult:
        res = self.req.post("/users/generatetoken", json={"accesscode": access_code})
        if res.status_code != 200:
            logger.warn("init get token request fail, err is: {}", res)
            return res.json()
        data = res.json()["data"]
        login_result = LoginResult()
        login_result.token = data["id"]
        login_result.user_id = data["userId"]
        return login_result
    
    def get_user_info(self, token: str, user_id: str) -> UserInfo:
        res = self.req.get(f"/users", params={"access_token": token})
        if res.status_code != 200:
            logger.warn("get user info request fail, err is: {}", res.json())
            return res.json()
        user_info = UserInfo()
        for user in res.json()["data"]["items"]:
            if user["id"] == user_id:
                user_info.username = user.get("username", "")
                break
        return user_info
    

class MdbInstanceAssignedApi(BaseBackendApi):

    def get_mdb_instance_assigned(self) -> str:
        """
        获取当前用户分配的mdb实例id
        Returns:
            str: connectionId
        """
        res = self.req.get("/mdb-instance-assigned")
        return res.json().get("data", {}).get("connectionId", "")
    
    def create_mdb_instance_assigned(self) -> str:
        """
        创建一个mdb实例
        Returns:
            str: connectionId
        """
        res = self.req.post("/mdb-instance-assigned/connection")
        return res.json().get("data", "")


class AgentApi(BaseBackendApi):

    def get_all_agents(self) -> list:
        res = self.req.get("/agent")
        return res.json().get("data", {}).get("items", [])
    
    def get_running_agents(self) -> list:
        agents = self.get_all_agents()
        return [agent for agent in agents if str(agent.get("status")).lower() == "running"]
    
    def get_agent_count(self) -> Tuple[int, bool]:
        res = self.req.get("/agent/agentCount")
        if res.status_code == 200 and res.json().get("code") == "ok":
            return res.json().get("data", {}).get("agentRunningCount", 0), True
        return res.json(), False
    

class DatabaseTypesApi(BaseBackendApi):

    def get_all_connectors(self) -> list:
        res = self.req.get("/DatabaseTypes/getDatabases", params={"filter": json.dumps({"where":{"tag":"All","authentication":"All"},"order":"name ASC"})})
        return res.json().get("data", {})


class LogCollectorApi(BaseBackendApi):

    def get_all_log_collectors(self) -> list:
        res = self.req.get("/logcollector")
        items = res.json().get("data", {}).get("items", [])
        return items


class ShareCacheApi(BaseBackendApi):

    def get_all_share_caches(self) -> list:
        res = self.req.get("/shareCache")
        return res.json().get("data", {}).get("items", [])
    
    def create_share_cache(self, data: dict) -> dict:
        res = self.req.post("/shareCache", json=data)
        return res.json().get("data", {})
    

class ExternalStorageApi(BaseBackendApi):

    def get_all_external_storages(self) -> Tuple[dict, bool]:
        res = self.req.get("/ExternalStorage/list")
        if res.status_code == 200 and res.json().get("code") == "ok":
            return res.json(), True
        return res.json(), False
    
    def create_external_storage(self, data: dict) -> Tuple[dict, bool]:
        res = self.req.post("/ExternalStorage", json=data)
        if res.status_code == 200 and res.json().get("code") == "ok":
            return res.json(), True
        return res.json(), False

