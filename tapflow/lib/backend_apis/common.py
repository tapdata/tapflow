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
        login_result = self.LoginResult()
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
                user_info.username = user["username"]
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
        return res.json().get("data", {}).get("connectionId", "")


class AgentApi(BaseBackendApi):

    def get_all_agents(self) -> list:
        res = self.req.get("/agent")
        return res.json().get("data", {}).get("items", [])
    
    def get_running_agents(self) -> list:
        agents = self.get_all_agents()
        return [agent for agent in agents if str(agent.get("status")).lower() == "running"]
