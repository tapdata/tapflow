import uuid

from tapflow.lib.request import req
from tapflow.lib.cache import client_cache
from tapflow.lib.utils.log import logger
from tapflow.lib.help_decorator import pad


def show_apiserver(quite=False):
    items = ApiServer.list()
    if not quite:
        logger.log(
            "{} {} {}",
            pad("id", 20),
            pad("name", 20),
            pad("uri", 40),
            "debug", "debug", "debug"
        )
    for i, v in enumerate(items):
        client_cache["apiserver"]["name_index"][v["clientName"]] = {
            "id": v["id"],
            "name": v["clientName"],
            "uri": v["clientURI"],
        }
        if not quite:
            logger.log(
                "{} {} {}",
                pad(v["id"][:6], 20),
                pad(v["clientName"], 20),
                pad(v["clientURI"], 40),
                "notice", "info", "notice"
            )


class ApiServer:
    def __init__(self, id=None, name=None, uri=None):
        logger.fwarn("This feature is expected to be abandoned in the future")
        self.id = id
        self.name = name
        self.uri = uri
        self.processId = str(uuid.uuid4())

    def save(self):
        url = "/ApiServers"
        # update
        item = False
        if self.id:
            item = self.get()
        if self.id and item:
            if item.get("createTime"):
                del item["createTime"]
            if self.name is not None:
                item.update({"clientName": self.name})
            if self.uri is not None:
                item.update({"clientURI": self.uri})
            res = req.patch(url, json=item)
            if res.json()["code"] == "ok":
                return res.json()["data"]
            else:
                return False
        # create
        else:
            if not self.processId or not self.name or not self.uri:
                logger.error("no attribute: {} not found", 'processId or name or uri')
                return False
            payload = {
                "processId": self.processId,
                "clientName": self.name,
                "clientURI": self.uri,
            }
            res = req.post(url, json=payload)
            if res.json()["code"] == "ok":
                return res.json()["data"]
            else:
                return False

    def delete(self):
        if not isinstance(self.id, str) and not self.id:
            logger.error("id must be set")
            return False
        url = "/ApiServers/" + self.id
        res = req.delete(url)
        if res.json()["code"] == "ok":
            return True
        else:
            return False

    def get(self):
        items = self.list()
        for item in items:
            if item["id"] == self.id:
                return item
        return False

    @classmethod
    def list(cls):
        url = "/ApiServers"
        res = req.get(url, params={"filter": '{"order":"clientName DESC","skip":0,"where":{}}'})
        if res.json()["code"] == "ok":
            return res.json()["data"]["items"]
        else:
            return False
