from IPython.core.magic import Magics, magics_class, line_magic
from tapflow.lib.utils.log import logger
from tapflow.lib.request import req
import json

@magics_class
class ApiCommand(Magics):
    @line_magic
    def unpublish(self, line):
        """
        unpublish api
        """
        if line == "":
            logger.warn("no unpublish api found")
            return
        base_path = line
        body = {
            "base_path": base_path
        }
        res = req.post("/api/unpublish", json=body).json()
        if res.get("code") == "ok":
            logger.info("unpublish api {} success", base_path)
        else:
            logger.warn("unpublish api {} fail, err is: {}", base_path, res["message"])

    @line_magic
    def publish(self, line):
        """
        publish api
        """
        if line == "":
            logger.warn("no publish api found")
            return
        base_path = line
        body = {
            "base_path": base_path
        }
        res = req.post("/api/publish", json=body).json()
        if res.get("code") == "ok":
            logger.info("publish api {} success", base_path)
        else:
            logger.warn("publish api {} fail, err is: {}", base_path, res["message"])
