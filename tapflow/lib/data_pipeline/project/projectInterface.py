#! /usr/bin/env python3


from tapflow.lib.data_pipeline.pipeline import Pipeline
from typing import Union


class ProjectInterface:

    def init(self) -> bool:
        """
        CN: 扫描当前path下的project文件，并在当前path下生成project配置文件,文件名为：当前目录名.project
        EN: Scan the project files under the current path, and generate the project configuration file in the current path, the file name is: current directory name.project

        :return: bool, whether the operation is successful
        """
        raise NotImplementedError

    def save(self) -> bool:
        """
        CN: 当path下不存在.project文件，则自动扫描并生成.project文件，默认为当前目录名.project，并保存project配置
        EN: If the path does not exist .project file, it will automatically scan and generate .project file, the default is the current directory name.project, and save the project configuration

        :return: bool, whether the operation is successful
        """
        raise NotImplementedError

    def start(self, path="", name="", id=""):
        """
        CN:   
            - 当path下不存在.project文件，则自动扫描并生成.project文件，默认为当前目录名.project，保存并启动
            - 当path下存在.project文件且project已经运行，则先将当前project配置与当前（远程）配置相比对，如果存在差别，则停止并重置远程project，随后修改配置并重新启动
            - 当path下存在.project文件且未启动，则直接修改配置并启动
        EN: 
            - If the path does not exist .project file, it will automatically scan and generate .project file, the default is the current directory name.project, save and start
            - If the path exists .project file and the project is already running, it will first compare the current project configuration with the current (remote) configuration, if there is a difference, it will stop and reset the remote project, then modify the configuration and restart
            - If the path exists .project file and the project is not running, it will directly modify the configuration and start

        :param path: str, the path to start
        :param name: str, the name of the pipeline to start
        :param id: str, the id of the pipeline to start
        :return: bool, whether the operation is successful
        """
        self.before_start()
        # Do start
        self.after_start()
        raise NotImplementedError

    def stop(self, path="", name="", id="") -> bool:
        """
        CN: 停止project
        EN: Stop the project
        """
        raise NotImplementedError

    def delete(self, path="", name="", id="") -> bool:
        """
        CN: 删除project
        EN: Delete the project
        """
        raise NotImplementedError

    def reset(self, path="", name="", id="") -> bool:
        """
        CN: 重置project
        EN: Reset the project
        """
        raise NotImplementedError

    def setSchedule(self, cron: str):
        """
        CN: 设置project的调度周期
        EN: Set the scheduling period of the project
        """
        raise NotImplementedError

    def exclude(self, path: str):
        """
        CN: 设置project的排除路径
        EN: Set the excluded path of the project
        """
        raise NotImplementedError

    def add_flow(self, flow: Union[str, Pipeline], depended: str=""):
        """
        CN: 添加flow
        EN: Add flow

        :param flow: str | Pipeline, flow name or flow instance
        :param depended: str, flow_name.stage(cdc_or_initial_sync).start_or_end
        """
        raise NotImplementedError
    
    def before_start(self):
        """
        CN: 在start之前执行
        EN: Execute before start
        """
        raise NotImplementedError

    def after_start(self):
        """
        CN: 在start之后执行
        EN: Execute after start
        """
        raise NotImplementedError

    def scan(self):
        """
        CN: 扫描当前path下的flow文件，并更新project配置
        EN: Scan the flow files under the current path, and update the project configuration
        """
        raise NotImplementedError
