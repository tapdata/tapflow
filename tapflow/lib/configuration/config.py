import configparser
import getpass
import os
from os.path import expanduser

from tapflow.lib.login import login_with_access_code, login_with_ak_sk
from tapflow.lib.op_object import show_agents
from tapflow.lib.utils.log import logger


DEFAULT_CONFIG_PATH = os.path.join(expanduser("~"), ".tapflow/config.ini")


def get_configuration_path():
    return DEFAULT_CONFIG_PATH


class Configuration:
    # OP Version
    server = ""
    access_code = ""
    # Cloud Version
    ak = ""
    sk = ""


class ConfigParser:
    
    def __init__(self, config_path, interactive=False):
        """
        :param config_path: config file path
        :param interactive: whether to interact with user
        """
        self.config_path = config_path
        self.interactive = interactive
        self.config = configparser.ConfigParser()

    def parse(self) -> Configuration:
        self.config.read(self.config_path)
        ini_dict = {section: dict(self.config.items(section)) for section in self.config.sections()}
        config = Configuration()
        config.server = ini_dict.get("backend", {}).get("server")
        config.access_code = ini_dict.get("backend", {}).get("access_code")
        config.ak = ini_dict.get("backend", {}).get("ak")
        config.sk = ini_dict.get("backend", {}).get("sk")
        return config
    
    def write(self, configuration: Configuration):
        # if directory not exist, create it
        if not os.path.exists(os.path.dirname(self.config_path)):
            os.makedirs(os.path.dirname(self.config_path))
        with open(self.config_path, "w") as f:
            f.write(f'''[backend]
; If you are using Tapdata Cloud, please provide the access key and secret key(ak & sk).
; You may obtain the keys by log onto Tapdata Cloud, and click "User Center" on the top right, then copy & paste the access key and secret key pair.
; You can sign up for a new account from: https://cloud.tapdata.io if you don't have one
{f"ak = {configuration.ak}" if configuration.ak else "; ak = "}
{f"sk = {configuration.sk}" if configuration.sk else "; sk = "}

; If you are using TapData Enterprise, please specify the server URL & access token.
{f"server = {configuration.server}" if configuration.server else "; server = "}
{f"access_code = {configuration.access_code}" if configuration.access_code else "; access_code = "}
''')
    
    def read_configuration_from_interactive(self) -> Configuration:
        """
        交互式模式下：
            1. 选择OP版本还是Cloud版本
            2. 设置server和access_code或者设置ak和sk
        """
        def select_version():
            """select op or cloud version"""
            print("""TapFlow requires TapData Live Data Platform(LDP) cluster to run. 
If you would like to use with TapData Enterprise or TapData Community, type L to continue. 
If you would like to use TapData Cloud, or you are new to TapData, type C or press ENTER to continue.""")
            prompt_message = "Please type L or C (L/[C]): "
            while True:
                select_option = input(prompt_message)
                if select_option in ["L", "C", ""]:
                    return select_option if select_option != "" else "C"
                print("Invalid input, please try again\n")
        
        def set_server_and_access_code():
            """set server and access_code"""
            server = input("Please enter server:port of TapData LDP server: ")
            access_code = getpass.getpass("Please enter access code: ")
            return server, access_code
        
        def set_ak_sk():
            print("You may obtain the keys by log onto TapData Cloud, and click: 'User Center' on the top right, then copy & paste the accesskey and secret key pair.")
            ak = getpass.getpass("Enter AK: ")
            sk = getpass.getpass("Enter SK: ")
            return ak, sk
        
        print("\n--- Please configure TapData cluster ---\n")

        select_option = select_version()

        configuration = Configuration()
        if select_option == "L":
            server, access_code = set_server_and_access_code()
            configuration.server = server
            configuration.access_code = access_code
        elif select_option == "C":
            ak, sk = set_ak_sk()
            configuration.ak = ak
            configuration.sk = sk
        return configuration

    def init(self):
        """
        init configuration from config file

        交互式模式下：
            1. 如果配置文件存在，则读取配置文件
            2. 如果配置文件不存在，则提示用户输入配置
            3. 将配置写入配置文件
        命令行模式下：
            1. 如果配置文件存在，则读取配置文件
            2. 如果配置文件不存在，退出并提示用户创建配置文件

        3. 判断是OP版本还是Cloud版本
        4. 根据版本选择登陆认证方式
        """

        # 如果配置文件不存在
        if not os.path.exists(self.config_path) and not os.path.isfile(self.config_path):
            if self.interactive:
                # 交互式模式下，读取用户输入的配置
                configuration = self.read_configuration_from_interactive()
                # 将配置写入配置文件
                self.write(configuration)
            else:
                # 配置文件不存在，退出并提示用户创建配置文件
                logger.warn("{}", f"no valid config file found, you can config {self.config_path} from sample file")
                os._exit(1)
        # 读取配置文件
        configuration = self.parse()
        # 根据配置文件中的ak和sk或者server和access_code，选择登录方式
        if configuration.ak and configuration.sk:
            login_with_ak_sk(configuration.ak, configuration.sk, configuration.server, interactive=self.interactive)
            if self.interactive:
                show_agents(quiet=False)
        elif configuration.server and configuration.access_code:
            login_with_access_code(configuration.server, configuration.access_code, interactive=self.interactive)
        else:
            logger.warn("{}", f"no valid config file found, you can config {self.config_path} from sample file")
            os._exit(1)