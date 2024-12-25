import argparse
import os
import sys
import subprocess
from IPython import start_ipython
import importlib
import importlib.util

def get_tapflow_version():
    try:
        # 尝试从 setup.py 获取版本号
        setup_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'setup.py')
        if os.path.exists(setup_path):
            with open(setup_path, 'r') as f:
                content = f.read()
                import re
                # 改进的正则表达式，支持更多版本字符串格式
                version_patterns = [
                    r"version\s*=\s*['\"]([^'\"]+)['\"]",  # 标准格式：version = '1.0.0' 或 version="1.0.0"
                    r"version\s*=\s*([0-9][^,\s]*)",      # 无引号格式：version = 1.0.0
                    r"__version__\s*=\s*['\"]([^'\"]+)['\"]",  # __version__ 格式
                ]
                
                for pattern in version_patterns:
                    version_match = re.search(pattern, content)
                    if version_match:
                        return version_match.group(1)
        
        # 如果找不到 setup.py，尝试从已安装的包中获取版本
        import pkg_resources
        return pkg_resources.get_distribution('tapflow').version
    except Exception:
        return "unknown"

# 添加项目根目录到 Python 路径
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 交互式模式
def interactive_mode(basepath, source_path):
    # 启动 IPython 交互式 shell，加载配置
    ipython_config_path = os.path.join(basepath, '.cli', 'ipython_config.py')
    if not os.path.exists(ipython_config_path):
        os.makedirs(os.path.dirname(ipython_config_path), exist_ok=True)
        with open(ipython_config_path, 'w') as f:
            f.write('''c = get_config()  #noqa
from IPython.terminal.prompts import Prompts, Token

class NoPrompt(Prompts):
    def in_prompt_tokens(self, cli=None):
        return [(Token.Prompt, 'tap> ')]

    def out_prompt_tokens(self):
        return [(Token.OutPrompt, 'tap> ')]

c.TerminalInteractiveShell.prompts_class = NoPrompt
''')

    # 使用绝对路径指定 profile-dir
    profile_dir = os.path.abspath(os.path.join(basepath, '.cli'))

    start_ipython(argv=['--no-banner', '--profile-dir=' + profile_dir, '-i', os.path.join(source_path, 'cli', 'cli.py')])

def execute_file(file_path):
    module_name = os.path.splitext(os.path.basename(file_path))[0]  # 从文件路径中提取模块名称
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# 命令行模式
def command_mode(basepath, source_path):
    # 创建主解析器
    parser = argparse.ArgumentParser(
        description="Tapflow command line interface",
        add_help=False
    )
    
    # 添加全局帮助选项
    help_group = parser.add_argument_group('Help Options')
    help_group.add_argument('-h', '--help', action='store_true', help="Show this help message and exit")
    help_group.add_argument('-v', '--version', action='store_true', help="Show version number and exit")

    # 配置文件选项
    parser.add_argument("-c", "--config", help="Specify the configuration file path", metavar="CONFIG")

    # 文件执行选项
    parser.add_argument("-f", "--file", help="Run a python file", metavar="FILE")

    # 项目操作相关参数
    project_group = parser.add_argument_group('Project Operations')
    project_group.add_argument('-d', metavar="DIR", nargs='?', const='', help="Project Operation mode with optional directory path")
    project_group.add_argument('--init', metavar="DIR", help="Initialize a new project in the specified directory")
    project_group.add_argument('--save', metavar="DIR", help="Save the project to the specified directory")
    project_group.add_argument('--start', metavar="DIR", help="Start the project")
    project_group.add_argument('--stop', metavar="DIR", help="Stop the project")
    project_group.add_argument('--delete', metavar="DIR", help="Delete the project")
    project_group.add_argument('--status', metavar="DIR", help="Show project status")
    project_group.add_argument('--list', action='store_true', help="List all projects")

    try:
        args, unknown = parser.parse_known_args()
    except Exception as e:
        parser.print_help()
        sys.exit(1)
    
    # 处理版本信息
    if args.version:
        version = get_tapflow_version()
        print(f"tapflow version {version}")
        sys.exit(0)

    # 处理帮助信息
    if args.help:
        if args.d is not None:  # 如果是 -d 相关的帮助
            print("\nProject Operations Help:")
            print("------------------------")
            print("Usage: tap -d [DIRECTORY] [OPTIONS]")
            print("\nOptions:")
            print("  DIRECTORY            Directory path (shorthand for --start)")
            print("  --init DIRECTORY     Initialize a new project")
            print("  --save DIRECTORY     Save the project")
            print("  --start DIRECTORY    Start the project")
            print("  --stop DIRECTORY     Stop the project")
            print("  --delete DIRECTORY   Delete the project")
            print("  --status DIRECTORY   Show project status")
            print("  --list              List all projects")
            print("\nExamples:")
            print("  tap -d /path/to/project           # Start project")
            print("  tap -d --init /path/to/project    # Initialize new project")
            print("  tap -d --status /path/to/project  # Check project status")
        else:
            parser.print_help()
        return

    # 设置配置文件路径并初始化
    config_path = os.path.abspath(args.config) if args.config else None
    
    # 动态导入并初始化命令行环境
    try:
        # 使用相对导入
        from ..cli import cli as cli_module
        cli_module.init(config_path)
    except ImportError:
        try:
            # 备选方案：直接导入本地模块
            import cli
            cli.init(config_path)
        except Exception as e:
            print(f"Error initializing command line mode: {e}")
            import traceback
            traceback.print_exc()

    # 处理 -f 参数
    if args.file:
        execute_file(args.file)
        return

    # 处理项目相关操作
    if args.d is not None:
        from tapflow.lib.data_pipeline.project.project import Project
        
        try:
            # 确定项目路径和操作
            project_path = None
            operation = None
            
            # 检查各个操作参数
            if args.init:
                project_path = args.init
                operation = "init"
            elif args.save:
                project_path = args.save
                operation = "save"
            elif args.start:
                project_path = args.start
                operation = "start"
            elif args.stop:
                project_path = args.stop
                operation = "stop"
            elif args.delete:
                project_path = args.delete
                operation = "delete"
            elif args.status:
                project_path = args.status
                operation = "status"
            elif args.list:
                operation = "list"
            elif args.d:  # 如果直接使用 -d 带路径，默认为 start 操作
                project_path = args.d
                operation = "start"
            
            # 执行操作
            if operation == "list":
                Project.list()
            elif project_path:
                project = Project(project_path)
                if operation == "init":
                    project.init()
                elif operation == "save":
                    project.save()
                elif operation == "start":
                    project.start()
                elif operation == "stop":
                    project.stop()
                elif operation == "delete":
                    project.delete()
                elif operation == "status":
                    project.status()
            else:
                parser.print_help()
                
        except Exception as e:
            print(f"Error in project operation: {e}")
            import traceback
            traceback.print_exc()
    else:
        parser.print_help()

def main():
    basepath = os.path.dirname(os.path.abspath(__file__))
    
    # 使用 importlib.util 专门获取 tapflow 包的路径
    try:
        spec = importlib.util.find_spec('tapflow')
        if spec is not None:
            source_path = os.path.dirname(spec.origin)
        else:
            # 如果找不到包，使用相对路径
            source_path = os.path.dirname(os.path.dirname(basepath))
    except Exception:
        # 如果出错，使用相对路径
        source_path = os.path.dirname(os.path.dirname(basepath))
    
    # 设置环境变量, 兼容 Windows
    os.environ["LC_ALL"] = "en_US.utf8"

    # 获取真实的命令行参数（排除脚本路径）
    args = sys.argv[1:]  # 去掉脚本路径
    # 如果有命令行参数，则进入命令行模式
    if args:
        command_mode(basepath, source_path)
    else:
        interactive_mode(basepath, source_path)

if __name__ == "__main__":
    main() 