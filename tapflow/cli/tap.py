import os
import sys
import subprocess
from IPython import start_ipython

def main():
    # 获取当前脚本所在的目录
    basepath = os.path.dirname(os.path.abspath(__file__))

    # 安装后的环境，动态获取安装路径
    source_path = subprocess.check_output(
        f"{sys.executable} -c \"import os, tapflow; print(os.path.dirname(tapflow.__file__))\"",
        shell=True
    ).decode().strip()

    # 设置环境变量, 兼容 Windows
    os.environ["LC_ALL"] = "en_US.utf8"

    # 启动 IPython 交互式 shell，加载配置
    ipython_config_path = os.path.join(basepath, '.cli', 'ipython_config.py')
    if not os.path.exists(ipython_config_path):
        os.makedirs(os.path.dirname(ipython_config_path), exist_ok=True)
        with open(ipython_config_path, 'w') as f:
            f.write('''c = get_config()  #noqa
from IPython.terminal.prompts import Prompts, Token

class NoPrompt(Prompts):
    def in_prompt_tokens(self, cli=None):
        return [(Token.Prompt, 'tap > ')]

    def out_prompt_tokens(self):
        return [(Token.OutPrompt, 'tap > ')]

c.TerminalInteractiveShell.prompts_class = NoPrompt
''')

    # 使用绝对路径指定 profile-dir
    profile_dir = os.path.abspath(os.path.join(basepath, '.cli'))

    start_ipython(argv=['--no-banner', '--profile-dir=' + profile_dir, '-i', os.path.join(source_path, 'cli', 'cli.py')])

if __name__ == "__main__":
    main() 