c = get_config()  #noqa
from IPython.terminal.prompts import Prompts, Token

class NoPrompt(Prompts):
    def in_prompt_tokens(self, cli=None):
        return [(Token.Prompt, 'tap > ')]

    def out_prompt_tokens(self):
        return [(Token.OutPrompt, 'tap > ')]

c.TerminalInteractiveShell.prompts_class = NoPrompt
