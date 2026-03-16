import builtins
import sys
import math
from typing import Any

PROMPT = '>>> '


def run_calc(context: dict[str, Any] | None = None) -> None:
    """Run interactive calculator session in specified namespace"""
    inp = sys.stdin
    out = sys.stdout
    if context is None:
        context = {}
    context['__builtins__'] = {}

    while True:
        q = inp.readline()
        if not q:
            out.write('>>> \n')
            return
        out.write('>>> ' + str(builtins.eval(q, context)) + "\n")

if __name__ == '__main__':
    context = {'math': math}
    run_calc(context)
