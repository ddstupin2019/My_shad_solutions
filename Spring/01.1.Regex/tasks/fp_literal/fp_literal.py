from dataclasses import dataclass
import re


@dataclass
class FPLiteral():
    integral: str
    fractional: str
    exp: str


RE_FP_LITERAL = r'^\s*((?:\d+\.\d*|\.\d+)(?:e[+-]?\d+)?)(?=\s|$|[+\-*/])'


def f_repr_fp_literal(m: re.Match[str]) -> FPLiteral:
    literal = m.group(1)
    exp_val = None
    if 'e' in literal:
        mantissa, exp_val = literal.split('e', 1)
    else:
        mantissa = literal
    int_part, frac_part = mantissa.split('.', 1)
    return FPLiteral(integral=int_part, fractional=frac_part, exp=exp_val) # type: ignore
