import re


RE_A_HREF = (r'<[aA](?:\s+[A-Za-z][A-Za-z0-9]*=(?:"[A-Za-z0-9#@!$%^&*()\[\]:/.]+"|[A-Za-z0-9#@!$%^&*()\[\]:/.]+))' +
             r'*\s*href=(?:"([A-Za-z0-9#@!$%^&*()\[\]:/.]+)"|([A-Za-z0-9#@!$%^&*()\[\]:/.]+))\s*' +
             r'(?:\s+[A-Za-z][A-Za-z0-9]*=(?:"[A-Za-z0-9#@!$%^&*()\[\]:/.]+"|[A-Za-z0-9#@!$%^&*()\[\]:/.]+))*\s*/?>')

def f_link(m: re.Match[str]) -> str:
    tag = m.group(0)
    hrefs = re.findall(
        r'href=(?:"([A-Za-z0-9#@!$%^&*()\[\]:/.]+)"|([A-Za-z0-9#@!$%^&*()\[\]:/.]+))',
        tag,
        flags=re.IGNORECASE
    )
    last = hrefs[-1]
    rez = last[0] or last[1]
    if rez[-1] == '/':
        rez = rez[:-1]
    return rez
