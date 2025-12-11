#!/usr/bin/env python3

"""
    Set default RISE plugin settings for each notebook
"""

import pathlib
import textwrap
import json
import re


def transform_rise_settings(match):
    content = match.group()
    settings = json.loads(content)
    settings.update({
        "enable_chalkboard": True,
        "footer": """<p style=\"margin: 5px; color: #a0a0a0\">YSDA Python Course</p>""",
        "scroll": True,
        "start_slideshow_at": "selected",
        "theme": "serif",
        "transition": "none"
    })
    return textwrap.indent(json.dumps(settings, indent=1), '  ').strip()


def patch_file(path):
    text = path.read_text()
    parsed = json.loads(text)

    patched_text = re.sub(r'(?<="rise": ) *\{[^\}]*\}', transform_rise_settings, text)

    try:
        patched_parsed = json.loads(patched_text)
    except json.JSONDecodeError:
        raise RuntimeError("Failed to patch %s" % path)

    if 'metadata' not in patched_text or 'rise' not in patched_parsed['metadata']:
        raise RuntimeError("No metadata->rise in %s. Add `metadata.rise = {}` manualy and run again" % path)

    parsed['metadata'].pop('rise', None)
    patched_parsed['metadata'].pop('rise', None)

    if parsed != patched_parsed:
        raise RuntimeError("Failed to patch %s" % path)

    path.write_text(patched_text)


for path in pathlib.Path('.').glob('**/*.ipynb'):
    try:
        patch_file(path)
    except RuntimeError as e:
        print(e)
