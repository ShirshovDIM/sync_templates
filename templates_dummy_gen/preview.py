import re

from jinja2 import BaseLoader, FileSystemLoader
from jinja2.ext import loopcontrols, do
from jinja2.nativetypes import NativeEnvironment

# tpl = 'templates/whitespace.jinja'
#
# templates_env = NativeEnvironment(loader=FileSystemLoader('templates'), extensions=[do, loopcontrols])
# tpl = templates_env.get_template('whitespace.jinja')
# code = tpl.render()
# print(code)
list = [1,1]

if list:
    print("list=True")
else:
    print("list=False")
