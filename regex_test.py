__author__ = 'kevin'
import re

str ="xzg - Hello,hello.wav,5808"
pattern = re.compile(r'(.+),(\d*)')
match = pattern.match(str)
if match:
    name,id= match.groups()
