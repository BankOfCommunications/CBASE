#!/usr/bin/env python2
"""
findall.py pat <input-file
# xpat has the form 'http://${url:str:.*}/${subpath:int:\d+}/index.html'
# '$', '{', '}' is special char
"""

import sys
import re

def tolist(x):
    if type(x) == list or type(x) == tuple:
        return x
    else:
        return [x]
def parse_xpat(pat):
    '''>>> parse_xpat('http://${url:str:.*}/${subpath:int:\d+}/index.html')
('http://(?P<url>.+)/(?P<subpath>\d+)/index.html', ('url:str', 'subpath:int'))'''
    rexp = re.sub(r'\${(\w+):(?:.*?:)?([^}:]+)}', r'(?P<\1>\2)', pat)
    keys = re.findall(r'\${([^}]+):.*?}', pat)
    return rexp, keys
if len(sys.argv) != 2:
    print __doc__
else:
    pat = sys.argv[1]
    rexp, keys = parse_xpat(sys.argv[1])
    print ' '.join(keys)
    for i in re.findall(rexp, sys.stdin.read(), re.M):
        print ' '.join(tolist(i))
        
