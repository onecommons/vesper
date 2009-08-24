#!/usr/bin/env python

import itertools, json

orig = json.load(open('userprefs.json', 'r'))
munged = []
for x in orig:
    n = x['name']
    v = x['value']
    if not isinstance(n, list):
        assert not isinstance(v, list)
        n = [n]
        v = [v]        
    data = {'iid':x['installid'], 'values':dict(itertools.izip(n, v)) }
    munged.append(data)

f = open('userprefs2.json', 'w')
json.dump(munged, f, sort_keys=True, indent=4)
f.close()
