#!/usr/bin/env python
import os

from vesper.backports import *
from vesper.web.route import Route
from vesper.app import Action, createApp

def load_data(data):
    try:
        import yaml
        return yaml.safe_load(data)
    except ImportError:
        return json.loads(data)

@Route("api/{action}")
def api_handler(kw, retval):
    data_store = kw['__server__'].dataStore
    params = kw['_params']
    action = kw['urlvars']['action']
    
    if action not in ('query', 'update', 'delete', 'add'):
        raise Exception("404 action not found") # todo    
    if 'data' not in params:
        raise Exception("500 missing required parameter") # todo

    out = {'action':action}
    try:
        if action == 'query':
            r = data_store.query(params['data'])
            # print r
            out.update(r)
        elif action == 'update':
            updates = load_data(params['data']) # XXX this should probably change
            
            query = "{*, where(%s)}" % params['where']
            # print "querying:", query
            target = data_store.query(query)['results']
            
            assert len(target) == 1, "Update 'where' clause did not match any objects; try an add"
            target = target[0]
            # print "target object:"
            # print target

            for k in updates.keys():
                # print "updating attribute:", k
                target[k] = updates[k]
                
            # print "updated target:"
            # print target
            
            changed = data_store.update(target) # returns statements modified; not so useful
            out['count'] = len(changed)
            
        elif action == 'add':
            data = load_data(params['data'])
            stmts = data_store.add(data)
            out['count'] = len(stmts)
        elif action == 'delete':
            print "XXX delete action not supported!"
            pass
            
        out['success'] = True
    except Exception, e:
        out['success'] = False
        out['error'] = str(e)

    # print out
    return json.dumps(out,sort_keys=True, indent=4)

createApp(__name__)
# STORAGE_URL="mem://"
# actions = {
#     'http-request' : rx.route.gensequence
# }
# raccoon.run(globals())
#     
