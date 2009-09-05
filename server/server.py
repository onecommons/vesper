#!/usr/bin/env python

import raccoon, rx.route
from rx.python_shim import *
from optparse import OptionParser
from rx.route import Route

import os
from string import Template

from mako.lookup import TemplateLookup

TEMPLATE_DIR="server/templates"
RESOURCE_DIR="server/resources"

template_loader = TemplateLookup(directories=[TEMPLATE_DIR], module_directory='/tmp/rhizome_mako_modules', output_encoding='utf-8', encoding_errors='replace')

# query/update history
_QUERIES = []
_UPDATES = []
try:
    data = json.load(open('hist.json', 'r')) # XXX store this per-db
    _QUERIES = data['queries']
    _UPDATES = data['updates']
    
except Exception, e:
    print "can't load history!", e

def store_query(q, update=False):
    if update:
        dat = _UPDATES
    else:
        dat = _QUERIES
    if q not in dat:
        dat.append(q)
        try:
            data = open('hist.json', 'w')
            json.dump({"queries":_QUERIES, "updates":_UPDATES}, data)
        except Exception, e:
            print "error saving query history!", e

@Route("index")
def index(kw, retval):
    method=kw['_environ']['REQUEST_METHOD']
    params = kw['_params']

    if method == 'GET':
        sample = ''
        if 'hist' in params:
            sample = _QUERIES[int(params['hist'])]
        data = {
            'path':'index', # XXX
            'label':'query',
            'sample':sample,
            'link':'/update',
            'link_label':'update',
            'hist':_QUERIES
        }
        template = template_loader.get_template('query.html')
        # data['hist'] = len(_UPDATES) + len(_QUERIES)
        return str(template.render(**data))
    else: # POST
        dom_store = kw['__server__'].domStore
        postdata = kw['_params']['data']
        store_query(postdata)

        r = dom_store.query(postdata)
        out = json.dumps(r,sort_keys=True, indent=4)
        return out
    
@Route("update")
def update(kw, retval):
    method=kw['_environ']['REQUEST_METHOD']
    params = kw['_params']
    
    if method == 'GET':
        sample = ''
        if 'hist' in params:
            sample = _UPDATES[int(params['hist'])]
        data = {
            'path':'update', # XXX
            'label': 'update',
            'sample': sample,
            'link':'/',
            'link_label':'query',
            'hist': _UPDATES
        }
        template = template_loader.get_template('query.html')
        return str(template.render(**data))
    else: #POST
        dom_store = kw['__server__'].domStore
        postdata = kw['_params']['data']
        store_query(postdata, update=True)
        
        data = json.loads(postdata)
        tmp = dom_store.update(data)
        from pprint import pformat
        return pformat(tmp)

    
@Route("hist")
def hist(kw, retval):
    # method=kw['_environ']['REQUEST_METHOD']
    data = {
        'queries': _QUERIES,
        'updates': _UPDATES    
    }
    template = template_loader.get_template('history.html')    
    return template.render(**data)


@Route("api/{action}")
def api_handler(kw, retval):
    dom_store = kw['__server__'].domStore
    params = kw['_params']
    action = kw['urlvars']['action']
    
    if action not in ('query', 'update', 'delete', 'add'):
        raise Exception("404 action not found") # todo    
    if 'data' not in params:
        raise Exception("500 missing required parameter") # todo

    out = {}
    try:
        if action == 'query':
            r = dom_store.query(params['data'])
            r = list(r) # XXX
            out['data'] = r
        elif action == 'update':            
            root = json.loads(params['data']) # XXX this should probably change
            
            query = "{*, where(%s)}" % root['where']
            # print "querying:", query
            target = list(dom_store.query(query))
            
            assert len(target) == 1, "Update 'where' clause did not match any objects; try an add"
            target = target[0]
            # print "target object:"
            # print target
            
            updates = root['data']
            for k in updates.keys():
                # print "updating attribute:", k
                target[k] = updates[k]
                
            # print "updated target:"
            # print target
            
            changed = dom_store.update(target) # returns statements modified; not so useful
            
        elif action == 'add':
            data = json.loads(params['data'])
            out['added'] = dom_store.add(data)
        elif action == 'delete':
            print "XXX delete action not supported!"
            pass
            
        out['success'] = True
    except Exception, e:
        out['success'] = False
        out['error'] = str(e)

    return json.dumps(out,sort_keys=True, indent=4)

@Route('resources/{file:.+}')
def servefile(kw, retval):
    path = kw['_name']
    f = kw['urlvars']['file']    
    path = os.path.join(RESOURCE_DIR, f)
    if os.path.exists(path):
        return file(path)
    return retval

actions = {
  'http-request' : rx.route.gensequence
}

# using STORAGE_URL sets modelFactory and STORAGE_PATH
#STORAGE_URL="tyrant://localhost:1978"
#STORAGE_URL="rdf://out2.nt"
STORAGE_URL="mem://"

parser = OptionParser()
(options, args) = parser.parse_args()
if len(args) > 0:
    STORAGE_URL = args[0]
    
try:
    raccoon.run(globals())
except KeyboardInterrupt:
    print "exiting!"
