#!/usr/bin/env python

import raccoon, rx.route, rx.replication
from rx.python_shim import *
from optparse import OptionParser
from rx.route import Route
from raccoon import Action

import os

import mako
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

def load_data(data):
    try:
        import yaml
        return yaml.safe_load(data)
    except ImportError:
        return json.loads(data)

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

        data = load_data(postdata)
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
            root = load_data(params['data']) # XXX this should probably change
            
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
            data = load_data(params['data'])
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

"""
XXX disable until mako bug fixed
@Action
def displayError(kw, retVal):
    type = kw['_errorInfo']['type']
    value = kw['_errorInfo']['value']
    tb = kw['_errorInfo']['traceback']
    return mako.exceptions.html_error_template().render(traceback=(type,value,tb))
actions['http-request-error'] = [displayError]    
"""

actions = {
  'http-request': rx.route.gensequence,
}

CONF = {
    'STORAGE_URL':"mem://",
    'actions':actions,
}

parser = OptionParser()
(options, args) = parser.parse_args()
if len(args) > 0:
    execfile(args[0], globals(), CONF)

if 'REPLICATION_CHANNEL' in CONF:
    CONF['saveHistory'] = True
    rep = rx.replication.get_replicator(CONF['branchId'], CONF['REPLICATION_CHANNEL'], hosts=CONF['REPLICATION_HOSTS'])
    CONF['DOM_CHANGESET_HOOK'] = rep.replication_hook
    
    @Action
    def startReplication(kw, retVal):
        rep.start(kw.__server__)
        
    @Action
    def stopReplication(kw, retVal):
        rep.stop()
        
    def addAction(name, func):
        action_map = CONF['actions']
        if name in action_map:
            action_map[name].append(func)
        else:
            action_map[name] = [func]
            
    addAction('load-model', startReplication)
    addAction('shutdown', stopReplication)
    
try:
    app = raccoon.createApp(**CONF).run()
except KeyboardInterrupt:
    print "exiting!"
