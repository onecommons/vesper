#!/usr/bin/env python

import raccoon, rx.route
from rx.python_shim import *
from optparse import OptionParser
from rx.route import Route
from raccoon import Action

import os, logging

import mako
from mako.lookup import TemplateLookup

TEMPLATE_DIR="server/templates"
RESOURCE_DIR="server/resources"

STAT_PROVIDERS = []

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
        data_store = kw['__server__'].dataStore
        postdata = kw['_params']['data']
        store_query(postdata)

        r = data_store.query(postdata, forUpdate=True)
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
        data_store = kw['__server__'].dataStore
        postdata = kw['_params']['data']
        store_query(postdata, update=True)

        data = load_data(postdata)
        tmp = data_store.update(data)
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

@Route("stats")
def stats(kw, retval):
    template = template_loader.get_template('status.html')
    tmp = [s.stats() for s in STAT_PROVIDERS]
    return template.render(data=[s.stats() for s in STAT_PROVIDERS])

@Route("api/{action}")
def api_handler(kw, retval):
    data_store = kw['__server__'].dataStore
    params = kw['_params']
    action = kw['urlvars']['action']

    if action not in ('query', 'update', 'delete', 'add'):
        raise Exception("404 action not found") # todo    
    
    # print action
    # print params
    
    out = {'action':action}
    try:
        if action == 'query':
            if 'where' not in params:
                raise Exception("500 missing required parameter")
            
            r = data_store.query(params['where'], forUpdate=True)
            out.update(r)
        elif action == 'update':
            
            updates = load_data(params['data'])
            where = params['where']
            
            query = "{ *, where(%s)}" % params['where']
            # print "querying:", query
            
            target = data_store.query(query, forUpdate=True)['results']
            # print target
            
            assert len(target) >= 1, "Update 'where' clause did not match any objects; try an add"
            target = target[0]
            # print "target object:"
            # print target

            for k in updates.keys():
                # print "updating attribute:", k
                target[k] = updates[k]

            # print "storing updated target:"
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
    tmp = json.dumps(out,sort_keys=True, indent=4)
    # print tmp
    return tmp

@Route('resources/{file:.+}')
def servefile(kw, retval):
    path = kw['_name']
    f = kw['urlvars']['file']    
    path = os.path.join(RESOURCE_DIR, f)
    if os.path.exists(path):
        return file(path)
    return retval

@Action
def displayError(kw, retVal):
    type = kw['_errorInfo']['type']
    value = kw['_errorInfo']['value']
    tb = kw['_errorInfo']['traceback']
    try:
        #work-around bug in mako.exceptions.html_error_template
        raise type, value, tb         
    except:
        return mako.exceptions.html_error_template().render()#traceback=(type,value,tb))
        
actions = {
  'http-request': rx.route.gensequence,
  'http-request-error': [displayError]
}

CONF = {
    'STORAGE_URL':"mem://",
    'actions':actions
}

parser = OptionParser()
parser.add_option("-s", "--storage", dest="storage", help="storage url")
parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False)
(options, args) = parser.parse_args()

if options.storage:
    CONF['STORAGE_URL'] = options.storage
    
f = (args and args[0] or None)
CONF = raccoon.createApp(fromFile=f, **CONF)
    
if CONF.get('REPLICATION_CHANNEL'):
    import rx.replication
    CONF.saveHistory = True
    rep = rx.replication.get_replicator(CONF.branchId, CONF.REPLICATION_CHANNEL, hosts=CONF.REPLICATION_HOSTS)
    CONF['CHANGESET_HOOK'] = rep.replication_hook
    
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
    
    STAT_PROVIDERS.append(rep)

# see if there's a log config file
serverdir = os.path.dirname(os.path.abspath(__file__))
logpath = os.path.join(serverdir, "log.conf")
if os.path.exists(logpath) and os.path.isfile(logpath):
    print "loading log config file:" + logpath
    CONF['logconfig'] = logpath
else:
    if options.verbose:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO
    log = logging.getLogger()
    log.setLevel(loglevel)
    format="%(asctime)s %(levelname)s %(name)s %(message)s"
    datefmt="%d %b %H:%M:%S"    
    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(logging.Formatter(format, datefmt))
    log.addHandler(stream)

try:
    CONF.run()
except KeyboardInterrupt:
    print "exiting!"
