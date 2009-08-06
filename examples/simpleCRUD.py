import raccoon
from string import Template
from cgi import escape
from optparse import OptionParser

try:
    import json
except ImportError:
    import simplejson as json
    
QUERY_PAGE = Template("""
<html><body>
<b>$label</b>&nbsp;|&nbsp;<a href="$link">$link_label</a>
<form action="/$path" method="post">
  <textarea cols="60" rows="30" name="data">$sample</textarea>
  <br>
  <input type="submit" value="do $label">
</form>
<a href="/hist">$hist queries in history</a>
</body></html>
""")

HIST_PAGE = Template("""
<html><body>
<b>$label</b>&nbsp;|&nbsp;<a href="$link">$link_label</a>
<form action="/$path" method="post">
  <textarea cols="60" rows="30" name="data">$sample</textarea>
  <br>
  <input type="submit" value="do $label">
</form>
</body></html>
""")

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


@raccoon.Action
def testaction(kw, retval):
    path=kw['_name']
    method=kw['_environ']['REQUEST_METHOD']
    dom_store = kw['__server__'].domStore
    params = kw['_params']
    
    if method == 'GET':
        if path == 'index':
            sample = ''
            if 'hist' in params:
                sample = _QUERIES[int(params['hist'])]
            data = {
                'path':path,
                'label':'query',
                'sample':sample,
                'link':'/update',
                'link_label':'update'
            }
            template = QUERY_PAGE
        elif path == 'update':
            sample = ''
            if 'hist' in params:
                sample = _UPDATES[int(params['hist'])]
            data = {
                'path':path,
                'label': 'update',
                'sample': sample,
                'link':'/',
                'link_label':'query'
            }
            template = QUERY_PAGE
        elif path == 'hist':
            buf = "<html><body>%d queries<hr>" % len(_QUERIES)
            for (i, q) in enumerate(_QUERIES):
                buf += "%d. <a href='/?hist=%d'>%s</a><br><br>" % (i, i, escape(q))
            buf += "<hr>%d updates<hr>" % len(_UPDATES)
            for (i,q) in enumerate(_UPDATES):
                buf += "%d. <a href='/update?hist=%d'>%s</a><br><br>" % (i, i, escape(q))
            buf += "</html></body>"
            return str(buf)
        else:
            kw['_responseHeaders']['_status'] = "404 Not Found"
            return "<html><body>Not Found</body></html>"            
        
        data['hist'] = len(_UPDATES) + len(_QUERIES)
        return template.substitute(**data)
    elif method == 'POST':
        dom_store = kw['__server__'].domStore
        postdata = kw['_params']['data']
        store_query(postdata, (path == 'update'))
        
        try:
            if path == 'index': # query
                r = dom_store.query(postdata)
                r = list(r)
                out = json.dumps(r,sort_keys=True, indent=4)
                return out
            
            elif path == 'update':
                # print "storing data:", postdata
                data = json.loads(postdata)
                dom_store.update(data)
                from pprint import pprint
                # return pprint(tmp)
                return 'success?'
                
        except Exception, e:
            err = """
            <html><body>
            <h3>Error</h3>
            <p>%s</p>
            </body></html>
            """
            kw['_responseHeaders']['_status'] = "500 Error"
            return err % e
        
actions = {
  'http-request' : [testaction]
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
