import raccoon
from string import Template

try:
    import json
except ImportError:
    import simplejson as json

@raccoon.Action
def testaction(kw, retval):
    path=kw['_name']
    method=kw['_environ']['REQUEST_METHOD']
    dom_store = kw['__server__'].domStore
    
    if path not in ('index', 'update'):
        kw['_responseHeaders']['_status'] = "404 Not Found"
        return "<html><body>Not Found</body></html>"
        
    if method == 'GET':
        if path == 'index':
            data = {
                'path':path,
                'label':'query',
                'sample':'{*}',
                'link':'/update',
                'link_label':'update'
            }
        else:
            data = {
                'path':path,
                'label': 'update',
                'sample': '[{"id":"12345", "data":"foo bar baz"}]',
                'link':'/',
                'link_label':'query'
            }
        template = Template("""
        <html><body>
        <b>$label</b>
        <form action="/$path" method="post">
          <textarea cols="40" rows="20" name="data">$sample</textarea>
          <input type="submit" value="$label">
        </form>
        <a href="$link">$link_label</a>
        </body></html>
        """)
        return template.substitute(**data)
    elif method == 'POST':
        dom_store = kw['__server__'].domStore
        postdata = kw['_params']['data']
        
        try:
            if path == 'index': # query
                r = dom_store.query(postdata)
                r = list(r)
                out = json.dumps(r)
                return out
            
            elif path == 'update':
                print "storing data:", postdata
                data = json.loads(postdata)
                dom_store.update(data)
                tmp = dom_store.model.getStatements()
                return str(tmp)
        
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

import rx.RxPathModelTyrant
modelFactory=rx.RxPathModelTyrant.TransactionTyrantModel
#modelFactory=rx.RxPathModel.MemModel
STORAGE_PATH="localhost:1978"

try:
    raccoon.run(globals())
except KeyboardInterrupt:
    print "exiting!"
