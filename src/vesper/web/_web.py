#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
'''
vesper.web
==========
'''
from vesper import utils
import time, sys, mimetypes

from vesper.app import RequestProcessor

try:
    from hashlib import md5 # python 2.5 or greater
except ImportError:
    from md5 import new as md5

try:
    import cPickle
    pickle = cPickle
except ImportError:
    import pickle

class HTTPRequestProcessor(RequestProcessor):
    '''
    Adds functionality for handling an HTTP request.
    '''

    statusMessages = {
        200 : "OK",
        206 : 'Partial Content',
        301 : "Moved Permanently",
        302 : "Moved Temporarily",
        304 : "Not Modified",
        400 : 'Bad Request',
        401 : 'Unauthorized',
        403 : 'Forbidden',
        404 : 'Not Found',
        426 : 'Upgrade Required',
        500 : 'Internal Server Error',
        501 : 'Not Implemented',
        503 : 'Service Unavailable',
    }

    defaultGlobalVars = RequestProcessor.defaultGlobalVars + ['_environ',
            '_uri',
            '_baseUri',
            '_responseCookies',
            '_requestCookies',
            '_responseHeaders',
    ]

    def __init__(self,*args, **kwargs):
        #add missing mimetypes that IE cares about:
        mimetypes.types_map['.ico']='image/x-icon'
        mimetypes.types_map['.htc']='text/x-component'

        super(HTTPRequestProcessor,self).__init__(*args, **kwargs)

    def handleHTTPRequest(self, kw):
        if self.requestsRecord is not None:
            self.requestsRecord.append(kw)

        #if the request name has an extension try to set
        #a default mimetype before executing the request
        name = kw['_name']
        i=name.rfind('.')
        if i!=-1:
            ext=name[i:]
            contentType=mimetypes.types_map.get(ext)
            if contentType:
                kw['_responseHeaders']['content-type']=contentType

        try:
            rc = {}
            #requestContext is used by all Requestor objects
            #in the current thread
            #rc['_environ']=kw['_environ']
            #rc['_responseHeaders'] = kw['_responseHeaders']
            #rc['_session']=kw['_session']
            assert self.requestContext
            self.requestContext.append(rc)

            self.validate_external_request(kw)

            result = self.runActions('http-request', kw)
            if result is not None:
                #if mimetype is not set, make another attempt
                if 'content-type' not in kw['_responseHeaders']:
                    contentType = self.guessMimeTypeFromContent(result)
                    if contentType:
                        kw['_responseHeaders']['content-type']=contentType

                status = kw['_responseHeaders']['_status']
                if isinstance(status, (float, int)):
                   msg = self.statusMessages.get(status, '')
                   status  = '%d %s' % (status, msg)
                   kw['_responseHeaders']['_status'] = status

                if not status.startswith('200'):
                    return result #don't set the following headers

                if (self.default_expires_in and
                    'expires' not in kw['_responseHeaders']):
                    if self.default_expires_in == -1:
                        expires = '-1'
                    else:
                        expires = time.strftime("%a, %d %b %Y %H:%M:%S GMT",
                                        time.gmtime(time.time() + self.default_expires_in))
                    kw['_responseHeaders']['expires'] = expires

                #XXX this etag stuff should be an action
                if self.use_etags:
                    resultHash = kw['_responseHeaders'].get('etag')
                    #if the app already set the etag use that value instead
                    if resultHash is None and isinstance(result, str):
                        resultHash = '"' + md5(result).hexdigest() + '"'
                        kw['_responseHeaders']['etag'] = resultHash
                    etags = kw['_environ'].get('HTTP_IF_NONE_MATCH')
                    if etags and resultHash in [x.strip() for x in etags.split(',')]:
                        kw['_responseHeaders']['_status'] = "304 Not Modified"
                        return ''

                return result
        finally:
            #import traceback;
            #traceback.print_exc()
            assert self.requestContext
            self.requestContext.pop()

        return self.default_not_found(kw)

    def requestFromEnviron(self, environ):
        import Cookie, wsgiref.util
        _name = environ['PATH_INFO'].lstrip('/')
        if not _name or _name.endswith('/'):
            _name += self.default_page_name

        _responseCookies = Cookie.SimpleCookie()
        _responseHeaders = utils.attrdict(_status="200 OK") #include response code pseudo-header
        kw = utils.attrdict(_environ=utils.attrdict(environ),
            _uri = wsgiref.util.request_uri(environ),
            _baseUri = wsgiref.util.application_uri(environ),
            _responseCookies = _responseCookies,
            _requestCookies = Cookie.SimpleCookie(environ.get('HTTP_COOKIE', '')),
            _responseHeaders= _responseHeaders,
            _name=_name
        )
        paramsdict = get_http_params(environ)
        kw.update(paramsdict)
        return kw
        
    def wsgi_app(self, environ, start_response):
        """
        A WSGI app that dispatches incoming HTTP requests to the 'http-request' action.
        
        Converts an HTTP requests into these Action keywords:

        :var _name: environ['PATH_INFO'] without leading or trailing '/' 
              or :confval:`default_page_name` if empty
        :var _uri:  wsgiref.util.request_uri(environ),
        :var _baseUri:  wsgiref.util.application_uri(environ),        
        :var _params: (a utils.attrdict) A merge of URL parameters and if a POST with form variables, 
        :var _postContent: POST requests without form variables will set this 
              with the contents of POST, otherwise None           
        :var _responseheaders: (a utils.attrdict) HTTP response headers plus `_status`
           Setting these will control the HTTP response headers sent.
        :var _responsecookies: (a Cookie.SimpleCookie)
        :var _requestcookies: (a Cookie.SimpleCookie)
        :var _environ: (a utils.attrdict) the WSGI `environ`
        """

        kw = self.requestFromEnviron(environ)
        response = self.handleHTTPRequest(kw)
        _responseHeaders = kw['_responseHeaders']
        _responseCookies = kw['_responseCookies']
        
        status = _responseHeaders.pop('_status')
        headerlist = _responseHeaders.items()
        if len(_responseCookies):
            headerlist += [('Set-Cookie', m.OutputString() )
                            for m in _responseCookies.values()]

        start_response(status, headerlist)
        if hasattr(response, 'read'): #its a file not a string
            block_size = 8192
            if 'wsgi.file_wrapper' in environ:
                return environ['wsgi.file_wrapper'](response, block_size)
            else:
                return iter(lambda: response.read(block_size), '')
        else:
            return [response]

    def guessMimeTypeFromContent(self, result):
        #obviously this could be improved,
        #e.g. detect encoding in xml header or html meta tag
        #or handle the BOM mark in front of the <?xml
        #detect binary vs. text, etc.
        if isinstance(result, (str, unicode)):
            test = result[:30].strip().lower()
        else:
            test = ''
        if test.startswith("<html") or test.startswith("<!doctype html"):
            return "text/html"
        elif test.startswith("<?xml") or test[2:].startswith("<?xml"):
            return "text/xml"
        elif self.default_mime_type:
            return self.default_mime_type
        else:
            return None

    def default_not_found(self, kw):
        kw['_responseHeaders']["content-type"]="text/html"
        kw['_responseHeaders']['_status'] = "404 Not Found"
        return '''<html><head><title>Error 404</title>
<meta name="robots" content="noindex" />
</head><body>
<h2>HTTP Error 404</h2>
<p><strong>404 Not Found</strong></p>
<p>The Web server cannot find the file or script you asked for.
Please check the URL to ensure that the path is correct.</p>
<p>Please contact the server's administrator if this problem persists.</p>
</body></html>'''

    def saveRequestHistory(self):
        if self.requestsRecord:
            requestRecordFile = file(self.requestRecordPath, 'wb')
            pickle.dump(self.requestsRecord, requestRecordFile)
            requestRecordFile.close()

    def playbackRequestHistory(self, debugFileName, out=sys.stdout):
        requests = pickle.load(file(debugFileName, 'rU'))
        import repr
        rpr = repr.Repr()
        rpr.maxdict = 20
        rpr.maxlevel = 2
        for i, request in enumerate(requests):
            verb = request['_environ']['REQUEST_METHOD']
            login = request.get('_session',{}).get('login','')
            print>>out, i, verb, request['_name'], 'login:', login
            #print form variables
            print>>out, rpr.repr(dict([(k, v) for k, v in request.items()
                                if isinstance(v, (unicode, str, list))]))

            self.handleHTTPRequest(request)

    def runWsgiServer(self, port=8000, server=None, middleware=None):
        if not server:
            from wsgiref.simple_server import make_server
            server = make_server
        if middleware:
            app = middleware(self.wsgi_app)
        else:
            app = self.wsgi_app
        httpd = server('', port, app)
        try:
            httpd.serve_forever()
        finally:
            self.runActions('shutdown')
            self.saveRequestHistory()

class UploadFile(object):

    def __init__(field):
        self._field = field

    filename = property(lambda self: self._field.filename)
    file = property(lambda self: self._field.file)
    contents = property(lambda self: self._field.value)

def get_http_params(environ):        
    '''build _params (and maybe _postContent)'''
    import cgi

    _params = {}
    _postContent = None
    getparams = utils.defaultattrdict()
    postparams = utils.defaultattrdict()

    if environ.get('QUERY_STRING'):
        forms = cgi.FieldStorage(environ=environ, keep_blank_values=1)
        for key in forms.keys():
            valueList = forms[key]
            if isinstance(valueList, list):# Check if it's a list or not
                getparams[key]= [item.value for item in valueList]
            else:
                getparams[key] = valueList.value

    if environ['REQUEST_METHOD'] == 'POST':
        forms = cgi.FieldStorage(fp=environ.get('wsgi.input'),
                                environ=environ, keep_blank_values=1)
        if forms.list is None:
            assert forms.file is not None
            _postContent = forms.file.read()
        else:
            for key in forms.keys():
                valueList = forms[key]
                if isinstance(valueList, list):# Check if it's a list or not
                    postparams[key]= [item.value for item in valueList]
                else:
                    # In case it's a file being uploaded, we save the filename in a map (user might need it)
                    if not valueList.filename:
                        postparams[key] = valueList.value
                    else:
                        postparams[key] = UploadFile(valueList)

    if getparams and postparams:
        #merge the dicts together
        for k,v in getparams.items():
            if k in postparams:
                if not isinstance(v, list):
                    v = [v]
                pv = postparams[k]
                if isinstance(pv, list):
                    v.extend(pv)
                else:
                    v.append(pv)
            postparams[k] = v
        _params = postparams
    else:
        _params = getparams or postparams

    return dict(_params=_params, _postContent=_postContent)

################ utilities for rendering html #################################
def q(s):
  '''
  Escape the string so that it can be used in double-quoted attribute values
  '''  
  return s.replace('&', '&amp;').replace('"','&quot;')

def aq(s):
  '''
  Return the given string quoted and escaped.
  '''
  if s.count('"') > s.count("'"):
    #technically html < 5 doesn't support &apos and so &#39; should be used but all
    #modern browsers do: see http://code.google.com/p/doctype/wiki/AposCharacterEntity
    return "'"+s.replace('&', '&amp;').replace("'",'&apos;')+"'"
  else:
    return '"'+s.replace('&', '&amp;').replace('"','&quot;')+'"'

def kwtoattr(kw, **merge):
  for k,v in merge.items():
    if k in kw:
      v = kw[k] + ' ' + v
    kw[k] = v 
  return ' '.join([
    ##strip _ so we can have python keywords as attributes (e.g. _class)
    (name.startswith('_') and name[1:] or name) + '='+ aq(str(value))
                                    for name, value in kw.items()])
