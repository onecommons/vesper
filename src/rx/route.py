#derived from http://pythonpaste.org/webob/do-it-yourself.html#routing
from raccoon import *
from rx.utils import defaultattrdict
import os.path
from urllib import url2pathname
import re, logging
log = logging.getLogger("server")

var_regex = re.compile(r'''
    \{          # The exact character "{"
    (\w+)       # The variable name (restricted to a-z, 0-9, _)
    (?::([^}]+))? # The optional :regex part
    \}          # The exact character "}"
    ''', re.VERBOSE)

def template_to_regex(template):
    regex = ''
    last_pos = 0
    for match in var_regex.finditer(template):
        regex += re.escape(template[last_pos:match.start()])
        var_name = match.group(1)
        expr = match.group(2) or '[^/]+'
        expr = '(?P<%s>%s)' % (var_name, expr)
        regex += expr
        last_pos = match.end()
    regex += re.escape(template[last_pos:])
    regex = '^%s$' % regex
    return regex

class Router(object):
    def __init__(self):
        self.routes = []

    def add_route(self, template, controller, **vars):
        if not isinstance(controller, Action):
            controller = Action(controller)
        self.routes.append((re.compile(template_to_regex(template)),
                            controller,
                            vars))
        return controller

    def find_route(self, kw):
        for regex, controller, vars in self.routes:
            #for n, v in httpmatches:
            #    if kw['environ'].get(n) != v:
            #        return None        
            match = regex.match(kw['_name'])
            if match:            
                urlvars = match.groupdict()
                urlvars.update(vars)
                kw['urlvars'] = defaultattrdict(urlvars)
                return controller
        return None
        
    def get_routes(self):
        return [(r[0].pattern, r[1].action) for r in self.routes]
    
routes = Router()

def Route(path, routes = routes, **vars):
    def _route(f):
        f = routes.add_route(path, f, **vars)
        return f
    return _route
    
def gensequence(kw, default=None): 
    route = routes.find_route(kw)
    if route:
        yield route
    elif default:
        yield default

@Action
def servefile(kw, retval):
    #value = kw
    #for n in name.split('.'):
    #    value = value[n]
    return _servefile(kw,retval,kw._name)

def _servefile(kw, retval, uri):
    path = url2pathname(uri)
    unauthorized = False
    server = kw.__server__
    for prefix in server.static_path:            
        filepath = os.path.join(prefix.strip(), path)        
        #print 'filepath', filepath
        #check to make sure the path url wasn't trying to sneak outside the path (e.g. by using "..")
        if server.SECURE_FILE_ACCESS:
            if not os.path.abspath(filepath).startswith(os.path.abspath(prefix)):
                unauthorized = True                
                continue
        unauthorized = False
        if os.path.exists(filepath):
            return file(filepath)

    #if unauthorized:
    #    raise UriException(UriException.RESOURCE_ERROR, uri, 'Unauthorized')                 
    return retval #not found

@Action #Route(default=True)
def servetemplate(kw, retval):
    path = kw._name
    template = kw.__server__.template_loader.get_template(path)
    if template:
        return template.render(params=kw._params, 
                        urlvars=kw.get('urlvars',{}), 
                        request=kw, 
                        config=kw.__server__.config,
                        server=kw.__server__, 
                        db=kw.__server__.dataStore)
    else:
        return retval
