#derived from http://pythonpaste.org/webob/do-it-yourself.html#routing
from raccoon import *
from rx.utils import attrdict

import re
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
                kw['urlvars'] = attrdict(urlvars)
                return controller
        return None

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
    path = kw['_name']
    if os.path.exists(path):
        return file(path)
    return retval
