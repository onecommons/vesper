#derived from http://pythonpaste.org/webob/do-it-yourself.html#routing

import os, re
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

    def find_route(self, kw):
        for regex, controller, vars in self.routes:
            match = regex.match(kw['_name'])
            if match:
                urlvars = match.groupdict()
                urlvars.update(vars)
                kw['urlvars'] = urlvars
                return controller
        return None

@Action
def fromdatastore(kw, retval):
    import jql
    query = "{comment:* where(label='%s')}" % kw['_name'] #XXX qnames are broken 
    result = list(jql.runQuery(query, kw['__server__'].dataStore.model))
    #print result
        
    template = '<html><body>%s</body></html>'
    if result:
        return template % result[0]['comment']
    return retval

@Action
def servefile(kw, retval):
    path = kw['urlvars']['file']
    if os.path.exists(path):
        return file(path)
    return retval

routes = Router()
routes.add_route('/static/{file:.+}', servefile)    

def gensequence(kw): 
    route = routes.find_route(kw)
    if route:
        yield route
    else: #default:
        yield fromdatastore
    #yield nextaction

actions = { 'http-request' : gensequence
        }

APPLICATION_MODEL = [{ 'id' : 'a_resource', 
                      'label' : 'foo', 
                       'comment' : 'page content.'
                    }]
                    
createApp(routes=routes, actions=actions, APPLICATION_MODEL=APPLICATION_MODEL)
