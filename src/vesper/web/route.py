#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
import os.path
from urllib import url2pathname
import logging

from routes import Mapper

from vesper.app import *
from vesper.utils import defaultattrdict

log = logging.getLogger("server")

route_map = Mapper()
route_map.minimization = False

# Routes mapper maintains a route cache that must be regenerated when
# new routes are added
_routemap_dirty = True

def Route(path, route_map=route_map, **vars):
    def _route(func):
        global _routemap_dirty
        if not isinstance(func, Action):
            func = Action(func)
        route_map.connect(None, path, controller=func, **vars)
        # print "mapping %s -> %s" % (path, str(func))
        _routemap_dirty = True
        return func
    return _route

def gensequence(kw, default=None):
    "Yields an Action.  This is a generator purely due to requirements in the Action code"
    global _routemap_dirty    
    if _routemap_dirty:
        # print "generating routemap"
        route_map.create_regs()
        _routemap_dirty = False
    
    request_url = kw['_name']
    r = route_map.match(request_url, kw.get('_environ'))
    if r:
        kw['urlvars'] = defaultattrdict(r) # XXX should probably strip 'action' & 'controller'
        yield r['controller']
    elif default:
        yield default

@Action
def servefile(kw, retval):
    return _servefile(kw,retval,kw._name)

def _servefile(kw, retval, uri):
    path = url2pathname(uri)
    unauthorized = False
    server = kw.__server__
    for prefix in server.static_path:            
        filepath = os.path.join(prefix.strip(), path)
        #check to make sure the path url wasn't trying to sneak outside the path (e.g. by using "..")
        if server.secure_file_access:
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
    from mako.exceptions import TopLevelLookupException
    
    path = kw._name
    db = kw.__server__.defaultStore
    if kw.urlvars:
        if 'path' in kw.urlvars:
            path = kw.urlvars.path
        if kw.urlvars.db:
            try:
                db = kw.__server__.stores[kw.urlvars.db]
            except KeyError:
                return retval #not found
        
    try: 
        template = kw.__server__.template_loader.get_template(path)
    except TopLevelLookupException:
        #couldn't find template
        return retval #not found
    if template:
        return template.render(params=kw._params, 
                        urlvars=kw.get('urlvars',{}), 
                        request=kw, 
                        config=kw.__server__.config,
                        server=kw.__server__,
                        __=defaultattrdict(), 
                        db=db)
    else:
        return retval
