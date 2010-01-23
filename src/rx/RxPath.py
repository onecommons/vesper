'''
    An implementation of RxPath.
    Loads and saves the DOM to a RDF model.

    Copyright (c) 2003 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
'''

from rx import utils
from rx.RxPathUtils import *
from rx.RxPathModel import *
from rx.RxPathSchema import *

import os.path, sys, traceback

from rx import logging #for python 2.2 compatibility
log = logging.getLogger("RxPath")

##########################################################################
## public utility functions
##########################################################################
    
def splitUri(uri):
    '''
    Split an URI into a (namespaceURI, name) pair suitable for creating a QName with
    Returns (uri, '') if it can't
    '''
    if uri.startswith(BNODE_BASE):  
        index = BNODE_BASE_LEN-1
    else:
        index = uri.rfind('#')
    if index == -1:        
        index = uri.rfind('/')
        if index == -1:
            index = uri.rfind(':')
            if index == -1:
                return (uri, '') #no ':'? what kind of URI is this?
    local = uri[index+1:]
    if not local or (not local[0].isalpha() and local[0] != '_'):
       return (uri, '')  #local name doesn't start with a namechar or _  
    if not local.replace('_', '0').replace('.', '0').replace('-', '0').isalnum():
       return (uri, '')  #local name has invalid characters  
    if local and not local.lstrip('_'): #if all '_'s
        local += '_' #add one more
    return (uri[:index+1], local)

def elementNamesFromURI(uri, nsMap):    
    predNs, predLocal  = splitUri(uri)
    if not predLocal: # no "#" or nothing after the "#" 
        predLocal = u'_'
    if predNs in nsMap:
        prefix = nsMap[predNs]
    else:        
        prefix = u'ns' + str(len(nsMap.keys()))
        nsMap[predNs] = prefix
    return prefix+':'+predLocal, predNs, prefix, predLocal

def getURIFromElementName(elem):
    u = elem.namespaceURI or ''
    local = elem.localName
    return u + getURIFragmentFromLocal(local)

def getURIFragmentFromLocal(local):
    if local[-1:] == '_' and not local.lstrip('_'): #must be all '_'s
        return local[:-1] #strip last '_'
    else:
        return local

