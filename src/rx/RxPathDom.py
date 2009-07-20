'''
    An XML DOM Implementation that conforms to RxPath.
    Loads and saves the DOM to a RDF model.

    Design notes:
    Queries the underlying model as needed to build up the DOM on demand.
    The DOM is only mutable in 2 ways:

    * Resources can be added or deleted by calling appendChild or
      removeChild on the root (document) node. Adding a resource has
      no effect on the underlying model until statements that
      reference it are added. Deleting a resource removes all
      statements that are the resource is the subject of.

    * Statements can be added or deleted by calling appendChild or removeChild
      on a Resource (i.e. a Subject or Object) node. Therefore it is
      an error to try to remove an Object node. Also, it is error to
      remove a resource if there is an object with a reference to it.

    The underlying model will be modified as the DOM is modified and
    the model should only be modified by the DOM for the lifetime of
    the DOM instance. In addition, the Document class has begin(),
    commit(), and rollback() methods to allow the DOM and underlying
    model to be modified atomically.

    The Document factory methods (e.g. createElementNS) create "regular" XML
    DOM nodes which will be coerced into RDF DOM nodes when attached the
    RDF DOM via appendChild, etc.
    
    Todo:    
    * check for object references when removing a resource   
    * you can not insert a list or container item, only append 
    * ascendant axes are not treated specially as per the spec    

    Copyright (c) 2003 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
'''

from rx import RxPath, utils, DomTree
from DomTree import SplitQName, XML_NAMESPACE
from utils import NotSet
from RxPath import OBJECT_TYPE_RESOURCE, OBJECT_TYPE_LITERAL
from RxPath import RDF_MS_BASE, RDF_SCHEMA_BASE

import xml.dom
from xml.dom import NotSupportedErr, HierarchyRequestErr, NotFoundErr,IndexSizeErr
import sys, copy

from rx import logging #for python 2.2 compatibility
log = logging.getLogger("RxPath")

class Node(xml.dom.Node, object):
    nodeName = None
    nodeValue = None
    nodeType = None
    parentNode = None
    childNodes = []
    firstChild = None
    lastChild = None
    previousSibling = None
    nextSibling = None
    attributes = None
    ownerDocument = None
    namespaceURI = None
    prefix = None
    localName = None

    # DOM Level 3
    baseURI = None

    # XPath Data Model
    rootNode = None
    xpathAttributes = []    
    xpathNamespaces = []

    def insertBefore(self, newChild, refChild):
        raise HierarchyRequestErr(self.nodeName + " nodes cannot have children")
    
    def replaceChild(self, newChild, oldChild):
        raise HierarchyRequestErr(self.nodeName + " nodes cannot have children")
    
    def removeChild(self, oldChild):
        raise HierarchyRequestErr(self.nodeName + " nodes cannot have children")
    
    def appendChild(self, newChild):
        raise HierarchyRequestErr(self.nodeName + " nodes cannot have children")

    def normalize(self): pass
    
    def hasChildNodes(self):
        # Force boolean result
        return not not self.childNodes
     # DOM Level 3
    def isSameNode(self, other):
        return self is other

    def cloneNode(self, deep):
        raise NotSupportedErr("cloning RDFDom nodes")

    #RxDom additions: copied from rx.DomTree.Node    
    def _get_docIndex(self):
        '''
        correct order even after DOM has been reordered
        '''
        return DomTree.DocIndex(self)
    
    docIndex = property(_get_docIndex)

    def __cmp__(self, other):
        try:
            return cmp(self.docIndex, other.docIndex)
        except:
            raise

    def __ne__(self, other): #so __eq__ is called instead of __cmp__
        return not self.__eq__(other)
    
    def __eq__(self, other):
        '''
        Equal if both nodes are in the same position in the tree
        '''
        #this can be operation is expensive -- note the recursive calls
        if self is other:
            return True
        if not isinstance(other, Node):
            return False
        if self.nodeType != other.nodeType:
            return False
        
        if self.parentNode != other.parentNode:
            return False
        elif self.parentNode is None:
            #both nodes have parentNode == None and so both are roots:
            # apply object equality (and we already did above)
            return False
        
        #at this point we must be siblings with other
        if self.previousSibling:            
            if self.previousSibling != other.previousSibling:
                return False
        elif other.previousSibling:
            return False
        
        if self.nextSibling:            
            if self.nextSibling != other.nextSibling:
                return False
        elif other.nextSibling:
            return False
        
        return True
            
    def cmpSiblingOrder(self, other):
        '''
        Assumes the other node is a sibling 
        '''
        if not other.parentNode:
            return -1
        else:
            assert self.parentNode == other.parentNode
            return cmp(self.parentNode.childNodes.index(self), other.parentNode.childNodes.index(other))

    def getSafeChildNodes(self, stopNode): #this also serves as marker that this node is in a RDFDom tree
        return self.childNodes

    def _orderedInsert(self, key, ctor, cmpPredicate=None, lo=0, hi=None, notify=None):
        if len(self.childNodes):
            if cmpPredicate is None:
                #ugh... the default comparison logic choose the right operand's __cmp__
                #      over the left operand's __cmp__ if its an old-style
                #      class and the left is a new-style class!
                cmpPredicate = lambda x, y: x.__cmp__(y)
            index = utils.bisect_left(self.childNodes, key, cmpPredicate, lo, hi)

            if index == len(self.childNodes): #we'll be the last item
                nextSibling = None
                previousSibling = self.lastChild
            else:
                #print >>sys.stderr, key, '\nbefore', self.childNodes[index]
                nextSibling = self.childNodes[index]
                previousSibling = nextSibling.previousSibling 
                #check for duplicate                                
                if cmpPredicate(nextSibling, key) == 0:
                    if notify: 
                        notify(key)
                    raise IndexError("can't insert duplicate keys")                    
            
            newNode = ctor(key, self, nextSibling, previousSibling)
            #we need to call this after the node os created but before it's added tree
            if notify: 
                notify(newNode)
            if nextSibling:
                nextSibling.previousSibling = newNode
            if newNode.previousSibling:
                newNode.previousSibling.nextSibling = newNode
            self._childNodes.insert(index,  newNode)
            if index == 0:
               self._firstChild = newNode
            else:
               self._lastChild = self.childNodes[-1]
        else:
            newNode = ctor(key, self, None, None)
            #we need to call this after the node os created but before it's added tree
            if notify: 
                notify(newNode)
            self._firstChild = self._lastChild = newNode
            self._childNodes = [ newNode ]
        return newNode
            
    def _doAppendChild(self, childNodes, newChild):
        #note: doesn't set newChild.parentNode or self.firstChild, self.lastChild
        if not childNodes:
            newChild.previousSibling = newChild.nextSibling = None
        else:
            childNodes[-1].nextSibling = newChild
            newChild.previousSibling = childNodes[-1]
            newChild.nextSibling = None
        childNodes.append(newChild)
        
    def _doRemoveChild(self, childNodes, oldChild):
        if childNodes is not None:        
            try:
                childNodes.remove(oldChild)
            except ValueError:
                raise NotFoundErr()

            if len(childNodes):
                self._firstChild = childNodes[0]
                self._lastChild = childNodes[-1]
            else:
                self._firstChild = self._lastChild = None
                
        if oldChild.nextSibling is not None:
            oldChild.nextSibling.previousSibling = oldChild.previousSibling

        if oldChild.previousSibling is not None:
            oldChild.previousSibling.nextSibling = oldChild.nextSibling

        oldChild.nextSibling = oldChild.previousSibling = None
        oldChild.parentNode = None                            

    def getKey(self):
        '''
        This is used by the cache to return a key that uniquely
        represents this node and current state of the DOM
        '''
        #todo: return a value that isn't tied to this in-node in memory representation
        #e.g. its position in the DOM.
        if self.parentNode:
            parentKey = self.parentNode.getKey()
        else:
            parentKey = None        
        return (id(self), parentKey)

def looksLikeObject(node):
    if node.nodeType == Node.TEXT_NODE:
        return True
    elif node.nodeType == Node.ELEMENT_NODE:
        if node.nextSibling != None or node.previousSibling != None:
            #can't have any siblings
            return False        
        if hasattr(node.ownerDocument, 'globalRecurseCheck'):
            try:
                oldVal = node.ownerDocument.globalRecurseCheck
                node.ownerDocument.globalRecurseCheck = 1
                return looksLikeResource(node)
            finally:
                node.ownerDocument.globalRecurseCheck = oldVal
        else:
            return looksLikeResource(node)
    return False

def looksLikeResource(node):
    if node.nodeType == Node.ELEMENT_NODE :
        #and len(node.attributes) <= 1:
        #if node.attributes and not node.hasAttributeNS(RDF_MS_BASE, 'about'):
        #    return False #the one attribute it has is not rdf:about
        #else:
            return reduce(lambda x, y: x and looksLikePredicate(y),
                          node.childNodes, True)
    else:
        return False
    
def looksLikePredicate(node):
    if (node.nodeType == Node.ELEMENT_NODE
           and len(node.childNodes) <= 1):
        #todo validate attributes
        if len(node.childNodes) > 0: #empty predicates treated like empty literal
            return looksLikeObject(node.firstChild)
        else:        
            return True
    return False

class Element(Node):
    nodeType = Node.ELEMENT_NODE

    def _get_xpathAttributes(self):
        #there won't be any namespace attributes so we don't need to filter those out
        return self.attributes.values()
    xpathAttributes = property(_get_xpathAttributes)
    
    xpathNamespaces = [] #there won't be any namespace attributes
    
    def getAttributeNS(self, namespaceURI, localName):
        raise HierarchyRequestErr(self.nodeName + " nodes cannot have attributes")
    
    def setAttributeNS(self, namespaceURI, qualifiedName, value):
        raise HierarchyRequestErr(self.nodeName + " nodes cannot set attributes")
    
    def removeAttributeNS(self, namespaceURI, localName):
        raise HierarchyRequestErr(self.nodeName + " nodes cannot remove attributes")
    
    def hasAttributeNS(self, namespaceURI, localName):
        raise HierarchyRequestErr(self.nodeName + " nodes cannot have attributes")
    
    def getAttributeNodeNS(self, namespaceURI, localName):
        raise HierarchyRequestErr(self.nodeName + " nodes cannot have attributes")

    def setAttributeNodeNS(self, newAttr):
        raise HierarchyRequestErr(self.nodeName + " nodes cannot set attributes")

    def removeAttributeNodeNS(self, oldAttr):
        raise HierarchyRequestErr(self.nodeName + " nodes cannot remove attributes")

class Resource(Element):
    _childNodes = None
    _firstChild = None
    _lastChild = None
    __attributes = None                    
    uriNode = None
    
    def __init__(self, owner, uri):
        assert uri
        self.stringValue = self.uri = uri
        self.ownerDocument = self.rootNode = owner
        prefix = self.ownerDocument.nsRevMap[RDF_MS_BASE]
        if prefix:
            nodeName = prefix+':'+u'Description'
                
        self.nodeName = self.tagName = nodeName
        self.namespaceURI = RDF_MS_BASE
        self.prefix = prefix
        self.localName = u'Description'
                
    def getAttributeNS(self, namespaceURI, localName):
        if namespaceURI == RDF_MS_BASE and localName == 'about':
            return self.uri
        else:
            return ''

    def getAttributeNodeNS(self, namespaceURI, localName):
        if namespaceURI != RDF_MS_BASE and localName != 'about':
            return None
        
        if not self.uriNode:            
            prefix = self.ownerDocument.nsRevMap[RDF_MS_BASE]            
            self.uriNode = Attr(self, RDF_MS_BASE, prefix, u'about', self.uri)
        return self.uriNode
        
    def hasAttributeNS(self, namespaceURI, localName):
        if namespaceURI == RDF_MS_BASE and localName == 'about':
            return True
        else:
            return False

    def _get_attributes(self):
        if self.__attributes is None:
            attr = self.getAttributeNodeNS(RDF_MS_BASE, 'about')        
            self.__attributes = { (RDF_MS_BASE, u'about') : attr }
        return self.__attributes
    
    attributes = property(_get_attributes)

    def _get_childNodes(self):
        #if self._childNodes:
        #    childNodes = self._childNodes()
        #    if not childNodes: #gc'd
        #       childNodes = self.ownerDocument.childrenCache.get(self)
        #       self._childNodes = weakref.ref( childNodes )
        #    else: childNodes = childNodes()
        #hashCalc(self): self.getKey(), valueCalc: dummy(self.toPredicateNodes()), sizeCalc(value): len(value())
        if self._childNodes is None:            
            childNodes = self.toPredicateNodes()
            if childNodes:                
                self._childNodes = childNodes
                self._firstChild = self._childNodes[0]
                self._lastChild = self._childNodes[-1]
            else:
                self._firstChild = self._lastChild = None
                self._childNodes = []
        return self._childNodes 
    
    childNodes = property(_get_childNodes)
    
    def _get_firstChild(self):
        self._get_childNodes()
        fc = self._firstChild
        if fc is None:
            return None
        else:
            return fc #fc() #weakref
    
    firstChild = property(_get_firstChild)    
        
    def _get_lastChild(self):
        self._get_childNodes()
        return self._lastChild
    
    lastChild = property(_get_lastChild)

    def getKey(self):
        '''
        This is used by the cache to return a key that uniquely
        represents this node and current state of the DOM
        '''
        if self.parentNode:
            parentKey = self.parentNode.getKey()
        else:
            parentKey = None        
        return (self.uri, parentKey)
                            
    def matchName(self, namespaceURI, local):
        schema = self.ownerDocument.schema
        wantUri = namespaceURI + RxPath.getURIFragmentFromLocal(local)
        
        if schema.isInstanceOf and schema.isInstanceOf(self.uri, wantUri):
            return True
            
        #support for multiple rdf:types:        
        #note: doesn't support subproperties of rdf:type
        for n in self.childNodes:
            if (n.nodeType == Node.ELEMENT_NODE and n.localName == 'type'
                    and n.namespaceURI == RDF_MS_BASE):
                type = n.childNodes[0].uri                
                if schema.isCompatibleType(type, wantUri):
                        return True
                
        return False

    def isCompound(self):
        '''
        returns a URI reference to the type of collection or container this resource is
        (http://www.w3.org/1999/02/22-rdf-syntax-ns#List, http://www.w3.org/1999/02/22-rdf-syntax-ns#Bag, etc.)
        or None if it is not one.
        '''
        if self.uri == RDF_MS_BASE+'nil': #we're an empty list
            return RDF_MS_BASE + 'List'            
        #optimization: check this first because we put ordering predicates last
        if self.lastChild:
            if self.lastChild.stmt.predicate == RDF_MS_BASE + 'first': 
                return RDF_MS_BASE + 'List'
            
        for p in self.childNodes:
            #from the RDF/XML spec it doesn't appear that list has to specify
            #its type is a list so also look for rdf:first
            if p.stmt.predicate == RDF_MS_BASE + 'type':
                if p.stmt.object in [RDF_MS_BASE + 'List', RDF_MS_BASE + 'Bag',
                        RDF_MS_BASE + 'Alt', RDF_MS_BASE + 'Seq']:
                    return p.stmt.object
                else:
                    return None
            elif p.stmt.predicate == RDF_MS_BASE + 'first': 
                return RDF_MS_BASE + 'List'                
        return None #not a list
                        
    def cmpSiblingOrder(self, other):
        '''
        Assumes the other node is a sibling 
        '''
        assert self.parentNode == other.parentNode
        return cmp(self.uri, other.uri)

    def getModelStatements(self):
        return reduce(lambda l, p: l.extend(p.getModelStatements()) or l,
                        self.childNodes, [])

    def findPredicate(self, stmt):
        '''returns the child node that represents the statement or None if not found.'''
        #todo: if we're not a list we could use bisect
        for predicate in self.childNodes:
            if predicate.stmt == stmt:
                return predicate
        return None
        
    def replaceChild(self, newChild, oldChild):
        '''
        adds a new statement and removes an old one
        '''        
        if self.insertBefore( newChild, oldChild): #the statement wasn't inserted because it already exists, so don't remove it
            self.removeChild(oldChild)
            log.debug("removed %s" % oldChild)
        else:
            log.debug("didn't remove %s" % oldChild)
                                
    def appendChild(self, newChild):
        '''
            add a statement. Order is signficant only when adding to a RDF container or collection.
        '''
        return self.insertBefore(newChild, None)
    
    def __repr__(self):
        if self._childNodes is None:
            return "<pResourceElem at %X: <%s>, ? children>" % (
                id(self), self.uri)
        else:
            return "<pResourceElem at %X: <%s>, %d children>" % (
            id(self),
            self.uri,
            len(self._childNodes)
            )

class Subject(Resource):    
    #design notes:
    # parent is always the root
    # siblings may change as resources are added or removed from the root
    # its children are generated the first time they are accessed
    # and may change when adding or removing a statement that it is the subject of
    # anywhere in the document (i.e. modifying the children of an Object node)
        
    def __init__(self, uri, owner, next=NotSet, prev=NotSet):
        Resource.__init__(self, owner, uri)        
        self.parentNode = owner
        self.nextSibling = next
        self.previousSibling = prev
        #todo: support disconnected nodes -- add getters (and setters): if NotSet getnextresource(uri)..
        self.revision = 0
    
    def __eq__(self, other):        
        if self is other:
            return True
        if not isinstance(other, Subject):
            return False
        else:
            return self.ownerDocument == other.ownerDocument and self.uri == other.uri 
    
    def _addListItem(self, children, listID):
        stmts = self.ownerDocument.model.getStatements(listID)
        
        nextList = None
        for stmt in stmts:                      
            if stmt.predicate == RDF_MS_BASE+'first':
                #change the subject to the head of the list
                stmt = RxPath.Statement(self.uri, *stmt[1:]) 
                self._doAppendChild(children, Predicate(stmt, self, listID=listID))
            elif stmt.predicate == RDF_MS_BASE+'rest':
                if stmt.object != RDF_MS_BASE+'nil':
                    nextList = stmt.object
                if nextList == listID:
                    raise  HierarchyRequestErr('model error -- circular list resource: %s' % str(listID))
            elif stmt.predicate != RDF_MS_BASE+'type':  #rdf:type statement ok, assumes its rdf:List
                raise  HierarchyRequestErr('model error -- unexpected triple for inner list resource')
        if nextList:
            self._addListItem(children, nextList)
        
    def toPredicateNodes(self):
        '''        
        Statements are sorted by (predicate uri, object value) unless they are RDF list or containers.
        If the RDF list or container has non-membership statements (usually just rdf:type) those will appear first.
        '''
        stmts = self.ownerDocument.model.getStatements(self.uri) #we assume list will be sorted
        children = []
        containerItems = {}
        
        listItem = nextList = None        
        for stmt in stmts:
            assert stmt.subject == self.uri, `self.uri` + '!=' + `stmt.subject`
            if stmt.predicate == RDF_MS_BASE+'first':
                listItem = stmt
            elif stmt.predicate == RDF_MS_BASE+'rest':
                if stmt.object != RDF_MS_BASE+'nil':
                    nextList = stmt.object                    
            elif stmt.predicate.startswith(RDF_MS_BASE+'_'): #rdf:_n
                ordinal = int(stmt.predicate[len(RDF_MS_BASE+'_'):])
                containerItems[ordinal] = stmt            
            elif not (stmt.predicate == RDF_MS_BASE+u'type' and 
                       stmt.object == RDF_SCHEMA_BASE+u'Resource'):
                #don't include the redundent rdf:type rdfs:Resource statement
                self._doAppendChild(children, Predicate(stmt, self))

        if listItem:
            self._doAppendChild(children, Predicate(listItem, self, listID=self.uri))
        if nextList:
            self._addListItem(children, nextList)

        #add any container items in order
        ordinals = containerItems.keys()
        ordinals.sort()        
        for ordinal in ordinals:
            stmt = containerItems[ordinal]
            realPredicate = stmt.predicate
            stmt = RxPath.Statement(stmt[0], RDF_SCHEMA_BASE+u'member', *stmt[2:])
            self._doAppendChild(children, Predicate(stmt, self, listID=realPredicate))            

        return children

    def insertBefore(self, newChild, refChild):
        '''
            add a statement. Order is signficant only when adding to a RDF container or collection.
        '''
        if isinstance(newChild, DomTree.DocumentFragment):
            retVal = newChild
            for c in tuple(newChild.childNodes):
                ins = self.insertBefore(c, refChild)
                if not ins:
                    retVal = None #if any one of these inserts return None we return None            
            return retVal
        
        newChild.normalize()
        if not looksLikePredicate(newChild):
            raise HierarchyRequestErr("can't add to this resource: the child "
                                "%s doesn't look like a predicate" % newChild)
        predicateURI = RxPath.getURIFromElementName(newChild)
        #append and insertbefore establish order, if rdf:_n need to
        # (add and remove) each following statement
        #rewrite predicate based on rdf:_n or listID or bNode        
        stmtID = newChild.getAttributeNS(RDF_MS_BASE,'ID')
        #todo: if stmtID we need to update model.reifiedIDs (transactionally)
        #      and add statement triples (see Statement.reify() )
        assert not stmtID, 'rdf:ID attribute not yet supported' #todo
        datatype = newChild.getAttributeNS(RDF_MS_BASE,'datatype')
        lang  = newChild.getAttributeNS(XML_NAMESPACE,'lang')
        listID = newChild.getAttributeNS(None,'listID')
        #for concision, allow the use of a rdf:resource attribute instead of a child element
        if newChild.getAttributeNS(RDF_MS_BASE,'resource'):            
            object = newChild.getAttributeNS(RDF_MS_BASE, 'resource')
            assert object
            objectType = OBJECT_TYPE_RESOURCE            
        elif newChild.firstChild is None:
            #if no children assign an empty string as the object            
            object = ''
            objectType = datatype or lang or OBJECT_TYPE_LITERAL            
        elif newChild.firstChild.nodeType == Node.TEXT_NODE:
            object = newChild.firstChild.nodeValue
            objectType = datatype or lang or OBJECT_TYPE_LITERAL
        else:
            object = newChild.firstChild.getAttributeNS(RDF_MS_BASE, 'about')
            if not object: #no explicit URI assigned, generate a bNode now
                object = RxPath.generateBnode()
                prefix = self.ownerDocument.nsRevMap[RDF_MS_BASE]
                if prefix: qname = prefix + ':' + 'about'
                else: qname = 'about'
                newChild.firstChild.setAttributeNS(RDF_MS_BASE, qname, object)
            objectType = OBJECT_TYPE_RESOURCE
        stmt = RxPath.Statement(self.uri, predicateURI, object,
                                        objectType=objectType)
        
        retVal = self.addStatement(stmt, listID, refChild)

        if newChild.firstChild and newChild.firstChild.nodeType != Node.TEXT_NODE:
            #recursively add any statements attached to the object of this statement
            self.ownerDocument.appendChild(newChild.firstChild)
            
        return retVal
        
    def addStatement(self, stmt, listID = '', refChild = None):  
        try:
            assert stmt.subject == self.uri            
            if stmt.predicate == RDF_MS_BASE+'first':
                #we're a list so we need to follow the insert order
                #set defaults as if we're the first item in the list                 
                previousListId = None                
                if refChild:
                    #previousListId = 
                    raise NotSupportedErr(
                        "inserting items into lists not yet supported")
                    #todo: predicateNode = self._doInsertBefore()
                else:
                    for i in xrange(len(self.childNodes)-1, -1, -1):
                        #iterate through in reverse order                        
                        if self.childNodes[i].stmt.predicate == RDF_MS_BASE+'first':
                            previousListId = self.childNodes[i].listID
                            break                    
                if previousListId: #we found a previous item in the list
                    if not listID:
                        #create new node if not specified in the newChild element
                        listID = RxPath.generateBnode()
                    else:                        
                        #make sure using this list id isn't used elsewhere
                        listStmts = self.ownerDocument.model.getStatements(
                                                listID, RDF_MS_BASE+'first')
                        for listStmt in listStmts:                            
                            if stmt.object != listStmt.object:
                                raise HierarchyRequestErr("model error: "
                                "list resource %s already used" % str(listID))
                            else:                                
                                previousListStmts = self.ownerDocument.model.getStatements(
                                                previousListId, RDF_MS_BASE+'rest')
                                if previousListStmts:
                                    if previousListStmts[0].object == listID:
                                        log.debug('add statement failed: '
                                        'statement already exists: %s' % (stmt,))
                                        return None
                                    elif (previousListStmts[0].object !=
                                                  RDF_MS_BASE+'nil'):
                                        #rdf:nil gets removed below
                                        raise HierarchyRequestErr("model error:"
        "list statement %s already exists but in different order" % str(listID))
                    #append the new list node to the end of list
                    previousRestListStmt = RxPath.Statement(previousListId,
                        RDF_MS_BASE+'rest', listID,
                        objectType=OBJECT_TYPE_RESOURCE)
                    self.ownerDocument.model.addStatement(previousRestListStmt)
                else: #this statement is the first item in the list, so our subject will be head list resource
                    assert not listID or listID == self.uri, `listID` + '==' + `self.uri`
                    listID  = self.uri

                #add the Predicate node to the DOM and add statements to the model 
                if refChild:
                    raise NotSupportedErr("inserting items into lists not yet supported") #todo 
                else:
                    if previousListId:
                        #we assume must have been a <previousListId, rdf:rest, rdf:nil> statement
                        #oldPreviousRestStmt = RxPath.Statement(previousListId,
                        #                RDF_MS_BASE+'rest', RDF_MS_BASE+'nil',
                        #                    objectType=OBJECT_TYPE_RESOURCE)
                        oldPreviousRestStmts = self.ownerDocument.model.getStatements(
                                                previousListId, RDF_MS_BASE+'rest',RDF_MS_BASE+'nil')
                        assert len(oldPreviousRestStmts) == 1
                        for oldNilStmt in oldPreviousRestStmts:                            
                            self.ownerDocument.model.removeStatement( oldNilStmt )
                            
                    predicateNode = Predicate(stmt, self, None, self.lastChild,
                                                                  listID=listID)
                    self.ownerDocument._invokeAddTrigger(predicateNode)
                    self._doAppendChild(self.childNodes, predicateNode)
                    self._firstChild = self._childNodes[0]
                    self._lastChild = self._childNodes[-1]
                    #terminate the list
                    restListStmt = RxPath.Statement(listID, RDF_MS_BASE+'rest',
                        RDF_MS_BASE+'nil', objectType=OBJECT_TYPE_RESOURCE)
                    self.ownerDocument.model.addStatement(restListStmt)
                #update statement with the real subject, the listID                
                stmt = RxPath.Statement(listID,*stmt[1:])
                self.ownerDocument.model.addStatement(stmt)
            elif stmt.predicate == RDF_SCHEMA_BASE+'member':                
                if refChild:
                    raise NotSupportedErr("inserting items into lists not "
                                          "yet supported") #todo
                ordinal = 0
                for i in xrange(len(self.childNodes)-1, -1, -1):
                    #iterate through in reverse order
                    if self.childNodes[i].listID:
                        childListID = self.childNodes[i].listID
                        assert childListID.startswith(RDF_MS_BASE+'_')
                        ordinal = int(childListID[len(RDF_MS_BASE+'_'):])
                        break
                ordinal += 1                
                if listID:
                    assert listID.startswith(RDF_MS_BASE+'_'), ('invalid '
                                'listID resource: '+ listID)
                    listIDordinal = int(listID[len(RDF_MS_BASE+'_'):])
                    assert listIDordinal >= ordinal, ('out of order listID '
                                'resource '+ listID + ', expected:', ordinal)
                listID = listID or RDF_MS_BASE+'_'+str(ordinal + 1)
                containerStmts = self.ownerDocument.model.getStatements(
                                stmt.subject, listID)
                if containerStmts:
                    if len(containerStmts) > 1:
                        raise  HierarchyRequestErr("model error: %s is used"
    "more than once for the same container %s" % str(listID), str(stmt.subject))
                    if containerStmts[0].object == stmt.object:
                        log.debug(
                    'add statement failed: statement already exists: %s'%(stmt,))
                        return None
                    else:
                        raise HierarchyRequestErr("model error: container "
            "statement %s already exists but in different order" % str(listID))

                predicateNode = Predicate(stmt, self,
                                          None, self.lastChild, listID=listID)
                self.ownerDocument._invokeAddTrigger(predicateNode)
                self._doAppendChild(self.childNodes, predicateNode)
                self._firstChild = self._childNodes[0]
                self._lastChild = self._childNodes[-1]
                                
                #update statement with the real predicate, the listID
                stmt = RxPath.Statement(stmt[0], listID, *stmt[2:])
                self.ownerDocument.model.addStatement(stmt)                
            else: #regular statement
                if self.childNodes and self.childNodes[-1].stmt.predicate in [
                   RDF_MS_BASE + 'first', RDF_SCHEMA_BASE + 'member' ]:
                    # the resource is a container or rdf list so insert
                    # this non-ordering statement before any of those                    
                    for i in xrange(len(self.childNodes) ):
                        #find the first ordering predicate
                        if self.childNodes[i].stmt.predicate in [
                            RDF_MS_BASE+'first', RDF_SCHEMA_BASE+'member']:
                            hi = i-1
                            break
                else:
                    hi = None

                predicateNode = self._orderedInsert(stmt, Predicate,
                                lambda x, y: cmp(x.stmt, y), hi = hi,
                                notify=self.ownerDocument._invokeAddTrigger)
                self.ownerDocument.model.addStatement(stmt)

            self.revision += 1        
            self.ownerDocument.revision += 1
            return predicateNode 

        except IndexError:
            #thrown by _orderedInsert: statement already exists in the model
            #(but note that this can happen when the stmt exists globally
            #   but we are trying to add it to specific context)            
            log.debug('add statement failed: statement already exists: '
                                                                    +str(stmt))
            return None 

    def removeChild(self, oldChild, removingResource=False):
        '''
            removes the statement identified by the child Predicate node oldChild.
            If the predicate is rdf:first then previous and next list item statements are adjusted.
            (Note: In the case of rdfs:member, sibling rdf:_n statements are not adjusted.)            
        '''
        if not removingResource and self.ownerDocument.removeTrigger:
           self.ownerDocument.removeTrigger(oldChild)
        
        if oldChild.stmt.predicate == RDF_MS_BASE+'first': #if a list item
            assert oldChild.listID
            #check if there's item in the list preceeding this one
            previousListItem = oldChild.previousSibling            
            while previousListItem:
                if previousListItem.listID:                                                            
                    break
                previousListItem = previousListItem.previousSibling

            #now check if an item followed this one                     
            restObject = RDF_MS_BASE+'nil'
            nextListItem = oldChild.nextSibling
            while nextListItem:
                if nextListItem.listID:
                    #there's an item after this one
                    restObject = nextListItem.listID
                    break
                nextListItem = nextListItem.nextSibling

            if previousListItem:
                #first, remove the statement that asserts this item follows it
                previousRestListStmt = RxPath.Statement(
                        previousListItem.listID, RDF_MS_BASE+'rest',
                        oldChild.listID, objectType=OBJECT_TYPE_RESOURCE)                
                self.ownerDocument.model.removeStatement( previousRestListStmt )
                    
            #remove the rdf:rest triple for this item
            oldRestListStmt = RxPath.Statement(oldChild.listID,
                RDF_MS_BASE+'rest', restObject, objectType=OBJECT_TYPE_RESOURCE)            
            self.ownerDocument.model.removeStatement( oldRestListStmt )
            
            if previousListItem:                
                #add a statement that linking the previous item
                #with the following item or with rdf:nill
                newRestListStmt = RxPath.Statement(previousListItem.listID,
                    RDF_MS_BASE+'rest', restObject, objectType=OBJECT_TYPE_RESOURCE)
                self.ownerDocument.model.addStatement(newRestListStmt)
            #else: no previous item.
            #      it looks like according to the rdf syntax spec sect. 7.2.19
            #      that we should remove this resource and replace it with
            #      rdf:nil, but that's way too much work
                    
            #the subject of the item is really listID, set that so we can
            #remove the correct statement below
            oldStmt = RxPath.MutableStatement(*oldChild.stmt)            
            oldStmt.subject = oldChild.listID        
        elif oldChild.stmt.predicate == RDF_SCHEMA_BASE+'member':
            #restore the orginal predicate before removing the statement from the model 
            assert oldChild.listID
            oldStmt = RxPath.MutableStatement(*oldChild.stmt)
            oldStmt.predicate = oldChild.listID
            #we don't bother reordering the sibling rdf:_n because their order
            #is still preserved after removing this on
            #(if not monotonic)
        else:
            oldStmt = oldChild.stmt
            
        oldStmt = RxPath.Statement(*oldStmt) #must be read-only 
        self.ownerDocument.model.removeStatement(oldStmt) #handle exception here?

        self._doRemoveChild(self._childNodes, oldChild)        
        self.revision += 1
        self.ownerDocument.revision += 1

class Object(Resource):
    #design notes: an object's siblings are always null, its parent never changes,
    # and its children will be synchronized when its source Subject changes
    # adding or removing children will cause the source Subject children to be updated
    
    def __init__(self, uri, parentNode):
        Resource.__init__(self, parentNode.ownerDocument, uri)
        self.source = self.ownerDocument.findSubject(uri)
        if not self.source:
            #not found, uri must be new
            self.source = self.ownerDocument.addResource(uri) 
        self.revision = self.source.revision
        self.parentNode = parentNode
        self._checkIfInLoop()        

    def _checkIfInLoop(self):        
        self.inLoop = None
        parent = self.parentNode
        while parent:
            if getattr(parent, 'uri', None) == self.uri: #circular reference!
                self.inLoop = parent
                break
            parent = parent.parentNode

    def toPredicateNodes(self):                
        children = []    
        for node in self.source.childNodes:
            self._doAppendChild(children, RecursivePredicate(node, self))            
        return children

    def _get_childNodes(self):
        if self.ownerDocument.globalRecurseCheck:
            return self.getSafeChildNodes(self.rootNode)
        else:
            self.checkAttributes() #may update childNodes
            return super(Object, self)._get_childNodes()
            
    childNodes = property(_get_childNodes) #overrides aren't called so we need to redefine property

    def removeChild(self, oldChild):
        '''
            removes the statement identified by the child Predicate node oldChild.
        '''
        #we're an recursive node, so find the Subject resource:
        predicate = self.source.findPredicate( oldChild.stmt)
        self.source.removeChild(predicate)

    def insertBefore(self, newChild, refChild):
        '''
            add a statement. Order is signficant only when adding to a RDF container or collection.
        '''
        #we're an recursive node, so find the Subject resource:        
        return self.source.insertBefore(newChild, refChild)
        
    def getSafeChildNodes(self, stopNode):
        '''
        Called by RxPath instead of childNodes when in "circularity checked" mode
        '''
        self.checkAttributes()
        if self.inLoop:            
            parent = self.parentNode
            while parent and parent != stopNode:
                if getattr(parent, 'uri', None) == self.uri: #circular reference! -- stop                
                    return []
                parent = parent.parentNode
        return super(Object, self)._get_childNodes()
                
    def checkAttributes(self):
        if self.revision < self.source.revision: 
            self._checkIfInLoop()            
            if self._childNodes is not None:
                if 1:#todo self.isCompound(): 
                    self._updateListNodes()
                else:
                    self._updateChildNodes()                
            self.revision = self.source.revision #add doc rev to the xpath and action cache keys?

    def isCompound(self):
        return self.source.isCompound()
    
    def __eq__(self, other):        
        if self is other:
            return True
        if not isinstance(other, Object):
            return False
        else:
            return self.parentNode == other.parentNode and self.uri == other.uri 

    def __repr__(self):
        return "!%s parent id: %X" % (Resource.__repr__(self), id(self.parentNode))
        #return "!" + Resource.__repr__(self) + " parent id: " + (id(self.parentNode))
        
    def _updateListNodes(self):
        '''
        synchronize list children with the source
        '''        
        #list nodes are unordered so we have to copy the whole child list
        newChildren = []
        for newChild in self.source.childNodes:
            found = None
            for i in xrange(len(self._childNodes)):
                oldChild = self._childNodes[i]                
                if newChild.stmt.object == oldChild.stmt.object:
                    found = oldChild #preserve old node
                    del self._childNodes[i]
                    break
            if found:
                child = found
            else:
                child = RecursivePredicate(newChild, self)
            self._doAppendChild(newChildren, child)

        #any nodes left should be removed 
        for child in self._childNodes:
            child._parentNode = child.nextSibling = child.previousSibling = None
            
        self._childNodes = newChildren
        if self._childNodes:        
            self._firstChild = self._childNodes[0]
            self._lastChild = self._childNodes[-1]
        else:
            self._firstChild = self._lastChild = None
            
    def _updateChildNodes(self):
        '''
        synchronize children with the source
        '''
        def getNextOrAppend():
            try:
                    latestOld = old
                    latestOld = oldIter.next()
                    return latestOld, latestOld
            except StopIteration: #no more old nodes
                    try:
                        while 1:
                            new = newIter.next()
                            newNode = RecursivePredicate(new, self)
                            #append newNode to the latestNode
                            if latestOld is not None:
                                latestOld.nextSibling = newNode
                            newNode.previousSibling = latestOld
                            newNode.nextSibling = None
                            
                            latestOld = newNode
                    except StopIteration:                    
                        return None, latestOld
        
        def getNextOrRemove():
            try: 
                    return newIter.next(), old
            except StopIteration: #no more new nodes
                    try:
                        last = old.previousSibling
                        toRemove = old     
                        while 1:                            
                            self._doRemoveChild(None, toRemove )
                            toRemove = oldIter.next()                            
                    except StopIteration:
                        return None, last
        
        newIter = iter(self.source.childNodes)
        oldIter = iter(self._childNodes)
        #note: we only update next/previousSibling but not _childNodes so that
        #we don't mess up the child list iterator
        old = None
        old, last = getNextOrAppend() 
        if old is not None:
            new, last = getNextOrRemove()
        if old is not None and new is not None:                            
            while 1:
                if new > old:        
                    self._doRemoveChild(None, old)
                    old, last = getNextOrAppend() 
                    if old is None:
                        break #we're done
                elif new < old:
                    #insert new node before the old
                    newChild = RecursivePredicate(new, self)
                    newChild.nextSibling = old
                    newChild.previousSibling = old.previousSibling
                    if old.previousSibling is not None:
                        old.previousSibling.nextSibling = newChild
                    old.previousSibling = newChild
                                            
                    new, last = getNextOrRemove()
                    if new is None:
                        break #we're done
                else: #equal, increment both
                    old, last = getNextOrAppend() 
                    if old is None:
                        break #we're done
                    new, last = getNextOrRemove()
                    if new is None:
                        break #we're done
            self._childNodes = [ last ]            
            while last._previousSibling:
                last = last._previousSibling
                self._childNodes.insert(-1, last)
            self._firstChild = self._childNodes[0]
            self._lastChild = self._childNodes[-1]                    
        else: #empty list
            self._childNodes = []
            self._firstChild = self._lastChild = None

class BasePredicate(Element):
    __attributes = None
    
    builtInAttr = { 
                    (None, u'listID') : 'self.listID',
                    (None, u'uri') : 'self.stmt.predicate',
                    (XML_NAMESPACE, u'lang'): "self.lang",
                    (RDF_MS_BASE, u'datatype') : 'self.datatype',
                  }
    
    #design notes:
    #  since statements are immutable, it always has one child (the object) and it never changes
    #  parent will be set to None if the stmt is removed from the model
    #  its siblings may change when statements with the same subject are added or removed
    #  how this change happens depends on whether is a Predicate or a RecursivePredicate
    def __init__(self,stmt, parent, listID):
        self.ownerDocument = self.rootNode = parent.ownerDocument        
        self.stmt = stmt
        self.listID=listID
        
        qname, self.namespaceURI, self.prefix, self.localName = \
               RxPath.elementNamesFromURI(stmt.predicate, self.ownerDocument.nsRevMap)
        self.nodeName = self.tagName = qname
                
        self._parentNode = parent
        if stmt.objectType == OBJECT_TYPE_RESOURCE:
            self.firstChild = self.lastChild = Object(stmt.object, self)            
        else:
            self.firstChild = self.lastChild = Text(stmt.object, self)            
        self.childNodes = [ self.firstChild ] 

    def insertBefore(self, newChild, refChild):
        raise HierarchyRequestErr("Predicate node (%s) cannot add or remove its child" % self.nodeName)
    
    def replaceChild(self, newChild, oldChild):
        raise HierarchyRequestErr("Predicate node (%s) cannot add or remove its child" % self.nodeName)
    
    def removeChild(self, oldChild):
        raise HierarchyRequestErr("Predicate node (%s) cannot add or remove its child" % self.nodeName)
    
    def appendChild(self, newChild):
        raise HierarchyRequestErr("Predicate node (%s) cannot add or remove its child" % self.nodeName)

    def getModelStatements(self):
        '''
        Returns a list of statements this Predicate Element represents
        as they appear in model. This will be different from the stmt
        property in the case of RDF lists and container predicates.
        Usually one statement but may be up to three if the Predicate
        node is a list item.
        '''
        if self.listID:
            stmt = RxPath.Statement(*self.stmt)
            if self.stmt.predicate == RDF_SCHEMA_BASE+'member':
                #replace predicate with listID
                stmt = RxPath.Statement(stmt[0], self.listID, *stmt[2:])
                return (stmt,)
            else:
                #replace subject with listID
                stmt = RxPath.Statement(self.listID, *stmt[1:])                
                listStmts = [ stmt ]
                if self.previousSibling and self.previousSibling.listID:
                    listStmts.append( RxPath.Statement(
                    self.previousSibling.listID, RDF_MS_BASE+'rest', self.listID,
                    objectType=OBJECT_TYPE_RESOURCE, scope=stmt.scope))
                if self.nextSibling is None:
                    listStmts.append( RxPath.Statement( self.listID,
                        RDF_MS_BASE+'rest', RDF_MS_BASE+'nil',
                        objectType=OBJECT_TYPE_RESOURCE, scope=stmt.scope))
                return tuple(listStmts)
        else:
            return (self.stmt, )

    def getKey(self):
        '''
        This is used by the cache to return a key that uniquely
        represents this node and current state of the DOM
        '''
        stmts = self.getModelStatements()
        unique = [stmts[0].predicate] + [x.subject for x in stmts[1:]]
        if self.parentNode:
            parentKey = self.parentNode.getKey()
        else:
            parentKey = None
        return (tuple(unique), parentKey)
            
    def cmpSiblingOrder(self, other):
        '''
        Assumes the other node is a sibling 
        '''
        assert self.parentNode == other.parentNode
        if self.listID: #we're a list so compare based on the order in the DOM
            return super(BasePredicate, self).cmpSiblingOrder(other)
        else:
            return cmp(self.stmt[1:4], other.stmt[1:4])

    def __eq__(self, other):        
        if self is other:
            return True
        if not isinstance(other, BasePredicate):
            return False
        else:
            return (self.parentNode == other.parentNode and
                self.stmt[1:4] == other.stmt[1:4] and self.listID == other.listID)

    def getAttributeNS(self, namespaceURI, localName):
        toEval = self.builtInAttr.get( (namespaceURI, localName) )
        if toEval:
            return eval(toEval) or u''
        else:
            return u''

    def hasAttributeNS(self, namespaceURI, localName):
        toEval = self.builtInAttr.get( (namespaceURI, localName) )
        if toEval and eval(toEval):        
            return True
        else:
            return False

    def getAttributeNodeNS(self, namespaceURI, localName):
        toEval = self.builtInAttr.get( (namespaceURI, localName) )
        if toEval:            
            node = getattr(self, localName + 'Node', None)
            if not node:
                prefix = self.ownerDocument.nsRevMap.get(namespaceURI, u'')                
                value = eval(toEval)
                if value:
                    node = Attr(self, namespaceURI, prefix, localName, value)
                    setattr(self, localName + 'Node', node)
                else:
                    return None
            return node                
        return None
    
    def _get_attributes(self):
        if self.__attributes is None:
            self.__attributes = dict([
                ((namespaceURI, localName),
                    self.getAttributeNodeNS(namespaceURI, localName))
                for ((namespaceURI, localName),toEval)
                  in self.builtInAttr.items() if eval(toEval)
              ])
        return self.__attributes
               
    attributes = property(_get_attributes)
    
    def _get_lang(self):        
        if len(self.stmt.objectType) > 1 and self.stmt.objectType.find(':') == -1:
            return unicode(self.stmt.objectType)
        else:
            return None

    lang = property(_get_lang)

    def _get_datatype(self):
        if self.stmt.objectType.find(':') > -1:
            return unicode(self.stmt.objectType)
        else:
            return None

    datatype = property(_get_datatype)

    def matchName(self, namespaceURI, local):
        return self.ownerDocument.schema.isCompatibleProperty(self.stmt.predicate,
                namespaceURI + RxPath.getURIFragmentFromLocal(local))            
        
    #work around for a 'bug' in _conversions.c, unlike Conversion.py (and object_to_string),
    #node_descendants() doesn't check for a stringvalue attribute
    def _get_stringValue(self):
        val = ''
        for n in self.childNodes:
            if hasattr(n, 'stringValue'):
                val += n.stringValue
            else:
                assert n.nodeType == Node.TEXT_NODE
                val += n.data
        return val

    stringValue = property(_get_stringValue)
    
    def __repr__(self):
        object = self.stmt.object
        if len(object) > DomTree.CDATA_REPR_LIMIT:
            object = object[:DomTree.CDATA_REPR_LIMIT] + '...'
        if isinstance(object, unicode):
            object = object.encode('utf8') 

        return "<pPredicateElem at %X: %s %s list %s>" % (
            id(self), repr(self.nodeName), object,self.listID)
    
class Predicate(BasePredicate):
    #design notes:
    #  parent is always a Subject node
    #  the parent will update its sibling property when an adjacent sibling is inserted or removed
    #  or set the parentNode to None if the statement is removed

    def __init__(self,stmt, parent, next=NotSet, prev=NotSet, listID=None):
        BasePredicate.__init__(self,stmt, parent, listID)
        self._nextSibling = next
        self._previousSibling = prev
        self.revision = 0
        

    def _get_parentNode(self):
        return self._parentNode

    def _set_parentNode(self, value):        
        assert value is None #only should happen when removing this statement
        self.revision += 1
        self._parentNode = value
        
    parentNode = property(_get_parentNode, _set_parentNode)
    
    def _get_nextSibling(self):
        if self._nextSibling is NotSet: 
            raise NotSupportedErr("disconnected nodes not yet supported")
            nextStmt = self.ownerDocument.model.getNextStatement(self.stmt)
            if nextStmt:
                self._nextSibling = Predicate( nextStmt, self._parentNode)
            else:
                self._nextSibling = None
        
        return self._nextSibling

    def _set_nextSibling(self, value):        
        self.revision += 1
        self._nextSibling = value
        
    nextSibling = property(_get_nextSibling, _set_nextSibling)

    def _get_previousSibling(self):
        if self._previousSibling is NotSet: #since None is a valid value
            raise NotSupportedErr("disconnected nodes not yet supported")            
            prevStmt = self.ownerDocument.model.getPrevStatement(self.stmt)
            if prevStmt:
                #the new nodes will be different the one in _parentNode.childNodes but that's ok
                self._previousSibling = Predicate( prevStmt, self._parentNode) 
            else:
                self._previousSibling = None
        
        return self._previousSibling

    def _set_previousSibling(self, value):        
        self.revision += 1
        self._previousSibling  = value
    
    previousSibling = property(_get_previousSibling, _set_previousSibling)
        
class RecursivePredicate(BasePredicate):
    #design notes:
    #  always has one child and it never changes
    #  parent will be set to None if the stmt is removed from the model
    #  its siblings may change when statements with the same subject are added or removed
    #  this will happen directly if its parent is a Subject node    
    #  or if its parent is an Object node
    #  its siblings will be regenerated when the document changes        
    def __init__(self, source, parent):
        self.source = source 
        self.revision = self.source.revision #needs to be set before calling base __init__
        BasePredicate.__init__(self,source.stmt, parent, source.listID)
                
    def _get_parentNode(self):
        self.checkAttributes()
        return self._parentNode

    parentNode = property(_get_parentNode)
    
    def _get_nextSibling(self):
        self.checkAttributes()
        return self._nextSibling

    def _set_nextSibling(self, value):        
        self._nextSibling = value
        
    nextSibling = property(_get_nextSibling, _set_nextSibling)
    
    def _get_previousSibling(self):
        self.checkAttributes()        
        return self._previousSibling
    
    def _set_previousSibling(self, value):        
        self._previousSibling  = value
    
    previousSibling = property(_get_previousSibling, _set_previousSibling)
    
    def checkAttributes(self):
        if self.revision < self.source.revision:
            if self.source.parentNode is None:
                #our statement's been removed from the model
                self._previousSibling = self._nextSibling = self._parentNode = None
            else:
                #this will force the parent to regenerate its child list and thus update node
                self._parentNode.childNodes 
            self.revision = self.source.revision

    def __repr__(self):
        return "!" + BasePredicate.__repr__(self) + " parent id: " + `id(self.parentNode)`
            
class Attr(Node):    
    nodeType = Node.ATTRIBUTE_NODE
    ownerElement = None
    
    def __init__(self, parent, namespaceURI, prefix, localName, value):
        if prefix:
            nodeName = prefix+u':' + localName
        else:
            nodeName = localName
        self.nodeName = self.name = nodeName
        self.namespaceURI = namespaceURI
        self.prefix = prefix
        self.localName = localName
        self.nodeValue = self.value = unicode(value)
        self.ownerElement = None
        # XPath Data Model
        self.parentNode = None

        self.ownerElement = self.parentNode = parent
        self.ownerDocument = self.rootNode = parent.ownerDocument
        self.baseURI = parent.baseURI        
        return

    def cmpSiblingOrder(self, other):
        if other.nodeType == Node.ATTRIBUTE_NODE:
            return 0 #attributes are unordered, so all siblings are equal
        else: #attributes come before child nodes
            return -1

    def getKey(self):
        '''
        This is used by the cache to return a key that uniquely
        represents this node and current state of the DOM
        '''
        if self.parentNode:
            parentKey = self.parentNode.getKey()
        else:
            parentKey = None        
        return ((self.namespaceURI, self.localName, self.nodeValue), parentKey)
        
    def __repr__(self):
        return "<pAttr at %X: name %s, value %s>" % (id(self),
                                                     repr(self.nodeName),
                                                     repr(self.nodeValue))   

class Text(Node):
    '''
    Text nodes can only appear as object nodes and thus have no siblings
    Their text value is immutable
    '''
    nodeType = Node.TEXT_NODE
    nodeName = u'#text'

    def __init__(self, data, parent):
        if not isinstance(data, unicode):
            data = unicode(data, 'utf8')
        self.nodeValue = self.data = data
        self.parentNode = parent
        self.ownerDocument = self.rootNode = parent.ownerDocument

    def getKey(self):
        '''
        This is used by the cache to return a key that uniquely
        represents this node and current state of the DOM
        '''
        if self.parentNode:
            parentKey = self.parentNode.getKey()
        else:
            parentKey = None        
        return (self.nodeValue, parentKey)

    def __repr__(self):
        if len(self.data) > DomTree.CDATA_REPR_LIMIT:
            data = self.data[:DomTree.CDATA_REPR_LIMIT] + '...'
        else:
            data = self.data
        return "<p%s at %X: %s>" % (self.__class__.__name__, id(self), `data`)

class Document(DomTree.Document, Node): #Note: DomTree.Node will always be invoked before this module's Node class
    '''
    Note: The factory functions create regular XML DOM nodes that create
    resources and statements when attached via appendChild(), etc.
    '''

    _childNodes = None
    _firstChild = None
    _lastChild = None
    globalRecurseCheck = False
    addTrigger = None
    removeTrigger = None
    newResourceTrigger = None
        
    nextIndex = 0 #never used
    defaultNsRevMap = { RDF_MS_BASE : 'rdf', RDF_SCHEMA_BASE : 'rdfs' }
    
    def __init__(self, model, nsRevMap = None, modelUri=None,
                        schemaClass = RxPath.defaultSchemaClass,graphManager=None):
        self.rootNode = self
        self.ownerDocument = self #todo: this violates the W3C DOM spec but fixes some 4suite bugs 
        self.model = model
        self.subjectDict = {}
        self.nsRevMap = nsRevMap or self.defaultNsRevMap.copy()
        if self.nsRevMap.get(RDF_MS_BASE) is None:
            self.nsRevMap[RDF_MS_BASE] = 'rdf'
        if self.nsRevMap.get(RDF_SCHEMA_BASE) is None:
            self.nsRevMap[RDF_SCHEMA_BASE] = 'rdfs'

        if modelUri: 
            self.stringValue=self.modelUri=modelUri
        else:
            self.stringValue =self.modelUri=RxPath.generateBnode()

        self.revision = 0
        
        self.graphManager = graphManager
        if graphManager:
            assert isinstance(graphManager, RxPath.Model)
            self.model = graphManager
                    
        self.schemaClass = schemaClass
        #we need to set the schema up now so that the schema from the model isn't
        #added as part of a transaction that may get rolled by
        self.schema = schemaClass(self.model)
        self.schema.setEntailmentTriggers(self._entailmentAdd, self._entailmentRemove)
        if isinstance(self.schema, RxPath.Model):
            self.model = self.schema
            self.findCompabilityStatements = False
        
    def __cmp__(self, other):
        if self is other:
            return 0
        elif other.ownerDocument == self:
            return -1
        else:
            getKey = getattr(other.ownerDocument or other, 'getKey', lambda: -1)
            return cmp(self.getKey(), getKey())

    def __eq__(self, other):
        if self is other:
            return True
        elif not isinstance(other, Document):
            return False
        else:
            return self.getKey() == other.getKey()
        
    def _get_childNodes(self):
        if self._childNodes is None:
            self._toSubjectNodes()
            assert self._childNodes is not None
        return self._childNodes 
    
    childNodes = property(_get_childNodes)

    def _get_firstChild(self):
        self._get_childNodes()
        return self._firstChild
    
    firstChild = property(_get_firstChild)    
        
    def _get_lastChild(self):
        self._get_childNodes()
        return self._lastChild
    
    lastChild = property(_get_lastChild)

    def _invokeAddTrigger(self, node):
        if self.addTrigger and isinstance(node, Node):
            self.addTrigger(node)

    def _toSubjectNodes(self):                
        children = []
        objects = {}
        lists = {}
        lastSubject = None
        islist = False
        
        #don't include rdf:List resources as a Subject node
        for stmt in self.model.getStatements():
            #assumes statements are sorted properly            
            if stmt.subject != lastSubject:                
                if lastSubject and not islist:
                    s = Subject(lastSubject, self)
                    self._doAppendChild(children, s)
                    self.subjectDict[lastSubject] = s
            
                lastSubject = stmt.subject
                islist = False
            
            if (stmt.object == RDF_MS_BASE+'List' and stmt.predicate ==
                RDF_MS_BASE+'type' or stmt.predicate == RDF_MS_BASE+'first'):
                lists.setdefault(stmt.subject, 1)
                islist = True #in case we're not inferring the type
                    
            if stmt.predicate == RDF_MS_BASE+'rest':
                lists[stmt.object] = 0 #not at the head of the list
            elif stmt.objectType == OBJECT_TYPE_RESOURCE:
                objects[stmt.object] = 1
                
        if lastSubject and not islist:
            s = Subject(lastSubject, self)
            self._doAppendChild(children, s)
            self.subjectDict[lastSubject] = s

        self._childNodes = children
        if self._childNodes:
            self._firstChild = self._childNodes[0]
            self._lastChild = self._childNodes[-1]
        else:
            self._firstChild = self._lastChild = None            
        
        for uri, head in lists.items():
            if head:
                subjectNode = self._orderedInsert(uri, Subject, lambda x, y: cmp(x.uri, y))        
                self.subjectDict[uri] = subjectNode

        #make sure all resources included as children, even those that just appear as an object
        for uri in objects:
            if uri not in self.subjectDict:
                subjectNode = self._orderedInsert(uri, Subject, lambda x, y: cmp(x.uri, y))        
                self.subjectDict[uri] = subjectNode                

    def findSubject(self, uri):
        self.childNodes #make sure childNodes exist
        s = self.subjectDict.get(uri)
        if s: return s
        #todo: sometime subjectDict gives us a false negative
    
        #todo for this to work:
        # need to make Subject.next, prevSibling on demand
        # when creating _childNodes use the subjects stored in WeakValueDictionary, then clear it out
        #if self._childNodes is None:
        #  subject = self.WeakValueDictionary[uri].get()
        #  if not subject:
        #      subject = Subject(uri, self)
        #      self.WeakValueDictionary[uri] = weakref(subject)
        
        #children are always sorted so we can use bisect
        #ugh... the default comparison logic choose the right operand's __cmp__
        #      over the left operand's __cmp__ if its an old-style class and the left if its is a new-style class!
        index = utils.bisect_left(self.childNodes, uri, lambda x, y: cmp(x.uri, y))
        if index == len(self.childNodes):
            return None
        node = self.childNodes[index]
        if node.uri != uri: 
            return None    
        else:
            #print 'found subject in not in dict!', uri #todo: why is this happening?
            return node

    def getModelStatements(self):
        return reduce(lambda l, p: l.extend(p.getModelStatements()) or l,
                        self.childNodes, [])
                
    def normalize(self): pass

    def replaceChild(self, newChild, oldChild):
        '''
        Adds a new resource and removes an old one.
        '''
        if self.insertBefore( newChild, oldChild): #resource might already exist and so will not be re-added
            self.removeChild(oldChild)
    
    def removeChild(self, oldChild, removeListObjects=False):
        '''
        Removes all the statements that the resource identified by the node oldChild.
        If the optional removeListObjects parameter is True then objects that are lists will be removed also.
        (Don't use this option if more than one statement refer to the list.)
        '''
        #todo: what if the resource is reference by an object? -- model should throw an exception?        
        if self.removeTrigger:
           self.removeTrigger(oldChild)

        del self.subjectDict[oldChild.uri]
        
        for predicate in tuple(oldChild.childNodes):
            #log.debug("removing statement " + str(predicate.stmt))
            list = None
            if removeListObjects:
                #if the object is a list 
                isCompound = getattr(predicate.childNodes[0], 'isCompound', None)
                if isCompound and isCompound() == RDF_MS_BASE + 'List':
                    list = predicate.childNodes[0]
            oldChild.removeChild(predicate, removingResource=True)
            if list: #recursively remove the next item in the list
                self.removeChild(list.source, True)

        #self.model.removeResource(oldChild.uri)
        self._doRemoveChild(self._childNodes, oldChild)        
        self.revision += 1

    def appendChild(self, newChild):
        '''
            Add a resource. Order is insignficant.
        '''
        return self.insertBefore(newChild, None)
    
    def insertBefore(self, newChild, refChild):
        '''
            Add a resource. Order is insignficant.
            Raises IndexError if the resource is already part of the model.
        '''
        if isinstance(newChild, DomTree.DocumentFragment):
            for c in tuple(newChild.childNodes):
                self.insertBefore(c, refChild)
            return
        
        newChild.normalize()
        if not looksLikeResource(newChild):
            raise HierarchyRequestErr(
            "can't add this resource: the child doesn't look like a resource")
        uri = newChild.getAttributeNS(RDF_MS_BASE,'about')
        if not uri: #generate a bNode if the element has no URI reference
            uri = RxPath.generateBnode()
            
        #log.debug('attempting to add resource %s' % uri)
        subjectNode = self.findSubject(uri)
        if not subjectNode:
            #todo: catch this exception?
            subjectNode = self._orderedInsert(uri, Subject,
                lambda x, y: cmp(x.uri, y), notify=self.newResourceTrigger)     
            #self.model.addResource(uri)
            self.subjectDict[uri] = subjectNode
            self.revision += 1
        if not (newChild.namespaceURI == RDF_MS_BASE
                and newChild.localName == 'Description'):
            #add class assertion            
            typeName = RxPath.getURIFromElementName(newChild)
            #log.debug('attempting to adding type statement %s for %s'
            #          % (typeName, uri))
            typeStmt = RxPath.Statement(uri, RDF_MS_BASE+'type', typeName,
                objectType=OBJECT_TYPE_RESOURCE)
            try:
                predicateNode = subjectNode._orderedInsert(typeStmt, Predicate,
                    lambda x, y: cmp(x.stmt, y), notify=self._invokeAddTrigger)
                self.model.addStatement(typeStmt)
                self.revision += 1
            except IndexError:
                #thrown by _orderedInsert: statement already exists in the model
                log.debug('type statement %s already exists for %s' % (typeName, uri))
                
        for child in newChild.childNodes: 
            subjectNode.appendChild(child)

    def getElementById(self, id):
        '''
        Equivalent to self.findSubject(id)
        
        This also implements RxPath's redefinition XPath's id().
        '''
        return self.findSubject(id)
    
    def addResource(self, uri):
        '''
        Add a new resource to the model and return the new Subject node
        Raises IndexError if the resource is already part of the model.
        '''
        subjectNode = self._orderedInsert(uri, Subject, lambda x, y: cmp(x.uri, y),
                                                notify=self.newResourceTrigger)
        #self.model.addResource(uri)
        self.subjectDict[uri] = subjectNode
        return subjectNode

    def removeResource(self, uri, removeListObjects=False):
        '''
        Removes the resouce and all statements that it is the subject of.
        (If the resource is a list all its nested list resources will also be removed.)
        '''
        subject = self.findSubject(uri)
        log.debug("removing %s" % subject)
        if subject:
            self.removeChild(subject, removeListObjects)
            
    def evalXPath(self, xpath, nsMap = None, vars=None, extFunctionMap = None,
                  node = None, expCache=None):
        node = node or self
        #print node    
        context = RxPath.XPath.Context.Context(node,
                    varBindings = vars, extFunctionMap = extFunctionMap,
                                               processorNss = nsMap)
        #extModuleList = os.environ.get("EXTMODULES","").split(":"))
        return RxPath.evalXPath(xpath, context, expCache)

    def getKey(self):
        '''
        This is used by the cache to return a key that uniquely
        represents this node and current state of the DOM
        '''
        from rx import MRUCache
        if self.graphManager:
            #the DOM store uses InvalidationKey to invalidate the cache
            #during rollback
            return MRUCache.InvalidationKey((self.graphManager.getTxnContext(),))
        else:    
            return (id(self),  self.revision)

    def commit(self, **kw):
        self.model.commit(**kw)

    def rollback(self):        
        self.model.rollback()
            
        #to remove the changes we need to rollback we just force the
        #DOM's nodes to be regenerated from the model by null-ing out
        #childNodes and then incrementing revision.
        
        #warning: its still possible the application may have a
        #dangling references to nodes that don't know they've been
        #deleted

        #todo: to fix this, add a rollbackCount to the doc and subject
        #nodes and have the various getters check compare that to see
        #if they need to regenerate themselves
        self._childNodes = None
        self.subjectDict = {}
        self.revision += 1 #note that revision is also used as part of cache keys

    def pushContext(self,uri):
        if self.graphManager:
            self.graphManager.pushContext(uri)

    def popContext(self):
        if self.graphManager:
            self.graphManager.popContext()        

    def _entailmentAdd(self, stmt):
        self._entailmentChange(stmt, True)

    def _entailmentRemove(self, stmt):
        self._entailmentChange(stmt, False)
        
    def _entailmentChange(self, stmt, add):
        if self._childNodes is not None: #already created them
            self._childNodes = None
            self.subjectDict = {}  
            #todo: handle this case more efficiently

class DocumentFragment(DomTree.DocumentFragment, Node):
    pass
        
import traceback, sys, re

def main(argv=sys.argv):
    modelPath = None
    try:
        if len(argv) > 1:
            if argv[1] in ['-t', '--transform']:
                print invokeRxSLT(argv[2], argv[3])
                return
            else:
                modelPath = argv[1]
    except IndexError:
        pass
    if not modelPath:
        print '''        
usage:
 rdfpath
   Enter interactive mode using the given RDF file
 rdfpath query
   Invoke the RxPath query using the given RDF file
 -t|--transform rdfpath xsltpath
   Invoke the RxSLT stylesheet on the given RDF file
'''
        return
    
    #not exactly matching the XPointer xmlns() Scheme production
    #we just disallow namespace URI with () instead of supporting escaping
    #also we allow the default the namespace to be set by making the prefix optional
    prog = re.compile(r"(\s*xmlns\s*\(\s*(\w*)\s*=\s*([^\(\)\t\n\r\f\v]+)\s*\))?(.*)")
    def doQuery(query):    
        try:
            while 1:
                m = prog.match(query)
                if m and m.group(3):
                   print 'set prefix', repr(m.group(2)), 'to', repr(m.group(3))
                   context.processorNss[m.group(2)] = m.group(3)
                   query = m.group(4)
                else:
                    break
            if query.strip():
                compExpr = RxPath.XPath.Compile(query)
                #compExpr.pprint()
                context.node = rdfDom
                #bug in context.clone() -- doesn't copy functions
                res = RxPath.XPath.Evaluate(compExpr, context=context)
            else:
                res = 'No query entered'
        except:
            print "Unexpected error:", sys.exc_info()[0]
            traceback.print_exc(file=sys.stdout)    
        else:
            print repr(res)
            if res:
                vname = 'v'+`len(vars)`
                vars[(None, vname)] =  res
                print 'set ' + vname

            #for n in res:
            #    Ft.Xml.Lib.Print.PrettyPrint(n)

    from rx import Uri
    uri = Uri.OsPathToUri(modelPath)
    model = RxPath.MemModel(RxPath.parseRDFFromURI(uri))
        
    #model = RxPath.initRedlandHashBdbModel('test-bdb', file(modelPath))    
    #model, db = RxPath.deserializeRDF( modelPath )
    #model = RxPath.FtModel(model)
    
    ns =[ ('http://rx4rdf.sf.net/ns/archive#', u'a'),
          ('http://rx4rdf.sf.net/ns/wiki#', u'wiki'),
          ('http://rx4rdf.sf.net/ns/auth#', u'auth'),
           ('http://www.w3.org/2002/07/owl#', u'owl'),
           ('http://purl.org/dc/elements/1.1/', u'dc'),
           ('http://xmlns.4suite.org/ext', 'xf'),
           ( RDF_MS_BASE, u'rdf'),
           ('http://www.w3.org/2000/01/rdf-schema#', u'rdfs'),
        ]
    extFunctionMap = None
    try:
        from rx import raccoon
        extFunctionMap = raccoon.DefaultExtFunctions        
        ns.append( (raccoon.RXWIKI_XPATH_EXT_NS, 'wf') )
    except ImportError:
        pass    
    nsMap = dict( ns )
    processorNss = dict( map(lambda x: (x[1], x[0]), ns) )
    vars = { (None, 'dummy') : [] } #dummy so Context uses this dictionary
    rdfDom = Document(model, nsMap)
    context = RxPath.XPath.Context.Context(rdfDom, varBindings=vars,
            extFunctionMap=extFunctionMap, processorNss = processorNss)    

    if len(argv) > 2:    
        query = argv[2]
    else:
        query = None
        
    if not query:
        while 1:
            sys.stderr.write("Enter Query: ")
            query = sys.stdin.readline()
            sys.stderr.write("\n")                
            #raise SystemExit("You must either specify a query on the command line our use the --file option")
            doQuery(query)            
    else:
        doQuery(query)

if __name__  == "__main__":
    main()
