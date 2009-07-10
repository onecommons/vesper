########################################################################
## 
# $Header$
"""
Code below based on Ft.Xml.FtMiniDom.DomTree.py with a few additions
(in particular the DocIndex class)

A Python implementation for the Domlette interface.  It is provided solely
as a fallback when cDomlette doesn't work for a particular installation.

Copyright 2002 Fourthought, Inc. (USA).
Detailed license and copyright information: http://4suite.org/COPYRIGHT
Project home, documentation, distributions: http://4suite.org/
"""

from xml.dom import Node as _Node
from xml.dom import NotSupportedErr, HierarchyRequestErr, NotFoundErr
from xml.dom import IndexSizeErr
#from Ft.Xml import SplitQName, XMLNS_NAMESPACE, XML_NAMESPACE
XMLNS_NAMESPACE = u"http://www.w3.org/2000/xmlns/"
XML_NAMESPACE = u"http://www.w3.org/XML/1998/namespace"
def SplitQName(qname):
    l = qname.split(':',1)
    if len(l) < 2:
        return None, l[0]
    return tuple(l)

# Number of characters to truncate to for Text and Comment repr's
CDATA_REPR_LIMIT = 20

try:
    property
except NameError:
    def _defproperty(klass, name):
        pass

    class _ComputedAttributes:
        def __getattr__(self, name):
            # Prevent infinite recursion
            if name.startswith("_get_"):
                raise AttributeError(name)
            try:
                func = getattr(self, "_get_" + name)
            except AttributeError:
                raise AttributeError(name)
            else:
                return func()
                
else:
    # class properties supported
    def _defproperty(klass, name):
        fget = getattr(klass, ("_get_" + name)).im_func
        setattr(klass, name, property(fget))

    class _ComputedAttributes:
        pass


class DOMImplementation:

    def createDocument(self, namespaceURI, qualifiedName, doctype):
        if doctype is not None:
            raise NotSupportedErr("doctype must be None for Domlettes")
        doc = Document(u'')
        if qualifiedName:
            elem = doc.createElementNS(namespaceURI, qualifiedName)
            doc.appendChild(elem)
        return doc

    def createRootNode(self, documentURI=u''):
        return Document(documentURI)

    def hasFeature(self, feature, version):
        if feature.lower() != 'core':
            return 0

        if not version or version == '2.0':
            # From DOM Level 2 Core 1.0: Section 1.2: DOMImplementation:
            # If the version is not specified, supporting any version of
            # the feature causes the method to return true.
            return 1
        return 0

    def __repr__(self):
        return "<pDOMImplementation at %X>" % id(self)

def findnearestcommonancestors(x, y):
    '''
    return the nearest common ancestor and the its two immediate children (or self)
    that are the ancestor-or-self of x and y respectively. 
    '''
    ancx = [] #this will be a list including x and its ancestors
    nextancx = x
    while nextancx:        
        if nextancx == y: #y is an ancestor-or-self of x
            #print 'fnd', nextancx.parentNode, nextancx, y
            if ancx:
                return y, ancx[-1], y
            else:
                return y, nextancx, y
        ancx.append(nextancx)
        nextancx = nextancx.parentNode        
        
    nextancy = y
    while nextancy.parentNode:
        try:  
            i = ancx.index(nextancy.parentNode) #if found, common ancestor found            
            if i > 0:
                xSiblingIndex = i - 1 #index to the child of the common ancestor
            else:
                xSiblingIndex = 0 #x is an ancestor-or-self of y
            #print 'fnd2\n\t', i, xSiblingIndex, ancx[xSiblingIndex], '\n\t', nextancy            
            return ancx[i], ancx[xSiblingIndex], nextancy 
        except ValueError:
            nextancy = nextancy.parentNode

    #they don't share roots
    #print 'not fnd', nextancx, nextancy
    return None, nextancx, nextancy

class DocIndex:
    '''
    The DocIndex class compares nodes based on their document order,
    even after the Dom tree has been rearranged.
    '''
    def __init__(self, node):
        self.node = node
    
    def __cmp__(self, other):
        #print 'compare', self, other
        #print 'compare', self.node, other.node
        if self.node == other.node:
            return 0

        if (self.node.nodeType == Node.ATTRIBUTE_NODE or other.node.nodeType
              == Node.ATTRIBUTE_NODE) and self.node.parentNode == other.node.parentNode:
            if self.node.nodeType == Node.ATTRIBUTE_NODE:
                if other.node.nodeType == Node.ATTRIBUTE_NODE:
                    return 0 #attribute nodes are unordered
                else: #attributes always come before other children
                    return -1
            else: #attributes always come before other children
                return 1
            
        root, nextancestorx, nextancestory = findnearestcommonancestors(self.node, other.node)
        if not root:
            #they're not part of the same tree
            return cmp(self.node.ownerDocument, other.node.ownerDocument)
        elif self.node == root: #self is the ancestor-or-self of other
            #print 'before',self.node#, nextancestorx, nextancestory
            return -1 
        elif other.node == root: #other is the anscestor-or-self of self
            #print 'after'
            return 1        
        else:  #they're siblings 
            assert nextancestorx.parentNode == nextancestory.parentNode
            #print 'cmp', nextancestorx.cmpSiblingOrder(nextancestory)
            return nextancestorx.cmpSiblingOrder(nextancestory)            
    
class Node(_Node, _ComputedAttributes, object): #change to derive from object for __getattribute__() support
    nodeName = None
    nodeValue = None
    nodeType = None
    parentNode = None
    childNodes = None
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
    #sync with Domlette implementation:
    xpathAttributes = None
    xpathNamespaces = None
    XPATH_NAMESPACE_NODE = _Node.NOTATION_NODE+1 #DOM level 3 XPath

    def preAddHook(self, newChild):
        return newChild

    def preRemoveHook(self, child):
        return child

    def _get_docIndex(self):
        return DocIndex(self)

    def __cmp__(self, other):
        return cmp(self.docIndex, other.docIndex)
    
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
        
    def insertBefore(self, newChild, refChild):
        #newChild = preAddHook(newChild)
        if isinstance(newChild, DocumentFragment):
            for c in tuple(newChild.childNodes):
                self.insertBefore(c, refChild)
            return newChild

        # Already owned, remove from its current parent
        if newChild.parentNode is not None:
            newChild.parentNode.removeChild(newChild)
        if refChild is None:
            self.appendChild(newChild)
        else:
            try:
                index = self.childNodes.index(refChild)
            except ValueError:
                raise NotFoundErr()
            self.childNodes.insert(index, newChild)
            newChild.nextSibling = refChild
            refChild.previousSibling = newChild
            if index:
                node = self.childNodes[index-1]
                node.nextSibling = newChild
                newChild.previousSibling = newChild
            else:
                newChild.previousSibling = None
                self.firstChild = newChild
            newChild.parentNode = self
        return newChild

    def replaceChild(self, newChild, oldChild):
        #newChild = self.preAddHook(newChild)
        if isinstance(newChild, DocumentFragment):
            refChild = oldChild.nextSibling
            self.removeChild(oldChild)
            return self.insertBefore(newChild, refChild)

        try:
            index = self.childNodes.index(oldChild)
        except ValueError:
            raise NotFoundErr()
        
        if newChild is oldChild:
            # Nothing to do
            return

        if newChild.parentNode is not None:
            newChild.parentNode.removeChild(newChild)

        self.childNodes[index] = newChild
        newChild.parentNode = self
        newChild.previousSibling = oldChild.previousSibling
        newChild.nextSibling = oldChild.nextSibling
        if newChild.previousSibling is not None:
            newChild.previousSibling.nextSibling = newChild
        else:
            self.firstChild = newChild

        if newChild.nextSibling is not None:
            newChild.nextSibling.previousSibling = newChild
        else:
            self.lastChild = newChild
        
        oldChild.nextSibling = oldChild.previousSibling = None
        oldChild.parentNode = None
        return oldChild

    def removeChild(self, oldChild):
        #oldChild = self.preRemoveHook(oldChild)
        import sys
        #print>>sys.stderr, id(self), type(self)        
        try:
            self.childNodes.remove(oldChild)
        except ValueError:
            #import sys
            #print 'should eq', oldChild.parentNode == self.childNodes[0].parentNode and self.childNodes[0].alias == oldChild.alias
            #print oldChild.parentNode, self.childNodes[0].parentNode, self.childNodes[0].alias, oldChild.alias 
            #print>>sys.stderr, 'self', self, id(self), type(self)        
            #print>>sys.stderr, 'oldchild', oldChild
            #print>>sys.stderr, 'firstchild', self.firstChild 
            #print>>sys.stderr, 'childNodes', self.childNodes
            #print>>sys.stderr, oldChild.parentNode, self.childNodes[0].parentNode 
            #print>>sys.stderr, oldChild.alias, self.childNodes[0].alias
            
            raise NotFoundErr()

        if oldChild.nextSibling is not None:
            oldChild.nextSibling.previousSibling = oldChild.previousSibling
        else:
            self.lastChild = oldChild.previousSibling

        if oldChild.previousSibling is not None:
            oldChild.previousSibling.nextSibling = oldChild.nextSibling
        else:
            self.firstChild = oldChild.nextSibling

        oldChild.nextSibling = oldChild.previousSibling = None
        oldChild.parentNode = None
        return oldChild
    
    def appendChild(self, newChild):
        #newChild = self.preAddHook(newChild)
        if isinstance(newChild, DocumentFragment):
            for c in tuple(newChild.childNodes):
                self.appendChild(c)
            return newChild

        if newChild.parentNode is not None:
            newChild.parentNode.removeChild(newChild)
        
        if not self.firstChild:
            self.firstChild = newChild
        else:
            self.lastChild.nextSibling = newChild
            newChild.previousSibling = self.lastChild
        self.lastChild = newChild
        self.childNodes.append(newChild)
        newChild.parentNode = self
        return newChild

    def hasChildNodes(self):
        # Force boolean result
        return not not self.childNodes

    def normalize(self):
        node = self.firstChild
        while node:
            if isinstance(node, Text):
                next = node.nextSibling
                while isinstance(next, Text):
                    node.nodeValue = node.data = (node.data + next.data)
                    node.parentNode.removeChild(next)
                    next = node.nextSibling
                if not node.data:
                    # Remove any empty text nodes
                    next = node.nextSibling
                    node.parentNode.removeChild(node)
                    node = next
                    # Just in case this was the last child
                    continue
            elif isinstance(node, Element):
                node.normalize()
            node = node.nextSibling
        return

    # DOM Level 3
    def isSameNode(self, other):
        return self is other

_defproperty(Node, "docIndex")

class Document(Node):
    
    nodeType = Node.DOCUMENT_NODE
    nodeName = u'#document'
    implementation = DOMImplementation()
    doctype = None

    # Moved from DocumentType interface (since it is not supported)
    publicId = None
    systemId = None

    def __init__(self, documentURI):
        self.unparsedEntities = {}
        self.childNodes = []
        self.documentURI = self.baseURI = documentURI
        #self.docIndex = 0
        self.nextIndex = 1
        self.rootNode = self
        return

    def _get_documentElement(self):
        for node in self.childNodes:
            if isinstance(node, Element):
                return node
        # No element children
        return None
    
    def cmpSiblingOrder(self, other):
        if other.parentNode:
            return 1
        elif self == other:
            return 0
        else:
            raise 'can not compare with document roots from two different documents'
        
    def createDocumentFragment(self):
        df = DocumentFragment()
        df.ownerDocument = df.rootNode = self
        return df

    def createTextNode(self, data):
        text = Text(data)
        text.ownerDocument = text.rootNode = self
        text.baseURI = self.baseURI
        #text.docIndex = self.nextIndex
        self.nextIndex += 1
        return text

    def createComment(self, data):
        comment = Comment(data)
        comment.ownerDocument = comment.rootNode = self
        comment.baseURI = self.baseURI
        #comment.docIndex = self.nextIndex
        self.nextIndex += 1
        return comment

    def createProcessingInstruction(self, target, data):
        pi = ProcessingInstruction(target, data)
        pi.ownerDocument = pi.rootNode = self
        pi.baseURI = self.baseURI
        #pi.docIndex = self.nextIndex
        self.nextIndex += 1
        return pi

    def createElementNS(self, namespaceURI, qualifiedName):
        prefix, localName = SplitQName(qualifiedName)
        element = Element(qualifiedName, namespaceURI, prefix, localName)
        element.ownerDocument = element.rootNode = self
        element.baseURI = self.baseURI
        #element.docIndex = self.nextIndex
        self.nextIndex += 3  # room for namespace and attribute nodes
        return element

    def createAttributeNS(self, namespaceURI, qualifiedName):
        prefix, localName = SplitQName(qualifiedName)
        attr = Attr(qualifiedName, namespaceURI, prefix, localName, u'')
        attr.ownerDocument = attr.rootNode = self.ownerDocument
        return attr

    def cloneNode(self, deep):
        raise NotSupportedErr("cloning document nodes")

    def importNode(self, node, deep):
        # Alien node, use nodeType checks only
        if node.nodeType == Node.ELEMENT_NODE:
            element = self.createElementNS(node.namespaceURI, node.nodeName)
            for attr in node.attributes.values():
                if attr.specified:
                    element.setAttributeNS(attr.namespaceURI, attr.name,
                                           attr.value)
            if deep:
                for child in node.childNodes:
                    element.appendChild(self.importNode(child, deep))
            return element

        if node.nodeType == Node.TEXT_NODE:
            return self.createTextNode(node.data)

        if node.nodeType == Node.COMMENT_NODE:
            return self.createComment(node.data)

        if node.nodeType == Node.PROCESSING_INSTRUCTION_NODE:
            return self.createProcessingInstruction(node.target, node.data)

        raise NotSupportedErr("importing nodeType %d" % node.nodeType)
            

    def __repr__(self):
        return '<pDocument at %X: %d children>' % (id(self),
                                                   len(self.childNodes))

_defproperty(Document, "documentElement")

class DocumentFragment(Node):

    nodeType = Node.DOCUMENT_FRAGMENT_NODE
    nodeName = "#document-fragment"

    def __init__(self):
        self.childNodes = []

    def __repr__(self):
        return '<pDocumentFragment at %X: %d children>' % (
            id(self), len(self.childNodes))



#__hash__?
#readonly ops: __add__(self, other) __radd__(self, other) __mul__(self, other) __rmul__(self, other)
#readonly methods: count index

class XPathNamespace(Node):
    '''The XPathNamespace interface represents the XPath namespace node type
    that DOM lacks.'''

    nodeType = Node.XPATH_NAMESPACE_NODE
    
    def __init__(self, parentNode, prefix, namespaceURI):          
        self.ownerDocument = parentNode.ownerDocument;
        self.localName = self.nodeName = prefix
        self.value = self.nodeValue = namespaceURI
        self.parentNode = parentNode
                           
    def __repr__(self):        
        return "<pXPathNamespace at %p: name %s, value %s>" % (id(self),
                      self.name,self.value);

class Element(Node):

    nodeType = Node.ELEMENT_NODE

    def __init__(self, nodeName, namespaceURI, prefix, localName):
        self.nodeName = self.tagName = nodeName
        self.namespaceURI = namespaceURI
        self.prefix = prefix
        self.localName = localName
        self.attributes = {}
        self.childNodes = []
        return

    def _get_attrkey(self, namespaceURI, localName):
        """Helper function to create the key into the attributes dictionary"""
        key = (namespaceURI, localName)
        if key == (XMLNS_NAMESPACE, 'xmlns'):
            # Default namespace declaration
            key = (namespaceURI, None)
        return key

    def _set_attribute(self, key, attr):
        """Helper function for setAttributeNS/setAttributeNodeNS"""
        self.attributes[key] = attr

        attr.ownerElement = attr.parentNode = self
        attr.ownerDocument = attr.rootNode = self.ownerDocument
        attr.baseURI = self.baseURI

        # Namespace nodes take self.docIndex + 1
        # Attributes are unordered so they all share the same docIndex
        #attr.docIndex = self.docIndex + 2
        return

    def _get_xpathAttributes(self):
        return [v for k,v in self.attributes.items() if k[0] != XMLNS_NAMESPACE]

    def _get_xpathNamespaces(self):
        import Domlette
        return [XPathNamespace(self,prefix,uri)
                for prefix, uri in Domlette.GetAllNs(self).items()]
        
    def getAttributeNS(self, namespaceURI, localName):
        key = self._get_attrkey(namespaceURI, localName)
        try:
            attr = self.attributes[key]
        except KeyError:
            return u''
        else:
            return attr.nodeValue

    def setAttributeNS(self, namespaceURI, qualifiedName, value):
        prefix, localName = SplitQName(qualifiedName)
        key = self._get_attrkey(namespaceURI, localName)
        attr = self.attributes.get(key)
        if attr:
            # Reuse existing attribute node
            attr.prefix = prefix
            attr.nodeValue = attr.value = value
            return
        
        attr = Attr(qualifiedName, namespaceURI, prefix, localName, value)
        self._set_attribute(key, attr)
        return

    def removeAttributeNS(self, namespaceURI, localName):
        key = self._get_attrkey(namespaceURI, localName)
        try:
            del self.attributes[key]
        except KeyError:
            pass
        return

    def hasAttributeNS(self, namespaceURI, localName):
        key = self._get_attrkey(namespaceURI, localName)
        return self.attributes.has_key(key)

    def getAttributeNodeNS(self, namespaceURI, localName):
        key = self._get_attrkey(namespaceURI, localName)
        return self.attributes.get(key)

    def setAttributeNodeNS(self, newAttr):
        key = self._get_attrkey(newAttr.namespaceURI, newAttr.localName)
        oldAttr = self.attributes.get(key)
        self._set_attribute(key, newAttr)
        return oldAttr

    def removeAttributeNode(self, oldAttr):
        for key, attr in self.attributes.items():
            if attr is oldAttr:
                del self.attributes[key]
                attr.ownerElement = attr.parentNode = None
                return attr

        raise NotFoundErr()

    def cloneNode(self, deep):
        doc = self.ownerDocument
        element = doc.createElementNS(self.namespaceURI, self.nodeName)
        for attr in self.attributes.values():
            new_attr = doc.createAttributeNS(attr.namespaceURI, attr.nodeName)
            new_attr.value = attr.value
            element.setAttributeNodeNS(new_attr)

        if deep:
            for child in self.childNodes:
                element.appendChild(child.cloneNode(deep))
        return element

    def __repr__(self):
        return "<pElement at %X: %s, %d attributes, %d children>" % (
            id(self),
            repr(self.nodeName),
            len(self.attributes),
            len(self.childNodes)
            )

_defproperty(Element, "xpathAttributes")
_defproperty(Element, "xpathNamespaces")

class _Childless:
    """
    Mixin that makes childless-ness easy to implement and avoids
    the complexity of the Node methods that deal with children.
    """
    
    childNodes = []
    #sync with Domlette:
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
    

class Attr(_Childless, Node):

    nodeType = Node.ATTRIBUTE_NODE
    specified = 1

    # Default when created via Document.createAttributeNS
    #docIndex = -1
    
    def __init__(self, nodeName, namespaceURI, prefix, localName, value):
        self.nodeName = self.name = nodeName
        self.namespaceURI = namespaceURI
        self.prefix = prefix
        self.localName = localName
        self.nodeValue = self.value = value
        self.ownerElement = None
        # XPath Data Model
        self.parentNode = None
        return

    def cmpSiblingOrder(self, other):
        if other.nodeType == Node.ATTRIBUTE_NODE:
            return 0 #attributes are unordered, so all siblings are equal
        else: #attributes come before child nodes
            return -1 

    def cloneNode(self, deep):
        raise NotSupportedErr("cloning of attribute nodes")

    def __repr__(self):
        return "<pAttr at %X: name %s, value %s>" % (id(self),
                                                     repr(self.nodeName),
                                                     repr(self.nodeValue))

class _CharacterData(_Childless, Node):

    def __init__(self, data):
        self.__dict__['data'] = data
        self.__dict__['nodeValue'] = data
        return

    def __setattr__(self, name, value):
        if name == 'data':
            self.__dict__['nodeValue'] = value
        elif name == 'nodeValue':
            self.__dict__['data'] = value
        self.__dict__[name] = value
        return

    def substringData(self, offset, count):
        if offset < 0:
            raise IndexSizeErr("offset cannot be negative")
        if offset >= len(self.data):
            raise IndexSizeErr("offset cannot be beyond end of data")
        if count < 0:
            raise IndexSizeErr("count cannot be negative")
        return self.data[offset:offset+count]

    def appendData(self, arg):
        self.data = self.data + arg
        return
    
    def insertData(self, offset, arg):
        if offset < 0:
            raise IndexSizeErr("offset cannot be negative")
        if offset >= len(self.data):
            raise IndexSizeErr("offset cannot be beyond end of data")
        if arg:
            self.data = "%s%s%s" % (
                self.data[:offset], arg, self.data[offset:])
        return
    
    def deleteData(self, offset, count):
        if offset < 0:
            raise IndexSizeErr("offset cannot be negative")
        if offset >= len(self.data):
            raise IndexSizeErr("offset cannot be beyond end of data")
        if count < 0:
            raise IndexSizeErr("count cannot be negative")
        if count:
            self.data = self.data[:offset] + self.data[offset+count:]
        return

    def replaceData(self, offset, count, arg):
        if offset < 0:
            raise IndexSizeErr("offset cannot be negative")
        if offset >= len(self.data):
            raise IndexSizeErr("offset cannot be beyond end of data")
        if count < 0:
            raise IndexSizeErr("count cannot be negative")
        if count:
            self.data = "%s%s%s" % (
                self.data[:offset], arg, self.data[offset+count:])
        return

    def __repr__(self):
        if len(self.data) > CDATA_REPR_LIMIT:
            data = self.data[:CDATA_REPR_LIMIT] + '...'
        else:
            data = self.data
        return "<p%s at %X: %s>" % (self.__class__.__name__, id(self), `data`)


class Text(_CharacterData):

    nodeType = Node.TEXT_NODE
    nodeName = u'#text'

    def cloneNode(self, deep):
        return self.ownerDocument.createTextNode(self.data)


class Comment(_CharacterData):

    nodeType = Node.COMMENT_NODE
    nodeName = u'#comment'
    
    def cloneNode(self, deep):
        return self.ownerDocument.createComment(self.data)


class ProcessingInstruction(_Childless, Node):

    nodeType = Node.PROCESSING_INSTRUCTION_NODE
    
    def __init__(self, target, data):
        self.target = self.nodeName = target
        self.nodeValue = self.data = data
        return

    def __setattr__(self, name, value):
        if name == 'data':
            self.__dict__['nodeValue'] = value
        elif name == 'nodeValue':
            self.__dict__['data'] = value
        elif name == 'target':
            self.__dict__['nodeName'] = value
        elif name == 'nodeName':
            self.__dict__['target'] = value
        self.__dict__[name] = value
        return

    def cloneNode(self, deep):
        return self.ownerDocument.createProcessingInstruction(self.nodeName,
                                                              self.nodeValue)

    def __repr__(self):
        return "<pProcessingInstruction at %X: %s %s>" % (id(self),
                                                          repr(self.nodeName),
                                                          repr(self.nodeValue))

implementation = Document.implementation
