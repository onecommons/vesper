EMPTY_NAMESPACE = None
EMPTY_PREFIX = None
XMLNS_NAMESPACE = u"http://www.w3.org/2000/xmlns/"
XML_NAMESPACE = u"http://www.w3.org/XML/1998/namespace"
XHTML_NAMESPACE = u"http://www.w3.org/1999/xhtml"

def SplitQName(qname):
    l = qname.split(':',1)
    if len(l) < 2:
        return None, l[0]
    return tuple(l)

XFalse = False
XTrue = True
Xbool = bool

def GenerateUuid():
    import random
    return random.getrandbits(16*8) #>= 2.4

def UuidAsString(uuid):
    """
    Formats a long int representing a UUID as a UUID string:
    32 hex digits in hyphenated groups of 8-4-4-4-12.
    """   
    s = '%032x' % uuid
    return '%s-%s-%s-%s-%s' % (s[0:8],s[8:12],s[12:16],s[16:20],s[20:])

def CompareUuids(u1, u2):
    """Compares, as with cmp(), two UUID strings case-insensitively"""
    return cmp(u1.upper(), u2.upper())
