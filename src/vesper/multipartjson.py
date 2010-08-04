#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
r"""
    multipart json is a simple packaging format for json data designed to make it to easy to read and edit large blocks of text.
    It reminiscent of multipart mime format.
    A multipart json file begins with the line `=multipart-json` followed by a sequence of either JSON or text sections. 
    
    A text section consists of header line which fits this pattern:
    = id endmarker
    where `id` consists of alphanumeric character and endmarker is any sequence of non-whitespace characters
    followed by any number of lines of text as long as it doesn't contain 
    the endmarker
    
    For example:
    
    = id1 endmarker
    foo bar
    endmarker
   
    The apis provide reading and writing multipart json files where
    the text sections can be referenced in the JSON as a object containing the single key "multipartjsonref" whose value is 
    the id in text section header. 
"""
import re

from vesper.backports import *

class Blob(object):
    def __init__(self, id, data):
        self.id = id
        self.data = data

    def write(self, stream):
        done = False
        for i in xrange(3, 1000):
            for char in '=!@#$%^&*()+':
                endmarker = char * i
                if endmarker not in self.data:
                    done = True
                    break
            if done:
                break
        if isinstance(self.data, unicode):
            self.data = self.data.encode('utf8')
        
        stream.write("= %s %s\n%s\n%s\n" 
            % (self.id, endmarker, self.data, endmarker) )
    
    def __repr__(self):
        return "Blob(%r, %r)" % (self.id, self.data)

class BlobRef(object):
    def __init__(self, id, pool):
        self.id = id        
        self._pool = pool
        
    def resolve(self, default=None, pool=None):
        if pool is None:
            pool = self._pool
        return pool.get(self.id, default)

    def __repr__(self):
        return "BlobRef(%r)" % (self.id)

pattern = re.compile(r'= (\w+) (\S+)\r?\n(.*?)\r?\n\2\r?\n', re.S)
header = '=multipart-json\n'
headerpattern = re.compile(r'=multipart-json\r?\n')

def load_one(data):
    '''
>>> test = """= an_id closingstring
... blah 
...   blah 
... closingstring
... { "something else with a closingstring"}
... """
>>> blob, end = load_one(test)
>>> blob
Blob('an_id', 'blah \\n  blah ')
>>> test[end:]
'{ "something else with a closingstring"}\\n'
''' 
    #XXX add support for yaml-style folded text sections?
    match = pattern.match(data)
    if not match:
        raise RuntimeError('bad blob: ' + data[:100])
    id, content = match.group(1), match.group(3)    
    return Blob(id, content), match.end()

def looks_like_multipartjson(data):
    return headerpattern.match(data)

defaultblobrefname = 'multipartjsonref'
def loads(data, doResolve=True, handleUnresolved='raise', default=None, 
    returnblobs=False, decoder=None, blobrefname=defaultblobrefname):
    '''
>>> loads("""=multipart-json
... = id1 ===
... adsfasdfasdf
... ===
... { "hello" : "world" }
... = id2 ====
... adsfas===dfasdf
... ====
... ["a", "json", "array"]
... """, returnblobs=True)
([{'hello': 'world'}, ['a', 'json', 'array']], {'id2': 'adsfas===dfasdf', 'id1': 'adsfasdfasdf'})
    '''
    blobs = {}
    pairs_hook = False
    def object_hook(obj):
        if pairs_hook:
            blobid = len(obj) == 1 and obj[0][0] == blobrefname and obj[0][1]
        else:
            blobid = len(obj) == 1 and obj.get(blobrefname) 
        
        if blobid:
            return BlobRef(blobid, blobs)
        elif default_object_hook:
            return default_object_hook(obj)
        else:
            return obj
            
    if blobrefname:
        if decoder:
            if decoder.object_pairs_hook:
                pairs_hook = True
                default_object_hook = decoder.object_pairs_hook
                decoder.object_pairs_hook = object_hook
            else:
                pairs_hook = False
                default_object_hook = decoder.object_hook
                decoder.object_hook = object_hook
        else:
            decoder = json.JSONDecoder(object_hook = object_hook)
            default_object_hook = None
    else:
        decoder = decoder or json._default_decoder
    
    objs = []    
    while data:
        if data[0] == '=':
            m = headerpattern.match(data)
            if m:
                pos = m.end()
            else:
                blob, pos = load_one(data)
                blobs[blob.id] = blob.data
        elif data[0].isspace():
            pos = 1
        else:
            obj, pos = decoder.raw_decode(data)
            objs.append(obj)
        data = data[pos:]

    if doResolve:
        objs = resolve(objs, handleUnresolved, default)
    if returnblobs:
        return objs, blobs
    else:
        return objs

if json.__version__[:3] < '2.0':
    #the version of json that ships with 2.6 returns unicode even when passed a string
    #update the docstring so doctests don't fail 
    loads.__doc__='''
>>> loads("""=multipart-json
... = id1 ===
... adsfasdfasdf
... ===
... { "hello" : "world" }
... = id2 ====
... adsfas===dfasdf
... ====
... ["a", "json", "array"]
... """, returnblobs=True)
([{u'hello': u'world'}, [u'a', u'json', u'array']], {'id2': 'adsfas===dfasdf', 'id1': 'adsfasdfasdf'})
    '''

def resolve(json, handleUnresolved='raise', default=None):
    '''
    Replaces any BlobRefs found in the given json with the string it references.
    
    `handleUnresolved` specifies the behavior when a BlobRef resolve fails
    can be one of "usedefault" (use the default value as 
    specified in the `default` argument),
    "raise" (the default), which raises an exception
    or "skip", which will leave the BlobRef in place.
    
>>> ref = BlobRef("a", dict(a="example blob"))
>>> badref = BlobRef("bad", {})
>>> resolve({ "key" : ref,  "a list" : [ref, badref]}, 'usedefault', 0)
{'a list': ['example blob', 0], 'key': 'example blob'}
    '''
    if isinstance(json, BlobRef):
        return json.resolve(json)

    todo = [ json ]
    while todo:
        obj = todo.pop()
        if isinstance(obj, dict):
            keyiter = obj.iteritems()
        elif isinstance(obj, list):
            keyiter = enumerate(obj)
        else:
            keyiter = None
        if keyiter:
            for k, v in keyiter:
                if isinstance(v, BlobRef):
                    val = v.resolve(handleUnresolved != 'usedefault' and v or default)
                    if val is v and handleUnresolved == 'raise':
                        raise RuntimeError('can not find BlobRef '+ v.id)
                    obj[k] = val
                elif isinstance(v, (dict, list)):
                    todo.append(v)
    return json

def _dump(objs, stream, includeHeader=True):
    '''
>>> import StringIO
>>> stream = StringIO.StringIO()
>>> _dump([
... { '1' : 2 },
... Blob('id1', "some ==== data \\n"),
... Blob('id2', "some more data \\n"),
... "a json string"
... ], stream)
>>> stream.getvalue()
'=multipart-json\\n{"1": 2}= id1 !!!\\nsome ==== data \\n\\n!!!\\n= id2 ===\\nsome more data \\n\\n===\\n"a json string"'
    '''
    if includeHeader:
        stream.write(header)
    for obj in objs:
        if isinstance(obj, Blob):
            obj.write(stream)
        else:
            json.dump(obj, stream)

def dump(objs, stream, blobmax=1024, blobmin=30, includeHeader=True, 
                            blobrefname=defaultblobrefname, **kw):
    '''
>>> import StringIO # doctest: +NORMALIZE_WHITESPACE
>>> stream = StringIO.StringIO()
>>> dump([
... { '1' : 2, 'long' : '1234567890', 'short' : '12345', 'longkey1234567890' : 'abcdefghij' },
... ['short', 'long: 1234567890'],
... "a long json string"
... ], stream, 9, 9)
>>> stream.getvalue()
'=multipart-json\\n{"1": 2, "short": "12345", "longkey1234567890": {"multipartjsonref":"1"}, "long": {"multipartjsonref":"2"}}\\n= 1 ===\\nabcdefghij\\n===\\n= 2 ===\\n1234567890\\n===\\n["short", {"multipartjsonref":"3"}]\\n= 3 ===\\nlong: 1234567890\\n===\\n{"multipartjsonref":"4"}\\n= 4 ===\\na long json string\\n===\\n'
    '''
    if includeHeader:
        stream.write(header)

    encoder = json.JSONEncoder(**kw)
    key_separator = encoder.key_separator
    count = 0
    def process(chunk, count):
        if chunk[0] == ',':
            startstring = chunk.find('"')
            if startstring > -1:
                stream.write(chunk[:startstring])
                chunk = chunk[startstring:] 
        if chunk[0] == '"' and len(chunk) > blobmin+2 and (
                        len(chunk) > blobmax+2 or r'\n' in chunk):
            decoded = json.loads(chunk)
            count += 1
            blobid = str(count)
            blobs.append(Blob(blobid, decoded))
            chunk = '{"%s":"%s"}' % (blobrefname, blobid)
        stream.write(chunk)
        return count

    for obj in objs:
        blobs = []
        #equivalent to json.dump():
        previous = None
        for chunk in encoder.iterencode(obj):
            if chunk == key_separator:
                #it was a key, don't process previous
                assert previous is not None
                stream.write(previous) 
            elif previous is not None:
                count = process(previous, count)
            previous = chunk            
        if previous is not None:
            process(previous, count)

        #write any blobs encountered:
        stream.write('\n')
        for blob in blobs:
            blob.write(stream)



