#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
'''
Test can be added to the test suite by calling:
 
t(query=None, result=None, **kw)

where kw can be one of these (shown with defaults):

ast=None If set and query is set, assert ast matches query otherwise execute ast as query.
rows=None if set, assert intermediate rows match
model=None Execute the query with this model
name=None name this test 
group=None add this test to the given group
skip=False
skipParse=False don't parse query, use given AST instead

bindvars, forUpdate, and useSerializer get passed to vesper.query.evalAST.

If any of these attributes are set on `t` they will used as the default value 
for subsequent calls to `t`. For example, setting `t.model` will apply that 
model to any test added if the test doesn't specify a model.
'''

from vesper import pjson
from vesper import query as jql
from vesper.query.jqlAST import *
from vesper.data import base
import sys, pprint
from vesper.utils import flatten

import vesper.query.jqlAST
import vesper.query.engine

class Test(object):
    def __init__(self, attrs):
        self.__dict__.update(attrs)

class Suite(object):    
    defaults = dict(ast=None, rows=None, result=None, skip=False, bindvars=None,
        printdebug=False, skipParse=False, model=None, name=None, query=None,
        group=None, unordered=False, forUpdate=False, useSerializer=False)

    def __init__(self):
        self.tests = []
        self._nextdoc = []
        
    def __mod__(self, doc):
        '''
        Use "t % 'foo'" to capture doc strings for --printdocs option
        '''
        self._nextdoc.append(doc)
        
    def __call__(self, query=None, results=None, **kw):
        '''
        optional arguments:
        rows: test the tupleset result matches this
        results : test the result of query execution matches this
        name: name the test
        '''
        defaults = self.defaults.copy()
        defaults.update(self.__dict__)
        defaults.update(query=query, results=results)
        model=kw.get('model')
        if model is not None:
            if not isinstance(model, base.Model):
                kw['model'] = modelFromJson(model)
        defaults.update(kw)
        t = Test(defaults)
        t.doc = '\n'.join(self._nextdoc)
        self._nextdoc = []  
        self.tests.append(t)
        return t

    def __iter__(self):
        for t in self.tests:
            yield t

#aliases for convenience
jc = JoinConditionOp
cs = ConstructSubject
qF = vesper.query.engine.SimpleQueryEngine.queryFunctions
QueryOp.functions = qF
def cp(name, *args, **kw):
    #print 'cp', name, args
    if not args:
        #no name
        return ConstructProp(None, name, **kw)
    if isinstance(name, str):        
        kw['nameFunc']=Constant(name)
    return ConstructProp(None, *args, **kw)

_models = {}
nameMap = {'refpattern':'(URIREF)'}

def modelFromJson(modelsrc, modelname=None, checkForDuplicateIds=True):
    model = pjson.Parser(generateBnode='counter', nameMap=nameMap,        
        toplevelBnodes=True, #set so top-level object use 'counter's
        checkForDuplicateIds = checkForDuplicateIds
    ).to_rdf(modelsrc)[0]
    model = vesper.data.store.basic.MemStore(model)
    model.bnodePrefix = '_:'
    if not modelname:
        modelname = 'model%s' % (len(_models)+1)
    _models[id(model)] = (modelsrc, modelname)
    return model

import logging, textwrap
logging.basicConfig() 

from string import Template

_printedmodels = []

def _pprintjson(src, width=50):
    #return pprint.pformat(src, width=width)    
    return pjson.json.dumps(src, indent=2) #textwrap.fill(, width)

def formatmodel(modelsrc, modelname):
    modeltext = _pprintjson(modelsrc)
    modelformatted = modeltext.replace('\n', '\n ... ')
    modelplain = modeltext.replace('\n', '\n  ')     
    return Template("""
 >>> from vesper import app
 >>> $modelname = app.createStore(
 ... '''$modelformatted''')
""").substitute(locals()),  Template("""
 from vesper import app
 $modelname = app.createStore(
 '''$modelplain''')\n
""").substitute(locals())

def formatPlainText(plaintext):
    rows=plaintext.count('\n')+1
    return Template("""
.. raw:: html

  <div class='example-plaintext' style='display:none'>
  <div><span class='close-example-plaintext'>X</span>Copy this code into your Python shell.</div>  
  <textarea rows='$rows' cols='60'>$plaintext
  </textarea></div>

""").substitute(locals())


#called by jsonqlDocTest.py
def printmodel(model):
    modelsrc, modelname = _models[id(model)]    
    assert id(model) not in _printedmodels
    _printedmodels.append(id(model))  
    modelFormatted, modelPlain = formatmodel(modelsrc, modelname)
    plaintextFormatted = formatPlainText(modelPlain)
    return '''
%s
.. code-block:: python

%s
''' % (plaintextFormatted, modelFormatted)

def printdocs(test):
    if not test.doc:
        return    
    modelsrc, modelname = _models[id(test.model)]    
    if id(test.model) in _printedmodels:    
        createmodel, modelplain = '', ''
    else:        
        _printedmodels.append(id(test.model))        
        createmodel, modelplain = formatmodel(modelsrc, modelname)
    doc = test.doc
    result = _pprintjson(test.results).replace('\n', '\n ')     
    queryformatted = test.query.replace('\n', '\n ... ')
    queryplain = test.query.strip().replace('\n', '\n  ')
    
    plaintext = Template("""$modelplain
 $modelname.query(
   '''$queryplain''')
""").substitute(locals())
    plaintextFormatted = formatPlainText(plaintext)
    plaintexthtml = '''

.. code-block:: jsonql

 %s

%s
.. code-block:: python
''' % (queryplain, plaintextFormatted)
    print Template("""
$doc$plaintexthtml$createmodel
 >>> ${modelname}.query(
 ... '''$queryformatted''')
 $result
""").substitute(locals())

def listgroups(t):
    currentgroup = None
    count = 0
    for test in flatten(t):
        count += 1
        if test.group != currentgroup:
            if currentgroup: print currentgroup, count
            currentgroup = test.group
            count = 0
    if currentgroup: print currentgroup, count
    print 'total:', len(list(flatten(t)))

def main(t, cmdargs=None):
    from optparse import OptionParser
    
    usage = "usage: %prog [options] [group name] [number]"
    parser = OptionParser(usage)
    for name, default in [('printmodel', 0), ('printast', 0), ('explain', 0),
        ('printdebug', 0), ('printrows', 0), ('quiet',0), ('listgroups',0),
        ('printdocs',0), ('skip', 0), ('dontabort', 0)]:
        parser.add_option('--'+name, dest=name, default=default, 
                                                action="store_true")
    (options, args) = parser.parse_args(cmdargs)
    if options.listgroups:
        listgroups(t)
        return
    options.num = -1
    options.group = None
    if args and args[0] != 'null':
        if len(args) > 1:
            options.group = args[0]
            options.num = int(args[1])
        else:            
            try:                        
                options.num = int(args[0])
            except:
                options.group = args[0]
    
    count = 0
    skipped = 0
    failed = 0
    currentgroup = None
    groupcount = 0
    lastmodelid = None
    for (i, test) in enumerate(flatten(t)):
        if test.group != currentgroup:
            currentgroup = test.group
            groupcount = 0
        else:
            groupcount += 1
        if options.group:
            if options.skip:
                if options.group == currentgroup:
                    if options.num == -1 or groupcount == options.num:
                        skipped += 1
                        continue                
            elif options.group != currentgroup:
                skipped += 1
                continue
        
        if options.num > -1:
            if options.skip:
                if i == options.num:
                    skipped += 1
                    continue
            else:
                if options.group:
                    if groupcount != options.num:
                        skipped += 1
                        continue
                elif i != options.num:
                    skipped += 1
                    continue
                
        if test.skip:
            skipped += 1
            continue
        count += 1

        if test.name:
            name = test.name
        elif test.group:
            name = '%s %d' % (test.group, groupcount)
        else:
            name = "%d" % i        
        
        if options.printdocs:
            options.quiet = True
            printdocs(test)
            continue
        
        if not options.quiet and not options.dontabort:
            print '*** running test:', name
            try:
                print 'query', test.query or test.ast
            except UnicodeEncodeError:
                print 'query', test.query.strip().encode('unicode_escape')

        if options.printmodel and id(test.model) != lastmodelid:
            lastmodelid = id(test.model)
            print 'model'
            pprint.pprint(list(test.model))

        if test.ast:
            if not test.skipParse and test.query:
                try:
                    (testast, errs) = jql.buildAST(test.query)
                except:
                    if test.ast != 'error':
                        raise
                    testast = None
                if not options.quiet: print 'comparing ast'
                if test.ast == 'error': #expect an error
                    if not options.quiet: 
                        print 'expected error in test', name
                    assert testast is None, (
                        'not expecting an ast for test %d: %s' % (i,testast))
                else:
                    if isinstance(test.ast, (str,unicode)):
                        ast = jql.buildAST(test.ast)[0]
                    else:
                        ast = test.ast
                    assert testast == ast, (
                            'unexpected ast for test %d: %s \n %s'
                            % (i, findfirstdiff(testast, ast), testast))
                ast = testast
            else:
                ast = test.ast
        else:
            if isinstance(test.query, QueryOp):
                ast = test.query
            else:
                (ast, errs) = jql.buildAST(test.query)
            assert ast, "ast is None, parsing failed"

        if options.printast:
            print "ast:"
            pprint.pprint(ast)

        if options.printrows or test.rows is not None:
            if ast:
                evalAst = ast.where            
                testrows = list(jql.evalAST(evalAst, test.model, test.bindvars))
            else:
                testrows = None
        if options.printrows:
            print 'labels', evalAst.labels
            print 'rows:'
            pprint.pprint(testrows)        
        if test.rows is not None:
            assert test.rows== testrows,  ('unexpected rows for test %d' % i)

        if options.explain:
            print "explain plan:"
            explain = sys.stdout
        else:
            explain = None
        
        if options.printdebug or test.printdebug:
            debug = sys.stdout
        else:
            debug = None
        
        if ast:
            testresults = list(jql.evalAST(ast, test.model, test.bindvars,
                    explain=explain, debug=debug, forUpdate = test.forUpdate, 
                                            useSerializer= test.useSerializer))
        else:
            testresults = None
        
        if not options.quiet and not options.dontabort:        
            print "Construct Results:", (options.printdebug and '(with debug)' or '')
            pprint.pprint(testresults)

        def printDontAbortMsg(errMsg):
            print '*** running test:', name
            print 'query', test.query
            print "Construct Results:", (options.printdebug and '(with debug)' or '')
            pprint.pprint(testresults)                    
            print errMsg
            
        if test.results is not None:
            resultsMatch = test.results == testresults
            if not resultsMatch and test.unordered:
                unorderedMatch = sorted(test.results) == sorted(testresults)
                errMsg = 'unexpected (unordered) results for test %d' % i
                if not unorderedMatch and options.dontabort:
                    printDontAbortMsg(errMsg)
                    failed += 1
                    continue
                else:
                    assert unorderedMatch, errMsg
                if not options.quiet:
                    print "warning: unexpected order for (unordered) test %d" % i
            else:
                errMsg = 'unexpected results for test %d' % i
                if not resultsMatch and options.dontabort:
                    printDontAbortMsg(errMsg)
                    failed += 1
                    continue
                else:
                    assert resultsMatch, errMsg

    if not options.printdocs:
        print '***** %d tests passed, %d failed, %d skipped' % (count-failed, failed, skipped)
    elif t._nextdoc:
        print '\n'.join(t._nextdoc)

#this model is shared by the documentation tests
def getExampleModel():
    return modelFromJson([
    {
    'id' : 'post1',
    'type' : 'post',
    'contents' : 'a post',
    'author' : '@user:1',
    },
    {
    'id' : 'comment1',
    'type' : 'comment',
    'parent' : '@post1',
    'author' : '@user:2',
    'contents' : 'a comment'
    },
    {
    'id' : 'comment2',
    'parent' : '@comment1',
    'type' : 'comment',
     'contents' : 'a reply',
     'author' : '@user:1'
    },
    {
    'id' : 'comment3',
    'parent' : '@comment4',
    'type' : 'comment',
     'contents' : 'different parent',
     'author' : '@user:1'
    },
    { 
    'id' : 'user:1',
    'type' : 'user', 
    'displayname': 'abbey aardvark', 
    'email' : [ 
      'abbey@aardvark.com',
      'abbey_aardvark@gmail.com'
     ]
    },
    {  
    'id' : 'user:2',
    'type' : 'user',
    'displayname': 'billy billygoat'
    }
    ])
