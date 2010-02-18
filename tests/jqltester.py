'''
Test can be added to the test suite by calling:
 
t(query=None, result=None, **kw)

where kw can be one of these (shown with defaults):

ast=None if set, assert ast matches
rows=None if set, assert intermediate rows match
skip=False
skipParse=False don't parse query, use given AST instead
model=None Execute the query with this model
name=None name this test 
group=None add this test to the given group

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
        group=None, unordered=False, forUpdate=False)

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
def cp(name, *args, **kw):
    #print 'cp', name, args
    if not args:
        #no name
        return ConstructProp(None, name, **kw)
    if isinstance(name, str):        
        kw['nameFunc']=Constant(name)
    return ConstructProp(None, *args, **kw)

_models = {}
def modelFromJson(modelsrc, modelname=None):
    model = pjson.Parser(generateBnode='counter', nameMap={'refs':'(URIREF)'}).to_rdf(modelsrc)
    model = vesper.data.store.basic.MemStore(model)
    model.bnodePrefix = '_:'
    if not modelname:
        modelname = 'model%s' % (len(_models)+1)
    _models[id(model)] = (modelsrc, modelname)
    return model

import logging
logging.basicConfig() 

from string import Template

_printedmodels = []
def printdocs(test):
    if not test.doc:
        return    
    modelsrc, modelname = _models[id(test.model)]    
    if id(test.model) in _printedmodels:    
        createmodel = ''
    else:
        _printedmodels.append(id(test.model))
        modelformatted = pprint.pformat(modelsrc).replace('\n', '\n ... ')  
        createmodel = Template("""
 >>> $modelname = app.createStore('''$modelformatted''')
""").substitute(locals())
    doc = test.doc
    result = pprint.pformat(test.results).replace('\n', '\n ')     
    queryformatted = test.query.replace('\n', '\n ... ')  
    print Template("""
$doc$createmodel
 >>> ${modelname}.query('''$queryformatted''')
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

def main(t, cmdargs=None):
    from optparse import OptionParser
    
    usage = "usage: %prog [options] [group name] [number]"
    parser = OptionParser(usage)
    for name, default in [('printmodel', 0), ('printast', 0), ('explain', 0),
        ('printdebug', 0), ('printrows', 0), ('quiet',0), ('listgroups',0),
        ('printdocs',0)]:
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
    currentgroup = None
    groupcount = 0
    lastmodelid = None
    for (i, test) in enumerate(flatten(t)):
        if test.group != currentgroup:
            currentgroup = test.group
            groupcount = 0
        else:
            groupcount += 1
        if options.group and options.group != currentgroup:
            skipped += 1
            continue
        
        if options.num > -1:
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
        
        if not options.quiet:
            print '*** running test:', name
            print 'query', test.query

        if options.printmodel and id(test.model) != lastmodelid:
            lastmodelid = id(test.model)
            print 'model'
            pprint.pprint(list(test.model))

        if test.ast:
            if not test.skipParse and test.query:
                (testast, errs) = jql.buildAST(test.query)
                #jql.rewriteAST(testast)
                if not options.quiet: print 'comparing ast'
                if test.ast == 'error': #expect an error
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
                            explain=explain, debug=debug, forUpdate = test.forUpdate))
        else:
            testresults = None
        
        if not options.quiet:        
            print "Construct Results:", (options.printdebug and '(with debug)' or '')
            pprint.pprint(testresults)

        if test.results is not None:
            resultsMatch = test.results == testresults 
            if not resultsMatch and test.unordered:                
                assert sorted(test.results) == sorted(testresults),  (
                            'unexpected (unordered) results for test %d' % i)
                if not options.quiet:
                    print "warning: unexpected order for (unordered) test %d" % i
            else:
                assert resultsMatch,  ('unexpected results for test %d' % i)

    if not options.printdocs:
        print '***** %d tests passed, %d skipped' % (count, skipped)
    elif t._nextdoc:
        print '\n'.join(t._nextdoc)
