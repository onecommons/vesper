#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
'''
parse JQL 
'''
from vesper.query.jqlAST import *
import logging, logging.handlers
#errorlog = logging.getLogger('parser')

#XXX: better error reporting:
#set p.linenum(n) and p.lexpos(n) on queryop
#associate op with queryexception when raising
#handler prints: "error near line x, pos y: "+exception message
#if op doesn't have line/pos info find the nearest parent that does
#even better:
#use p.linespan(n) and p.lexspan(n) to recreate the substring in the query
#that op is a translation of

class LogCaptureHandler(logging.handlers.BufferingHandler):
    "Simple logging handler that captures and retains log messages"
    def shouldFlush(self, record):
        return False        

class Tag(tuple):
    __slots__ = ()

    def __new__(cls, *seq):
        return tuple.__new__(cls, seq)

    def __repr__(self):
        return self.__class__.__name__+tuple.__repr__(self)

    #for compatibility with QueryOp iterators:
    args = property(lambda self: [s for s in self if hasattr(s,'args')])

class _Env (object):
    _tagobjs = {}

    def __getattr__(self, attr):
        tagclass = self._tagobjs.get(attr)
        if not tagclass:
            #create a new subclass of Tag with attr as its name
            tagclass = type(Tag)(attr, (Tag,), {'__slots__' : ()})
            self._tagobjs[attr] = tagclass
        return tagclass

T = _Env()

#####PLY ####

import ply.lex
import ply.yacc

###########
#### TOKENS
###########

reserved = ('TRUE', 'FALSE', 'NULL', 'NOT', 'AND', 'OR', 'IN', 'IS', 'NAMEMAP', 
           'ID', 'MAYBE', 'WHERE', 'LIMIT', 'OFFSET', 'DEPTH', 'MERGEALL',
           'GROUP', 'ORDER', 'BY', 'ASC', 'DESC', 'OMITNULL')#'INCLUDE', 'EXCLUDE', 'WHEN')

tokens = reserved + (
    # Literals (identifier, integer constant, float constant, string constant)
    'NAME', 'INT', 'FLOAT', 'STRING', 

    # Operators (+,-,*,/,%,  |,&,~,^,<<,>>, ||, &&, !, <, <=, >, >=, ==, !=)
    'PLUS', 'MINUS', 'TIMES', 'DIVIDE', 'MOD',
    #'OR', 'AND', 'NOT', 'XOR', 'LSHIFT', 'RSHIFT',
    #'LOR', 'LAND', 'LNOT',
    'LT', 'LE', 'GT', 'GE', 'EQ', 'NE',

    # Delimeters ( ) [ ] { } , . 
    'LPAREN', 'RPAREN',
    'LBRACKET', 'RBRACKET',
    'LBRACE', 'RBRACE',
    'COMMA', 'PERIOD', 'COLON',

    'PROPSTRING', 'LABEL', 'BINDVAR'
)

# Operators
t_PLUS             = r'\+'
t_MINUS            = r'-'
t_TIMES            = r'\*'
t_DIVIDE           = r'/'
t_MOD           = r'%'
t_LT               = r'<'
t_GT               = r'>'
t_LE               = r'<='
t_GE               = r'>='
t_EQ               = r'==?'
t_NE               = r'!='

# Delimeters
t_LPAREN           = r'\('
t_RPAREN           = r'\)'
t_LBRACKET         = r'\['
t_RBRACKET         = r'\]'
t_LBRACE           = r'\{'
t_RBRACE           = r'\}'
t_COMMA            = r','
t_PERIOD           = r'\.'
t_COLON            = r':'

reserved_map = { }
for r in reserved:
    reserved_map[r.lower()] = r

reserved_constants = {
 'true' : True,
 'false' : False,
 'null' : None
}

_namere = r'[A-Za-z_\$][\w_\$]*'

def t_INT(t):
    r'\d+'
    t.value = int(t.value)
    return t

def t_FLOAT(t):
    r'(\d+)(\.\d+)?((e|E)(\+|-)?(\d+))'
    t.value = float(t.value)
    return t

def t_STRING(t):
    r'''(?:"(?:[^"\n\r\\]|(?:"")|(?:(\\(x|u|U))[0-9a-fA-F]+)|(?:\\.))*")|(?:'(?:[^'\n\r\\]|(?:'')|(?:(\\(x|u|U))[0-9a-fA-F]+)|(?:\\.))*')'''    
    #support JSON strings which means need to decode escapes like \r and \u0000 
    #since this is a subset of python literal syntax so we can use the unicode-escape decoding
    if isinstance(t.value, unicode):
        #decode on unicode triggers a string encoding with the default encoding
        #so we need to convert the unicode into a string that looks like Python
        # unicode literal, hence the encode('ascii', 'backslashreplace')
        t.value = t.value[1:-1].encode('ascii', 'backslashreplace').decode("unicode-escape")
    else:
        t.value = t.value[1:-1].decode("unicode-escape").encode('utf8')
    #XXX don't do unicode-escape if no escapes appear in string
    return t

def t_PROPSTRING(t):
    r'''(?:<(?:[^<>\n\r\\]|(?:<>)|(?:(\\(x|u|U))[0-9a-fA-F]+)|(?:\\.))*>)'''
    if isinstance(t.value, unicode):        
        t.value = t.value[1:-1].encode('ascii', 'backslashreplace').decode("unicode-escape")
    else:
        t.value = t.value[1:-1].decode("unicode-escape").encode('utf8')
    return t

def t_LABEL(t):
    v = t.value[1:]
    t.value = T.label(v)
    return t
t_LABEL.__doc__ = r'\?'+ _namere +''

def t_NAME(t):
    key = t.value.lower() #make keywords case-insensitive (like SQL)
    t.type = reserved_map.get(key,"NAME")
    t.value = reserved_constants.get(key, t.value)
    return t
t_NAME.__doc__ = '(?P<name>' + _namere + ')'

def t_BINDVAR(t):
    t.value = t.value[1:]
    return t
t_BINDVAR.__doc__ = r'\:'+ _namere +''

# SQL/C-style comments
def t_comment(t):
    r'/\*(.|\n)*?\*/'
    t.lexer.lineno += t.value.count('\n')

# Comment (both Python and C++-Style)
def t_linecomment(t):
    r'(//|\#).*?\n'
    t.lexer.lineno += 1

def t_error(t):
    # print "t_error:", t.lexpos, t.lineno, t.type, t.value
    t.lexer.errorlog.error("Illegal character %s at line:%d char:%d" % (repr(t.value[0]), t.lineno, t.lexpos))
    t.lexer.skip(1)

# Newlines
def t_NEWLINE(t):
    r'(\n|\r)+'
    # print ">>%s<< (%d %d)" % (t.value, t.value.count("\n"), t.value.count("\r"))
    t.lexer.lineno += max(t.value.count("\n"),t.value.count("\r"))

# Completely ignored characters
t_ignore = ' \t\x0c'

#lexer = ply.lex.lex(errorlog=errorlog, optimize=1) #, debug=1) 

# Parsing rules

def resolveQNames(nsmap, root):
    for c in root.depthfirst(descendPredicate=
            lambda n: c is root or not isinstance(n, Select)):
        if isinstance(c, Select):
            if c is root:
                nsmap = nsmap.initParseContext({'namemap':c.namemap}, nsmap)
            else:
                resolveQNames(nsmap, c)
        else:
            c._resolveQNames(nsmap) 

def p_root(p):
    '''
    root : topconstruct
    '''
    p[0] = p[1]
    select = p[0]
    resolveQNames(p.parser.jqlState.namemap, select)
    #we're done parsing, now join together joins that share labels,
    #removing any joined joins if their parent is a dependant (non-root) Select op
    p.parser.jqlState.buildJoins(select)
    #XXX should check for orpaned joins so that can be evaluated 
    #or at least warn until that is implemented
    if not select.where or not select.where.args:
        #top level queries without a filter (e.g. {*}) 
        #should not include anyonmous objects that have already appeared 
        select.skipEmbeddedBNodes = True

def p_construct0(p):
    '''
    topconstruct : dictconstruct
                | listconstruct
                | valueconstruct

    nestedconstruct : dictconstruct
                    | listconstruct
    '''
    assert isinstance(p[1], T.construct)
    shape = {
    '{' : Construct.dictShape,
    '[' : Construct.listShape, 
    '(' : Construct.valueShape   
    }[ p[1][0] ]
    
    label = p[1][1]
    props = p[1][2]
    op = Construct(props, shape)
    defaults = dict(where = None, offset = None, limit = None, namemap = None,
       groupby = None, depth=None, orderby=None, mergeall=False)

    if len(p[1]) > 3 and p[1][3]:
        for constructop in p[1][3]:
            defaults[ constructop[0].lower() ] = constructop[1]

    groupby = defaults['groupby']
    if groupby:
        if isinstance(groupby[0], (Project,Label)):
            arg = groupby[0]
        else:
            raise QueryException("bad group by expression")
                
        defaults['groupby'] = groupby = GroupBy(arg)
    
    if label:
        assert isinstance(label, T.label)
        op.id.appendArg( Label(label[0]) )
    
    where = defaults['where']
    where = defaults['where'] = p.parser.jqlState.joinFromConstruct(op, 
                                        where, groupby, defaults['orderby'])    
    p[0] = Select(op, **defaults)
    assert not where or where.parent is p[0]

precedence = (
    ('left', 'ASSIGN'),
    ('left', 'OR'),
    ('left', 'AND'),
    ('right','MAYBE'),
    ('right','NOT'),
    ("left", "IN"), 
    ("nonassoc", 'LT', 'LE', 'GT', 'GE', 'EQ', 'NE'),
    ('left','PLUS','MINUS'),
    ('left','TIMES','DIVIDE', 'MOD'),
    ('right','UMINUS', 'UPLUS'),
)

def p_expression_in(p):
    """
    expression : expression IN LPAREN exprlist RPAREN
    """    
    p[0] = In(p[1], *p[4])

def p_expression_notin(p):
    """
    expression : expression NOT IN expression
    """    
    p[0] = Not(In(p[1], p[4]))

def p_expression_notin2(p):
    """
    expression : expression NOT IN LPAREN exprlist RPAREN
    """    
    p[0] = Not(In(p[1], *p[5]))

def p_expression_binop(p):
    """
    expression : expression PLUS expression
              | expression MINUS expression
              | expression TIMES expression
              | expression DIVIDE expression
              | expression MOD expression
              | expression LT expression
              | expression LE expression
              | expression GT expression
              | expression GE expression
              | expression EQ expression
              | expression NE expression
              | expression IN expression              
              | expression AND expression
              | expression OR expression
    """
    op = p.parser.jqlState.mapOp(p[2].upper())
    p[0] = op(p[1], p[3])

def p_expression_uminus(p):
    '''expression : MINUS expression %prec UMINUS
                  | PLUS expression %prec UPLUS'''
    if p[1] == '-':
        p[0] = p.parser.jqlState.getFuncOp('negate',p[2])
    else:
        p[0] = p[2]

def p_expression_maybe(p):
    'expression : MAYBE expression'
    p[2].maybe = True
    p[0] = p[2]

def p_expression_notop(p):
    'expression : NOT expression'
    p[0] = Not(p[2])

def p_expression_isop(p):
    '''
    expression : expression IS NULL
               | expression IS NOT NULL
    '''
    if len(p) == 4:
        p[0] = IsNull(p[1])
    else:
        p[0] = Not(IsNull(p[1]))

def p_expression_group(p):
    'expression : LPAREN expression RPAREN'
    p[0] = p[2]

def p_expression_atom(p):
    'expression : atom'
    p[0] = p[1]

def p_atom_constant(p):
    '''constant : INT
            | FLOAT
            | STRING
            | NULL
            | TRUE
            | FALSE
    '''
    p[0] = Constant(p[1])

def p_atom(p):
    """atom : columnref
            | funccall
            | constant
            | join
    """
    p[0] = p[1]

def p_atom_bindvar(p):
    """
    atom : BINDVAR
    """
    p[0] = BindVar(p[1])

def p_atom_label(p):
    """
    atom : LABEL
    """
    p[0] = Label(p[1][0])

def p_label_id(p):
    '''
    columnref : LABEL PERIOD ID
    '''
    p[0] = Label(p[1][0])

#next one must come after above rule so that it takes priority
def p_constructitem6(p):
    '''
    constructitem : ID
    '''    
    p[0] = _makeConstructProp(p[1], Project(0), True, False, False)

def p_atom_id(p):
    """
    atom : ID
    """
    p[0] = Project(0)

def p_funcname(p):
    '''funcname : NAME
                | PROPSTRING
    '''
    p[0] = p[1]

def p_funccall(p):
    "funccall : funcname LPAREN arglist RPAREN"
    try:
        p[0] = p.parser.jqlState.getFuncOp(p[1], *p[3])
    except KeyError:
        msg = "unknown function " + p[1]
        p[0] = ErrorOp(p[3], msg)
        p.parser.errorlog.error(msg)

def p_arglist(p):
    """    
    arglist : arglist COMMA expression
            | arglist COMMA keywordarg
            | keywordarg
            | expression
    exprlist : exprlist COMMA expression
             | expression
    constructitemlist : constructitemlist COMMA constructitem
                      | constructitem
    constructoplist : constructoplist COMMA constructop
                      | constructop
    listconstructitemlist : listconstructitemlist COMMA listconstructitem
                          | listconstructitem
    sortexplist : sortexplist COMMA sortexp
                | sortexp
    barecolumnreflist : barecolumnreflist COMMA barecolumnref
                      | barecolumnref 
    arrayindexlist : arrayindexlist COMMA arrayindex
                   | arrayindex
    jsondictlist : jsondictlist COMMA jsondictitem
                 | jsondictitem
    """
    if len(p) == 4:
        p[0] = p[1] + [p[3]]
    else:
        p[0] = [p[1]]

def p_constructoplist(p):
    """
    constructoplist : constructoplist constructop
    """
    p[1].append(p[2])
    p[0] = p[1]

def p_arglist_empty(p):
    """
    arglist : empty
    constructitemlist : empty
    constructoplist : empty
    listconstructitemlist : empty
    sortexplist : empty
    barecolumnreflist : empty
    arrayindexlist : empty
    jsondictlist : empty
    """
    p[0] = []

def p_keyword_argument(p):
    '''
    keywordarg : NAME EQ expression  %prec ASSIGN
    '''
    p[0] = T.keywordarg(p[1], p[3])

def p_join(p):
    """
    join : LBRACE expression RBRACE
         | LBRACE LABEL COMMA expression RBRACE
         | LBRACE LABEL expression RBRACE
    """
    try:
        if len(p) == 5:
            expr = p[3]            
            assert isinstance(p[2], T.label)
            label = p[2][0]            
        elif len(p) == 6:
            expr = p[4]
            assert isinstance(p[2], T.label)
            label = p[2][0]            
        else:
            assert len(p) == 4
            expr = p[2]
            label = None
        p[0] = p.parser.jqlState.makeJoinExpr(expr)
        if label:
            p[0].name = label
            p.parser.jqlState.addLabeledJoin(label, p[0])
    except QueryException, e:
        import traceback
        traceback.print_exc()#file=sys.stdout)
        p[0] = ErrorOp(p[2], "Invalid Join")
        p.parser.errorlog.error("invalid join: "  +  str(e) + ' ' + repr(p[2]))

def _makeConstructProp(n, v, nameIsFilter, derefName, omit):
    if n == '*':
        n = None
        nameIsFilter = False
    
    if derefName:
        derefName = n
        n = None
    
    if omit:
        omit = PropShape.omit    
    if isinstance(v, T.forcelist):
        return ConstructProp(n, v[0], omit or PropShape.uselist, 
                    PropShape.uselist, nameIsFilter, nameFunc=derefName)
    else:
        return ConstructProp(n, v, omit or PropShape.usenull,
                            nameIsFilter=nameIsFilter, nameFunc=derefName)

def p_constructitem3(p):
    '''
    constructitem : expression COLON dictvalue
    '''
    p[0] = _makeConstructProp(p[1], p[3], False, True, False)

def p_constructitem3b(p):
    '''
    constructitem : OMITNULL expression COLON dictvalue
    '''
    #omitnull implies maybe, see rewrite.joinFromConstruct()
    p[0] = _makeConstructProp(p[2], p[4], False, True, True)

def p_constructitem4(p):
    '''
    constructitem : barecolumnref
    '''    
    p[0] = _makeConstructProp(p[1], Project(p[1]), True, False, False)

def p_constructitem4b(p):
    '''
    constructitem : OMITNULL barecolumnref
    '''
    #omitnull implies maybe, see rewrite.joinFromConstruct()
    p[0] = _makeConstructProp(p[2], Project(p[2]), True, False, True)

def p_constructitem4c(p):
    '''
    constructitem : MAYBE barecolumnref
    '''
    op = Project(p[2])
    op.maybe = True
    p[0] = _makeConstructProp(p[2], op, True, False, False)

def p_constructitem5(p): 
    '''
    constructitem : LBRACKET barecolumnref RBRACKET
    '''
    p[0] = _makeConstructProp(p[2], T.forcelist(Project(p[2])), True, False, False)

def p_constructitem5b(p): 
    '''
    constructitem : LBRACKET OMITNULL barecolumnref RBRACKET
    '''
    #omitnull implies maybe, see rewrite.joinFromConstruct()
    p[0] = _makeConstructProp(p[3], T.forcelist(Project(p[3])), True, False, True)

def p_constructitem5c(p): 
    '''
    constructitem : LBRACKET MAYBE barecolumnref RBRACKET  
    '''
    op = Project(p[3])
    op.maybe = True
    p[0] = _makeConstructProp(p[3], T.forcelist(op), True, False, False)

#XXX
"""
def p_constructitem_include(p): 
    '''
    constructitem : INCLUDE dictconstruct
    listconstructitem : INCLUDE listconstruct
    '''
    #XXX
    p[0] = _makeConstructProp(None, p[2], False)

def p_constructitem_exclude1(p): 
    '''
    constructitem : EXCLUDE barecolumnreflist
    listconstructitem : EXCLUDE arrayindexlist    
    '''
    p[0] = p[2] #XXX _makeConstructProp(None, p[2], False)

def p_constructitem_exclude2(p): 
    '''     
    constructitem : EXCLUDE barecolumnreflist WHEN expression
    listconstructitem : EXCLUDE arrayindexlist WHEN expression 
    '''
    exp = p[4] #XXX
    p[0] = p[2] #_makeConstructProp(None, p[2], False)
"""

def p_arrayindex(p):
    '''
    arrayindex : INT
                | TIMES
    '''
    p[0] = p[1]
    
def p_barecolumnref(p):
    '''barecolumnref : NAME
                    | TIMES
                    | PROPSTRING
    '''
    p[0] = p[1]

#IMPORTANT needs to follow "constructitem : barecolumnref" rule because of reduce/reduce conflict
def p_columnref_trailer(p):
    '''
    columnreftrailer : barecolumnref
                    | columnreftrailer PERIOD barecolumnref
    '''
    if len(p) == 2:
        p[0] = [ p[1] ]
    else:
        p[0] = p[1]
        p[1].append(p[3])

def p_columnref(p):
    '''
    columnref : LABEL PERIOD columnreftrailer
              | columnreftrailer
    '''
    if len(p) == 2:
        p[0] = Project(p[1])
    else: #?var.column
        p[0] = Project(p[3], p[1][0])

def p_columnref_id(p):
    '''
    columnref : LABEL PERIOD columnreftrailer PERIOD ID
              | columnreftrailer PERIOD ID
    '''
    if len(p) == 2:
        p[0] = Project(p[1], constructRefs=False)
    else: #?var.column
        p[0] = Project(p[3], p[1][0], constructRefs=False)

def p_dictvalue(p): 
    '''
    dictvalue : expression
              | LBRACKET expression RBRACKET
              | nestedconstruct
              | LBRACKET nestedconstruct RBRACKET
    '''
    if len(p) == 4:
        p[0] = T.forcelist(p[2])
    else:
        p[0] = p[1]

def p_constructop_where(p):
    '''
    constructop : WHERE expression
    '''
    p[0] = T.constructop(p[1], p[2])

def p_constructop_groupby(p):
    '''
    constructop : GROUP BY arglist
    '''
    p[0] = T.constructop("groupby", p[3])

def p_constructop2(p):
    '''
    constructop : LIMIT INT
                | OFFSET INT
                | DEPTH INT
    '''
    p[0] = T.constructop(p[1], p[2])

def p_constructop_orderby(p):
    '''
    constructop : ORDER BY sortexplist
    '''
    p[0] = T.constructop("orderby", OrderBy(*p[3]) )

def p_constructop5(p):
    '''
    constructop : MERGEALL
    '''
    p[0] = T.constructop(p[1], True)

def p_constructop6(p):
    '''
    constructop : NAMEMAP EQ jsondict
    '''
    p[0] = T.constructop(p[1], p[3])

def p_jsondict(p):
    '''    
    jsondict : LBRACE jsondictlist RBRACE
    '''
    p[0] = dict(p[2])

def p_jsondictitem(p):
    '''
    jsondictitem : STRING COLON STRING
                 | STRING COLON jsondict
                 | NAME COLON STRING
                 | NAME COLON jsondict
    '''
    p[0] = (p[1], p[3])

def p_orderbyexp_1(p):
    '''
    sortexp : expression
            | expression ASC
    '''
    p[0] = SortExp(p[1])

def p_orderbyexp_2(p):
    '''
    sortexp : expression DESC
    '''
    p[0] = SortExp(p[1], True)

def p_constructlist(p):
    '''
    dictconstructlist : constructitemlist constructoplist 
                      | constructitemlist COMMA constructoplist
                      | constructitemlist COMMA constructoplist COMMA

    listconstructlist : listconstructitemlist constructoplist 
                      | listconstructitemlist COMMA constructoplist
                      | listconstructitemlist COMMA constructoplist COMMA    
    '''
    if len(p) == 3:
        p[0] = [ p[1], p[2] ]
    else:
        p[0] = [p[1], p[3]]

def p_construct(p):
    '''
    dictconstruct : LBRACE LABEL COMMA dictconstructlist RBRACE
                  | LBRACE LABEL dictconstructlist RBRACE
                  | LBRACE dictconstructlist RBRACE

    listconstruct : LBRACKET LABEL COMMA listconstructlist RBRACKET
                  | LBRACKET LABEL listconstructlist RBRACKET
                  | LBRACKET listconstructlist RBRACKET
    '''
    if len(p) == 4:
        p[0] = T.construct(p[1], None, *p[2])
    elif len(p) == 5:
        p[0] = T.construct(p[1], p[2], *p[3])
    else:
        assert len(p) == 6
        p[0] = T.construct(p[1], p[2 ], *p[4])

def p_valueconstruct(p):
    '''
    valueconstruct :  LPAREN expression constructoplist RPAREN        
                    | LPAREN expression COMMA constructoplist RPAREN
                    | LPAREN expression COMMA constructoplist COMMA RPAREN
    '''
    props = [_makeConstructProp(None, p[2], False, False, False)]
    
    if len(p) == 5:
        p[0] = T.construct('(', None, props, p[3])
    else:
        p[0] = T.construct('(', None, props, p[4])

def p_listconstructitem(p):
    '''
    listconstructitem : expression
    '''
    p[0] = _makeConstructProp(None, p[1], False, False, False)

def p_error(p):
    #print "p_error:", p.lexpos, p.lineno, p.type, p.value
    if p:
        p.lexer.errorlog.error("Syntax error at '%s' (line %d char %d)" % (p.value, p.lineno, p.lexpos))
    else:
        threadlocals.lexer.errorlog.error("Syntax error at EOF")

def p_empty(p):
    'empty :'
    pass

#parser = ply.yacc.yacc(start="root", errorlog=errorlog, optimize=1)#, debug=True)

####parse-tree-to-ast mapping ####
def buildparser(errorlog=None, debug=0):
    if not errorlog:
        import sys
        errorlog = ply.yacc.PlyLogger(sys.stderr)

    try:
        import vesper.query.lextab
        lextab=vesper.query.lextab
    except ImportError:
        if not debug: errorlog.warning('vesper.query.lextab not found, generating local lextab.py')
        lextab = 'lextab'
    
    if debug:
        lexer = ply.lex.lex(errorlog=errorlog, debug=1)
    else:
        lexer = ply.lex.lex(errorlog=errorlog, lextab=lextab, optimize=1)
    lexer.errorlog=errorlog
    
    try:
        import vesper.query.parsetab
        tabmodule=vesper.query.parsetab
    except ImportError:
        if not debug: errorlog.warning('vesper.query.parsetab not found, generating local parsetab.py')
        tabmodule = 'parsetab'

    try:
        if debug:
            parser = ply.yacc.yacc(start="root", errorlog=errorlog)
        else:
            parser = ply.yacc.yacc(start="root", errorlog=errorlog,
                                        tabmodule=tabmodule, optimize=1)
        parser.errorlog=errorlog
    except AttributeError:
        #yacc.write_table throws AttributeError: 'module' object has no attribute 'split'
        #because its expecting a string not a module
        #this happens when the jql.parsetab is out of data
        tabmodule = 'parsetab'
        errorlog.warning('vesper.query.parsetab was out of date, generating local parsetab.py')
        parser = ply.yacc.yacc(start="root", errorlog=errorlog,tabmodule=tabmodule, optimize=1)
        parser.errorlog = errorlog

    return lexer, parser

#each thread needs its own lexer and parser
import threading, thread
threadlocals = threading.local()

def parse(query, functions, debug=False, namemap=None):
    
    #get a separate logger for each thread so that concurrent parsing doesn't
    #intermix messages
    #XXX don't use python logging for this        
    errorlog=logging.getLogger('parser.%s' % thread.get_ident())
    try:
        lexer = threadlocals.lexer
        parser= threadlocals.parser
    except AttributeError:
        #create a new parser for this thread
        lexer, parser = buildparser(errorlog=errorlog)
        threadlocals.lexer, threadlocals.parser = lexer, parser
        
    lexer.lineno = 1 #XXX report ply bug: there doesn't seem to be any way to reset the lexer?
    
    # create a new log handler per-parse to capture messages
    log_messages = LogCaptureHandler(10)
    errorlog.addHandler(log_messages)    
    try:
        from vesper.query import rewrite
        parseState = rewrite._ParseState(functions, namemap)
        parser.jqlState = parseState
        
        #XXX only turn tracking on if there's an error
        r = parser.parse(query,lexer, tracking=True, debug=debug)
        # log messages should be safe to serialize
        msgs = ["%s: %s" % (tmp.levelname, tmp.getMessage()) for tmp in log_messages.buffer]
        return (r, msgs)
    finally:
        errorlog.removeHandler(log_messages)
