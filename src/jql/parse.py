'''
parse JQL 
'''

from jqlAST import *
import logging, logging.handlers
#errorlog = logging.getLogger('parser')

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

class QName(Tag):
    __slots__ = ()

    def __new__(cls, prefix, name):
        return tuple.__new__(cls, (prefix, name) )

    prefix = property(lambda self: self[0])
    name = property(lambda self: self[1])

#####PLY ####

import ply.lex
import ply.yacc

###########
#### TOKENS
###########

reserved = ('TRUE', 'FALSE', 'NULL', 'NOT', 'AND', 'OR', 'IN', 'IS', #'NS', 
           'ID', 'MAYBE', 'WHERE', 'LIMIT', 'OFFSET', 'DEPTH', 'MERGEALL',
           'GROUPBY', 'ORDERBY', 'ASC', 'DESC', 'INCLUDE', 'EXCLUDE', 'WHEN')

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

    'PROPSTRING', 'LABEL', 'QNAME', 'QSTAR', 'BINDVAR'
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
    r'''(?:"(?:[^"\n\r\\]|(?:"")|(?:\\x[0-9a-fA-F]+)|(?:\\.))*")|(?:'(?:[^'\n\r\\]|(?:'')|(?:\\x[0-9a-fA-F]+)|(?:\\.))*')'''
    t.value = t.value[1:-1]
    return t

def t_PROPSTRING(t):
    r'''(?:<(?:[^<>\n\r\\]|(?:<>)|(?:\\x[0-9a-fA-F]+)|(?:\\.))*>)'''
    t.value = t.value[1:-1]
    return t

def t_LABEL(t):
    v = t.value[1:]
    t.value = T.label(v)
    return t
t_LABEL.__doc__ = r'\?'+ _namere +''

def t_QNAME(t):
    prefix, name = t.lexer.lexmatch.group('prefix','name')
    if prefix:
        t.value = QName(prefix[:-1], name)
    else:
        key = t.value.lower() #make keywords case-insensitive (like SQL)
        t.type = reserved_map.get(key,"NAME")
        t.value = reserved_constants.get(key, t.value)
    return t
t_QNAME.__doc__ = '(?P<prefix>'+_namere+':)?(?P<name>' + _namere + ')'

def t_BINDVAR(t):
    t.value = t.value[1:]
    return t
t_BINDVAR.__doc__ = r'\@'+ _namere +''

def t_QSTAR(t):    
    t.value = QName(t.value[:-2], '*')
    return t
t_QSTAR.__doc__ = _namere + r':\*'

# SQL/C-style comments
def t_comment(t):
    r'/\*(.|\n)*?\*/'
    t.lexer.lineno += t.value.count('\n')

# Comment (both Python and C++-Style)
def t_linecomment(t):
    r'(//|\#).*\n'
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
                if c.ns:
                    nsmap.update(c.ns)
            else:
                resolveQNames(nsmap.copy(), c)
        else:
            c._resolveQNames(nsmap) 

def p_root(p):
    '''
    root : topconstruct
    '''
    p[0] = p[1]
    resolveQNames({}, p[0])
    
    #we're when done parsing, now join together joins that share labels,
    #removing any joined joins if their parent is a dependant (non-root) Select op
    labeledjoins = {}
    for label, joins in p.parser.jqlState.labeledjoins.items():
        if not joins:
            continue
        #construct that only have id labels will not have a join
        #we only want to add the join if there are no another joins for the label
        firstjoin = joins.pop()
        for join in joins:
            join.parent.removeArg(join) #XXX better support for removeArg
            firstjoin.appendArg(join)

        labeledjoins[label] = firstjoin
        firstjoin.name = label 
    #print 'labeledjoins', labeledjoins
    #print 'refs', p.jqlState.labelreferences
    p.parser.jqlState._buildJoinsFromReferences(labeledjoins)
    select = p[0]
    if not select.where or not select.where.args:
        #top level queries without a filter (e.g. {*}) 
        #should not include anyonmous objects that have already appeared 
        select.skipEmbeddedBNodes = True

def p_construct0(p):
    '''
    topconstruct : dictconstruct
                | listconstruct
                | valueconstruct

    construct : dictconstruct
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
    defaults = dict(where = None, ns = {}, offset = None, limit = None,
                    groupby = None, depth=None, orderby=None)

    if len(p[1]) > 3 and p[1][3]:
        for constructop in p[1][3]:
            defaults[ constructop[0].lower() ] = constructop[1]

    groupby = defaults['groupby']
    if groupby:
        if isinstance(groupby[0], Constant):
            arg = Project(groupby[0].value)
        elif isinstance(groupby[0], Project):
            arg = groupby[0]
        else:
            raise QueryException("bad group by expression")
                
        defaults['groupby'] = groupby = GroupBy(arg)
    
    if label:
        assert isinstance(label, T.label)
        op.id.appendArg( Label(label[0]) )
    where = defaults['where'] = p.parser.jqlState._joinFromConstruct(op, defaults['where'], groupby)
    #XXX add support for ns constructop    
    
    p[0] = Select(op, **defaults)
    assert not where or where.parent is p[0]

precedence = (
    ('left', 'ASSIGN'),
    ('left', 'OR'),
    ('left', 'AND'),
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

def p_expression_andmaybe(p):
    'expression : expression AND MAYBE expression'    
    p[4].maybe = True
    p[0] = And(p[1], p[4])

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

#next one must come after above rule so that it takes priority
def p_constructitem6(p):
    '''
    constructitem : ID
    '''    
    p[0] = _makeConstructProp(p[1], Project(0), True, False)

def p_atom_id(p):
    """
    atom : ID
    """
    p[0] = Project(p[1])

def p_funcname(p):
    '''funcname : NAME
                | QNAME
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
    arglist : arglist COMMA argument
            | arglist COMMA keywordarg
            | keywordarg
            | argument
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
    """
    p[0] = []

def p_argument(p):
    '''
    argument : expression
    '''
    p[0] = p[1]

def p_keyword_argument(p):
    '''
    keywordarg : NAME EQ expression  %prec ASSIGN
    '''
    p[0] = T.keywordarg(p[1], p[3])

def p_join(p):
    "join : LBRACE expression RBRACE"
    try:
        p[0] = p.parser.jqlState.makeJoinExpr(p[2])
    except QueryException, e:
        import traceback
        traceback.print_exc()#file=sys.stdout)
        p[0] = ErrorOp(p[2], "Invalid Join")
        p.parser.errorlog.error("invalid join: "  +  str(e) + ' ' + repr(p[2]))

def _makeConstructProp(n, v, nameIsFilter, derefName = False):
    if n == '*':
        n = None
        nameIsFilter = False
    
    if derefName:
        derefName = n
        n = None
    
    if isinstance(v, T.forcelist):
        return ConstructProp(n, v[0],
                PropShape.uselist, PropShape.uselist, nameIsFilter, nameFunc=derefName)
    else:
        return ConstructProp(n, v, nameIsFilter=nameIsFilter, nameFunc=derefName)

def p_constructitem3(p):
    '''
    constructitem : expression COLON dictvalue
    '''
    p[0] = _makeConstructProp(p[1], p[3], False, True)

def p_constructitem4(p):
    '''
    constructitem : barecolumnref
    '''    
    p[0] = _makeConstructProp(p[1], Project(p[1]), True, False)

def p_constructitem5(p): 
    '''
    constructitem : LBRACKET barecolumnref RBRACKET
    '''
    p[0] = _makeConstructProp(p[2], T.forcelist(Project(p[2])), True, False)

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

def p_arrayindex(p):
    '''
    arrayindex : INT
                | TIMES
    '''
    p[0] = p[1]
    
def p_barecolumnref(p):
    '''barecolumnref : NAME
                    | QNAME
                    | TIMES
                    | PROPSTRING
                    | QSTAR
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

def p_dictvalue(p): 
    '''
    dictvalue : LBRACKET construct RBRACKET
              | construct
              | expression              
    '''
    if len(p) == 4:
        p[0] = T.forcelist(p[2])
    else:
        p[0] = p[1]

def p_constructop1(p):
    '''
    constructop : WHERE LPAREN expression RPAREN
                | GROUPBY LPAREN arglist RPAREN
    '''
    #           | NS LPAREN arglist RPAREN
    p[0] = T.constructop(p[1], p[3])

def p_constructop2(p):
    '''
    constructop : LIMIT INT
                | OFFSET INT
                | DEPTH INT
    '''
    p[0] = T.constructop(p[1], p[2])

def p_constructop3(p):
    '''
    constructop : WHERE LPAREN MAYBE expression RPAREN
    '''
    p[4].maybe = True
    p[0] = T.constructop(p[1], p[4])

def p_constructop4(p):
    '''
    constructop : ORDERBY LPAREN sortexplist RPAREN
    '''
    p[0] = OrderBy(p[3])

def p_constructop5(p):
    '''
    constructop : MERGEALL
    '''
    p[0] = T.constructop(p[1])

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
    props = [_makeConstructProp(None, p[2], False)]
    
    if len(p) == 5:
        p[0] = T.construct('(', None, props, p[3])
    else:
        p[0] = T.construct('(', None, props, p[4])

def p_listconstructitem(p):
    '''
    listconstructitem : expression
    '''
    p[0] = _makeConstructProp(None, p[1], False)

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
        import jql.lextab
        lextab=jql.lextab
    except ImportError:
        if not debug: errorlog.warning('jql.lextab not found, generating local lextab.py')
        lextab = 'lextab'
    
    if debug:
        lexer = ply.lex.lex(errorlog=errorlog, debug=1)
    else:
        lexer = ply.lex.lex(errorlog=errorlog, lextab=lextab, optimize=1)
    lexer.errorlog=errorlog
    
    try:
        import jql.parsetab
        tabmodule=jql.parsetab
    except ImportError:
        if not debug: errorlog.warning('jql.parsetab not found, generating local parsetab.py')
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
        errorlog.warning('jql.parsetab was out of date, generating local parsetab.py')
        parser = ply.yacc.yacc(start="root", errorlog=errorlog,tabmodule=tabmodule, optimize=1)
        parser.errorlog = errorlog

    return lexer, parser

#each thread needs its own lexer and parser
import threading, thread
threadlocals = threading.local()

def parse(query, functions, debug=False):
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
        import jql.rewrite
        parseState = jql.rewrite._ParseState(functions)        
        parser.jqlState = parseState
        
        #XXX only turn tracking on if there's an error
        r = parser.parse(query,lexer, tracking=True, debug=debug)
        # log messages should be safe to serialize
        msgs = ["%s: %s" % (tmp.levelname, tmp.getMessage()) for tmp in log_messages.buffer]
        return (r, msgs)
    finally:
        errorlog.removeHandler(log_messages)
