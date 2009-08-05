'''
basic structure of a jql query:

construct = {
columnname | string : expression | [expression?] | construct
*
where(
    (expression,) |
    (columname = expression,)+
)

querypartfunc(expression) # groupby | orderby | limit | offset
}

{foo : bar} #construct foo, where foo = bar
{"foo" : "bar"} #construct "foo" : "bar"
{"foo" : bar}  #construct foo, value of bar property (on this object)
#construct foo where value of baz on another object
{foo : ?child.baz
    where ({ id=?child, bar="dd"})
}

#construct foo with a value as the ?child object but only matching baz property
{'foo' : { id : ?child, * }
    where ( foo = ?child.baz)
}

#same as above, but only child id as the value
{'foo' : ?child
    where ( foo = ?child.baz)
}

'''

from jqlAST import *
import logging
#logging.basicConfig() #XXX only if logging hasn't already been set
errorlog = logging.getLogger('parser')

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
            tagclass = type(Tag)(attr, (Tag,), {})
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

reserved = ('TRUE', 'FALSE', 'NULL', 'NOT', 'AND', 'OR', 'IN', 'IS', 'NS',
           'ID', 'OPTIONAL', 'WHERE', 'LIMIT', 'OFFSET', 'GROUPBY', 'ORDERBY')

tokens = reserved + (
    # Literals (identifier, integer constant, float constant, string constant, char const)
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

    'URI', 'VAR', 'QNAME', 'QSTAR'
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

def t_URI(t):
    r'''<(([a-zA-Z][0-9a-zA-Z+\\-\\.]*:)/{0,2}[0-9a-zA-Z;/?:@&=+$\\.\\-_!~*'()%]+)?("\#[0-9a-zA-Z;/?:@&=+$\\.\\-_!~*'()%]+)?>'''
    t.value = t.value[1:-1]
    return t

def t_VAR(t):
    v = t.value[1:]
    t.value = T.var(v)
    return t
t_VAR.__doc__ = r'\?'+ _namere +''

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
    errorlog.error("Illegal character %s" % repr(t.value[0]))
    t.lexer.skip(1)

# Newlines
def t_NEWLINE(t):
    r'(\n|\r)+'
    t.lexer.lineno += max(t.value.count("\n"),t.value.count("\r"))

# Completely ignored characters
t_ignore = ' \t\x0c'

lexer = ply.lex.lex(errorlog=errorlog) #optimize=1)

# Parsing rules
class _ParseState(object):
    def __init__(self):
        self.labeledjoins = {}
        self.labeledjoinorder = []
        self.labelreferences = {}
        self._anonJoinCounter = 0

    def addLabeledJoin(self, name, join):
        if join.name:
            if join.name != name:
                raise QueryException(
                   "can't assign id %s, join already labeled %s: %s"
                    % (name, join.name, join))
        else:
            join.name = name

        self.labeledjoins.setdefault(name,[]).append(join)

        if name in self.labeledjoinorder:
            #outermost wins
            self.labeledjoinorder.remove(name)
        #assumes this is called in bottoms-up parse order
        self.labeledjoinorder.append(name)

    def getLabeledJoin(self, name):
        jlist = self.labeledjoins.get(name)
        if not jlist:
            return None
        return jlist[0]

    def nextAnonJoinId(self):
        self._anonJoinCounter += 1
        return '@' + str(self._anonJoinCounter)

    def joinMoved(self, join, from_, to):
        #print 'moving',join, 'from', from_, 'to', to
        #XXX test: { id = ?self and ?self = 1 }
        if not join.name:
            join.name = self.nextAnonJoinId()
        if not from_ or join is to or from_ is to:
            return False
        #XXX implement replaceArg for filter, any expression arg
        if from_.parent:
            from_.replaceArg(join, Label(join.name))
            return True
        return False

def _YaccProduction_getattr__(self, name):
    if name == 'jqlState':        
        parseState = _ParseState()
        self.jqlState = parseState
        return parseState
    else:
        raise AttributeError, name

#there doesn't seem to be an decent way to store glabal parse state
#so monkey patch the "p" so that a state object is created upon first reference
assert not hasattr(ply.yacc.YaccProduction,'__getattr__')
ply.yacc.YaccProduction.__getattr__ = _YaccProduction_getattr__

def _joinFromConstruct(construct, where, parseState):
    '''
    build a join expression from the construct pattern
    '''
    left = where
    for prop in construct.args:
        if prop == construct.id:
            pass
        else:
            if isinstance(prop.value, Select):
                value = prop.value.where
            else:
                value = prop.value
            if value == Project('*'):
                value = None

            if prop.nameIsFilter:
                if value:
                    value = Eq(Project(prop.name), value)                    
                else:
                    value = Project(prop.name)
                prop.appendArg( Project(prop.name) )
                
                if not left:
                    left = value
                else:
                    left = And(left, value)
            elif value:
                #don't want treat construct values as boolean filter
                #but we do want to find projections which we need to join
                #(but we skip project(0) -- no reason to join)
                for child in value.depthfirst(
                 descendPredicate=lambda op: not isinstance(op, ResourceSetOp)):
                    if isinstance(child, Project) and child.fields != [SUBJECT]:
                        import copy
                        if not left:                            
                            left = copy.copy( child )
                        else:
                            assert child
                            left = And(left, copy.copy( child ))

            #XXX: handle outer joins:
            #if prop.ifEmtpy == PropShape.omit:
            #    jointype = JoinConditionOp.RIGHTOUTER
            #else:
            #    jointype = JoinConditionOp.INNER
            #join.appendArg(JoinConditionOp(filter, SUBJECT,jointype))

    if left:
        left = makeJoinExpr(left, parseState)
        assert left
    
    if not left:
        left = Join()

    if construct.id:
        name = construct.id.getLabel()
        assert left
        try:
            parseState.addLabeledJoin(name, left)
        except:
            #print construct
            #print where
            #print left
            raise

    return left

def p_root(p):
    '''
    root : construct
    '''
    p[0] = p[1]
    #we're when done parsing, now join together joins that share labels,
    #removing any joined joins if their parent is a dependant (non-root) Select op
    labeledjoins = {}
    for label, joins in p.jqlState.labeledjoins.items():
        if not joins:
            continue
        #construct that only have id labels will not have a join
        #we only want to add the join if there are no another joins for the label
        firstjoin = joins.pop()
        for join in joins:
            join.parent.removeArg(join) #XXX
            firstjoin.appendArg(join)
        labeledjoins[label] = firstjoin
        firstjoin.name = label 
    #print 'labeledjoins', labeledjoins
    #print 'refs', p.jqlState.labelreferences
    _buildJoinsFromReferences(labeledjoins, p.jqlState)
    if not p[0].where:
        p[0].appendArg( Join() )
    #print 'root where', p[0].where

def p_construct(p):
    '''
    construct : dictconstruct
                | listconstruct
    '''

    if isinstance(p[1], T.listconstruct):
        shape = Construct.listShape
    elif isinstance(p[1], T.dictconstruct):
        shape = Construct.dictShape
    else:
        assert 0, 'unexpected token'

    props = p[1][0]
    op = Construct(props, shape)

    defaults = dict(where = None, ns = {}, offset = -1, limit = - 1,
                    groupby = None)
    if len(p[1]) > 1 and p[1][1]:
        for constructop in p[1][1]:
            defaults[ constructop[0].lower() ] = constructop[1]

    where = _joinFromConstruct(op, defaults['where'], p.jqlState)
    #XXX other constructops: limit, offset, ns, groupby    
    p[0] = Select(op, where)
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

def p_expression_notin(p):
    """
    expression : expression NOT IN expression
    """    
    p[0] = Not(In(p[1], p[4]))

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
    #print [repr(p[i]) for i in range(0,4)]
    p[0] = _opmap[p[2].upper()](p[1], p[3])

def p_expression_uminus(p):
    '''expression : MINUS expression %prec UMINUS
                  | PLUS expression %prec UPLUS'''
    if p[1] == '-':
        p[0] = qF.getOp('negate',p[2])
    else:
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

def p_atom_var(p):
    """atom : VAR
    """
    p[0] = Label(p[1][0])

def p_atom_id(p):
    """atom : ID
    """
    p[0] = Project(p[1])

def p_barecolumnref(p):
    '''barecolumnref : NAME
                    | QNAME
                    | TIMES
                    | URI
                    | QSTAR
    '''
    p[0] = p[1]

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
    columnref : VAR PERIOD columnreftrailer
              | columnreftrailer
    '''
    if len(p) == 2:
        p[0] = Project(p[1])
    else: #?var.column
        p[0] = Project(p[3], p[1][0])

def p_funcname(p):
    '''funcname : NAME
                | QNAME
    '''
    p[0] = p[1]

def p_funccall(p):
    "funccall : funcname LPAREN arglist RPAREN"
    try:
        p[0] = qF.getOp(p[1], *p[3])
    except KeyError:
        msg = "unknown function " + p[1]
        p[0] = ErrorOp(p[3], msg)
        errorlog.error(msg)

def p_arglist(p):
    """
    arglist : arglist COMMA argument
            | arglist COMMA keywordarg
            | keywordarg
            | argument
    constructitemlist : constructitemlist COMMA constructitem
                      | constructitem
    constructoplist : constructoplist COMMA constructop
                      | constructop
    listconstructitemlist : listconstructitemlist COMMA listconstructitem
                          | listconstructitem
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
    constructitemlist : constructempty
    constructoplist : empty
    listconstructitemlist : empty
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
        p[0] = makeJoinExpr(p[2], p.jqlState)
    except QueryException, e:
        import traceback
        traceback.print_exc()#file=sys.stdout)
        p[0] = ErrorOp(p[2], "Invalid Join")
        errorlog.error("invalid join: "  +  str(e) + ' ' + repr(p[2]))

def _makeConstructProp(n, v, nameIsFilter):
    if isinstance(v, T.forcelist):
        return ConstructProp(n, v[0],
                PropShape.uselist, PropShape.uselist, nameIsFilter)
    else:
        return ConstructProp(n, v, nameIsFilter=nameIsFilter)

def p_constructitem1(p):
    '''
    constructitem : STRING COLON dictvalue
    '''
    p[0] = _makeConstructProp(p[1], p[3], False)

def p_constructitem2(p):
    '''
    constructitem : columnname COLON dictvalue
    '''
    p[0] = _makeConstructProp(p[1], p[3], True)

def p_constructitem3(p):
    '''
    constructitem : ID COLON VAR
    '''
    p[0] = ConstructSubject(value=p[3][0])

def p_constructitem4(p):
    '''
    constructitem : TIMES
    '''
    p[0] = ConstructProp(None, Project('*'))

def p_constructitem5(p):
    '''
    constructitem : optional
    '''
    p[0] = p[1]

#def p_constructitem(p):
#    '''
#    constructitem : dictkey COLON dictvalue
#                    | optional
#                    | TIMES
#    '''
#    if len(p) == 2:
#        if p[1] == '*':
#            p[0] = ConstructProp(None, Project('*'))
#        else:
#            p[0] = p[1]
#    else:
#        p[0] = ConstructProp(p[1], p[3])
#
#def p_dictkey(p):
#    '''
#    dictkey : STRING
#            | columnname
#    '''
#    p[0] = p[1]

def p_columnname(p): 
    '''
    columnname : NAME
               | QNAME
               | URI
    '''
    p[0] = p[1]

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

def p_optional(p):
    '''
    optional : OPTIONAL LPAREN constructitemlist RPAREN
             | OPTIONAL LPAREN constructitemlist COMMA RPAREN
    '''
    for i, prop in enumerate(p[3]):
        if isinstance(prop, ConstructSubject):
            p[3][i] = ErrorOp(prop, "Subject in Optional")
            errorlog.error('subject spec not allowed in Optional')
        else:
            prop.ifEmpty = PropShape.omit
            #XXX jc.join = 'outer'
    p[0] = p[3]

def p_constructop(p):
    '''
    constructop : constructopname LPAREN expression RPAREN
    '''
    p[0] = T.constructop(p[1], p[3])

XXX = '''
constructop : WHERE LPAREN expression RPAREN
            | GROUPBY LPAREN columnamelist RPAREN
            | ORDERBY LPAREN columnamelist RPAREN
            | NS LPAREN keywordarglist RPAREN
            | LIMIT NUMBER
            | OFFSET NUMBER
'''

def p_constructopname(p):
    '''
    constructopname : WHERE
                    | LIMIT
                    | OFFSET
                    | GROUPBY
                    | ORDERBY
                    | NS
    '''
    p[0] = p[1]

def p_dictconstruct(p):
    '''
    dictconstruct : LBRACE constructitemlist RBRACE
                  | LBRACE constructitemlist constructoplist RBRACE
                  | LBRACE constructitemlist COMMA constructoplist RBRACE                  
                  | LBRACE constructitemlist COMMA constructoplist COMMA RBRACE
    '''
    if len(p) == 4:
        p[0] = T.dictconstruct( p[2], None)
    elif len(p) == 5:
        p[0] = T.dictconstruct( p[2], p[3])
    else:
        p[0] = T.dictconstruct( p[2 ], p[4])

def p_listconstruct(p):
    '''
    listconstruct : LBRACKET listconstructitemlist RBRACKET
        | LBRACKET listconstructitemlist constructoplist RBRACKET
        | LBRACKET listconstructitemlist COMMA constructoplist RBRACKET
        | LBRACKET listconstructitemlist COMMA constructoplist COMMA RBRACKET
    '''
    if len(p) == 4:
        p[0] = T.listconstruct( p[2], None)
    elif len(p) == 5:
        p[0] = T.listconstruct( p[2], p[3])
    else:
        p[0] = T.listconstruct( p[2], p[4])

def p_listconstructitem(p):
    '''
    listconstructitem : expression
                      | optional
    '''
    #XXX should dictvalue:
    #p[0] = makeConstructProp(None, p[1], False)
    p[0] = p[1]

def p_error(p):
    if p:
        errorlog.error("Syntax error at '%s'" % p.value)
    else:
        errorlog.error("Syntax error at EOF")

def p_empty(p):
    'empty :'
    pass

def p_constructempty(p):
    'constructempty :'
    #redundant rule just to make it obvious that the related reduce/reduce
    #conflict is harmless
    pass

parser = ply.yacc.yacc(start="root", errorlog=errorlog ) #, debug=True)

####parse-tree-to-ast mapping ####

_opmap = {
"AND" : And,
"OR" : Or,
"NOT" : Not,
"IN" : In,
"=" : Eq,
"==" : Eq,
'!=' : lambda *args: Not(Eq(*args)),
'<' : lambda *args: Cmp('<',*args),
'>' : lambda *args: Cmp('>',*args),
'<=' : lambda *args: Cmp('<=',*args),
'>=' : lambda *args: Cmp('>=',*args),
'+' : lambda *args: qF.getOp('add',*args),
'-' : lambda *args: qF.getOp('sub',*args),
'*' : lambda *args: qF.getOp('mul',*args),
'/' : lambda *args: qF.getOp('div',*args),
'%' : lambda *args: qF.getOp('mod',*args),
}

logicalops = {
 And : Join,
 Or : Union,
}

def rewriteLabelsInFilters():
    '''
    Filter conditions that depend are a label are actually join predicates on
    the object that the label references. So we need to rewrite the filter and
    add join conditions:

    * build a multimap between joins and labels (label => [join])
    whenever encountered: id = ?label, ?foo.bar and id : ?foo
    * build joincondition map for other references to labels
      (label => [(join, joinconditionpred)]), exclude joinconditionpred from filter
    * when done parsing, join together joins that share labels,
      removing any joined joins if their parent is a dependant (non-root) Select op

    this:
    {
    id : ?owner,
    'mypets' : {
          'dogs' : { * where(owner=?owner and type='dog') },
          'cats' : { * where(owner=?owner and type='cat') }
        }
    }

    equivalent to:
    {
    id : ?owner,

    'mypets' : {
          'dogs' : { * where(id = ?pet and type='dog') },
          'cats' : { * where(id = ?pet and type='cat') }
        }

    where ( {id = ?pet and owner=?owner} )
    }

    (but what about where ( { not id = ?foo or id = ?bar and id = ?baz }

    also, this:
    {
    'foo' : ?baz.foo
    'bar' : ?baz.bar
    }
    results in joining ?baz together

    here we don't join but use the label to select a value
    { 
      'guardian' : ?guardian,
      'pets' : { * where(owner=?guardian) },
    }

this is similar but does trigger a join on an unlabeled object:
    {
      'guardian' : ?guardian,
      'dogs' : { * where(owner=?guardian and type='dog') },
      'cats' : { * where(owner=?guardian and type='cat') }
    }

join( filter(eq(project('type'), 'dog')),
     filter(eq(project('owner'),objectlabel='guardian')
  jc(
     join( filter(eq(project('type'), 'cat')),
          filter(eq(project('owner'),objectlabel='guardian')
     ),
     Eq(Project('guardian'), Project('guardian'))
)

XXX test multiple labels in one filter, e.g.: { a : ?foo, b : ?bar where (?foo = ?bar) }
XXX test self-joins e.g. this nonsensical example: { * where(?foo = 'a' and ?foo = 'b') }
    '''

def _buildJoinsFromReferences(labeledjoins, parseState):
    skipped = []
    for join, conditions in parseState.labelreferences.items():
        currentjoin = join

        def labelkey(item):
            label = item[0]
            try:
                return parseState.labeledjoinorder.index(label)
            except ValueError:
                return 999999 #sort at end
        #sort by order of labeled join appearence
        #XXX is that enough for correctness? what about sibling joins?
        conditions.sort(key=labelkey)        
        for label, (op, pred) in conditions:
            labeledjoin = labeledjoins.get(label)
            if not labeledjoin:
                if label in skipped:
                    #XXX support unlabeled joins
                    raise QueryException('unlabeled joins not yet supported')
                else:
                    #XXX keep skipped around to check if there are construct labels
                    #for this label, if not, emit warning
                    skipped.append(label)
                continue

            if op is join:
                #any subsequent join predicates should operate on the new join
                op = currentjoin
            if op is not labeledjoin:
                if isinstance(op, Join):
                    parseState.joinMoved(op, op.parent, labeledjoin)
                labeledjoin.appendArg(JoinConditionOp(op, pred))
            currentjoin = labeledjoin

    if skipped: #XXX should just be warning?
        raise QueryException(
                'reference to unknown label(s): '+ ', '.join(skipped))
    return skipped

def _getASTForProject(project, parseState):
    '''
    Return an op that will retrieve the values that match the projection

    bar return 
    
    Filter(Eq('bar', Project(PROPERTY)), objectlabel='bar'))

    foo.bar returns

    jc(Join(
        jc(Filter(Eq('bar', Project(PROPERTY)), objectlabel='bar'), OBJECT),
        jc(Filter(Eq('foo', Project(PROPERTY)) ), SUBJECT)
      ),
    '_1')

    JoinCondition(
    Join(
        jc(Filter(None, Eq('bar'), None, subjectlabel='_1'), OBJECT),
        jc(Filter(None, Eq('baz'), None, objectlabel='baz'), OBJECT)
      )
    '_1')

    In other words, join the object value of the "bar" filter with object value
    of "baz" filter.
    We add the label'baz' so that the project op can retrieve that value.
    The join condition join this join back into the enclosing join
    using the subject of the "bar" filter.

    bar == { ?id where }

    Filter(Eq('bar'), Join(jc(
        Filter(None, Eq('foo'), None, propertyname='foo'), 'foo'))

    ?foo.bar is shorthand for
    { id : ?foo where(bar) }
    thus:
    Join( Filter(Eq('bar',Project(PROPERTY)), subjectlabel='foo', objectlabel='bar') )

    '''
    #XXX we need to disabiguate labels with the same name
    op = None
    if project.name == SUBJECT:
        assert not project.varref
        return op

    for propname in reversed(project.fields):
        #XXX if propname == '*', * == OBJECT? what about foo = * really a no-op
        if not op:
            op = Filter(Eq(propname, Project(PROPERTY)), objectlabel=propname)
        else:
            subjectlabel = parseState.nextAnonJoinId()
            filter = Filter(Eq(propname, Project(PROPERTY)),
                                            subjectlabel=subjectlabel)
            #create a new join, joining the object of this filter with
            #the subject of the prior one
            op = JoinConditionOp(
                    Join( JoinConditionOp(op, SUBJECT),
                        JoinConditionOp(filter, OBJECT)), subjectlabel)

    if project.varref:
        #XXX fix this questionable hack: addLabel should be available on Joins, etc.
        for child in op.breadthfirst():
            if isinstance(child, Filter):
                child.addLabel(project.varref, SUBJECT)
                break
        op = Join(op)
        parseState.addLabeledJoin(project.varref, op)
    
    return op

def consolidateFilter(filter, projections):
    '''
    Crucial optimization is to consolidate filters that can be applied at once:
    
    e.g. instead foo = 'bar' being:


    Filter(Eq(Project(PROPERTY), 'foo'), objectlabel='foo')
    Filter(Eq(Project('foo'),'bar'))

    consolidate those into:

    Filter(Eq(Project(PROPERTY), 'foo'),Eq(Project(OBJECT), 'bar'), objectlabel='foo')

    A consolidated filter can only have predicate per position

    e.g. foo = bar

    goes from:

    Filter(Eq(Project(PROPERTY), 'foo'), objectlabel='foo')
    Filter(Eq(Project(PROPERTY), 'bar'), objectlabel='bar')
    Filter(Eq(Project('foo'),Project('bar')))

    only can be:

    Filter(Eq(Project(PROPERTY), 'bar'), objectlabel='bar')
    Filter(Eq(Project(PROPERTY), 'foo'),Eq(Project(OBJECT), Project('bar')) )

    however, this isn't much of an optimization, so, for simplicity, we don't bother
    '''
    #XXX consolidate subject filters
    filterprojects = [(p, i) for (i, (p,f)) in enumerate(projections)
                                                if isinstance(f, Filter)]
    if len(filterprojects) == 1:
        p, i = filterprojects[0]
        name = p.name
        assert len(p.fields) == 1
        p.fields = [OBJECT] #replace label with pos
        filter.appendArg( Eq(Project(PROPERTY), name) )
        filter.addLabel(name, OBJECT)
        #remove replaced filter:
        del projections[i]
        return True
    return False

def makeJoinExpr(expr, parseState):
    '''
    Rewrite expression into Filters, operations that filter rows
    and ResourceSetOps (join, union, except), which group together the Filter 
    results by id (primary key).
    
    We also need to make sure that filter which apply individual statements
    (id, property, value) triples appear before filters that apply to more than
    one statement and so operate on the simple filter results.

    filters to test:
    foo = (?a or ?b)
    foo = (a or b)
    foo = (?a and ?b)
    foo = (a and b)
    foo = {c='c'}
    foo = ({c='c'} and ?a)
    '''
    cmproots = []
    to_visit = []
    visited = set()
    to_visit.append( (None, expr) )

    labeledjoins = parseState.labeledjoins
    labelreferences = parseState.labelreferences

    newexpr = None
    while to_visit:
        parent, v = to_visit.pop()
        if id(v) not in visited:
            visited.add( id(v) )

            notcount = 0
            while isinstance(v, Not):
                notcount += 1
                assert len(v.args) == 1
                v = v.args[0]
            
            optype = logicalops.get(type(v))
            if optype:
                if notcount % 2: #odd # of nots
                #if the child of the Not is a logical op, we need to treat this
                #as a Except op otherwise just include it in the compare operation
                    notOp = Except()
                    if not parent:
                        parent = newexpr = notOp
                    else:
                        parent.appendArg(notOp)
                        parent = notOp

                if not parent:
                    parent = newexpr = optype()
                elif type(parent) != optype:
                    #skip patterns like: and(and()) or(or())
                    newop = optype()
                    parent.appendArg(newop)
                    parent = newop
                
                to_visit.extend([(parent, a) for a in v.args]) #descend
            else: #if not isinstance(v, Join): #joins has already been processed
                if not parent: #default to Join
                    parent = newexpr = Join()
                if notcount % 2:
                    v = Not(v)
                cmproots.append( (parent, v) )

    #for each top-level comparison in the expression    
    for parent, root in cmproots:
        #first add filter or join conditions that correspond to the columnrefs
        #(projections) that appear in the expression

        #look for Project ops but don't descend into ResourceSetOp (Join) ops
        projectops = []
        skipRoot = False
        labels ={}
        
        for child in root.depthfirst(
                descendPredicate=lambda op: not isinstance(op, ResourceSetOp)):
            if isinstance(child, ResourceSetOp):                
                if child is root or (child.parent is root
                                    and isinstance(child.parent, Not)):
                    #XXX standalone join, do an "(not) exists join"
                    skipRoot = True #don't include this root in this join
                else:
                    if not child.name:
                        child.name = parseState.nextAnonJoinId()
                        parseState.addLabeledJoin(child.name, child)
                    child = Label(child.name)
                    #replace this join with a Label
                    #XXX same as Label case (assign label if necessary)
                    raise QueryException('join in filter not yet implemented')
            if isinstance(child, Project):
                 projectop = _getASTForProject(child, parseState)
                 if projectop:
                    projectops.append( (child, projectop) )
            elif isinstance(child, Label):
                labels.setdefault(child.name,[]).append(child)

        if len(labels) > 1:
            if False:#len(labels) == 2:
                ##XXX throws 'can't assign id , join already labeled b: Join:'b'
                #with {id=?a and ?a = 1} and {id=?b and ?b = 2} and ?b = ?a
                #XXX currently only handle patterns like ?a = ?b
                #need to handle pattern like "foo = (?a or ?b)" (boolean)
                # or "?a = ?b = ?c"  or foo(?a,?b) or ?a != ?b
                (a, b) = [v[0] for v in labels.values()]
                if root == Eq(a, b):                    
                    #note: this isn't necessary associate with the parent op
                    #that may come later when joins on labelreferences are made
                    parentjoin = parseState.getLabeledJoin(a.name)
                    if parentjoin:
                        labelname = b.name
                        joincond = (b, SUBJECT)
                    else:
                        parentjoin = parseState.getLabeledJoin(b.name)
                        if parentjoin:
                            labelname = a.name
                            joincond = (a, SUBJECT)
                        else:
                            raise QueryException(
                'could not find reference to neither %s or %s' % (a.name, b.name))
                    labelreferences.setdefault(parentjoin, []).append(
                                            (labelname, joincond) )
                    skipRoot = True
            else:
                raise QueryException('expressions like ?a = ?b not yet supported')
        for labelname, ops in labels.items():
            child = ops[0] #XXX need to worry about expressions like foo(?a, ?a) ?
            if root == Eq(Project(SUBJECT), child):
                #its a declaration like id = ?label
                parseState.addLabeledJoin(labelname, parent)
            else: #label reference
                child.__class__ = Constant #hack so label is treated as independant
                if root.isIndependent():
                    #expr doesn't depend it's parent join, so  treat as filter
                    joincond = (Filter(root), SUBJECT) #filter, join pred
                else:
                    joincond = (parent, root) #join, join pred
                #replace the label reference with a Project(SUBJECT):
                Project(SUBJECT)._mutateOpToThis(child)
                labelreferences.setdefault(parent, []).append(
                                                    (labelname, joincond) )
                
            skipRoot = True #don't include this root in this join

        #try to consolidate the projection filters into root filter.
        if skipRoot:
            filter = Filter()
        else:
            filter = Filter(root)
            consolidateFilter(filter, projectops)
        for (project, projectop) in projectops:
            parent.appendArg(projectop)
        if not skipRoot:
            parent.appendArg( filter )

    #XXX remove no-op and redundant filters
    assert newexpr
    return newexpr

def parse(query):
    return parser.parse(query,tracking=True)#, debug=True)

