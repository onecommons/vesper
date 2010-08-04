#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
'''
convert a ply parser into reST productionlist suitable for Sphinx to generate grammar documentation.

See http://docs.python.org/dev/documenting/markup.html#grammar-production-displays for more info.
'''

import vesper.query.parse, re

token_productions = {
'LABEL' : ['"?" `name`'],
'BINDVAR' : ['"@" `name`'],
'QNAME' : ['`name` ":" `name`'],
'QSTAR' : ['`name` ":*"'], 
'PROPSTRING' :  ['"<" jsonchars+ ">"'],
'STRING' :  ['""" jsonchars* """', '"\'" jsonchars* "\'"'],
'NAME' : ['[A-Za-z_$][A-Za-z0-9_$]*'],
}

def replace_tokens(match):
    token = match.group(0)
    if token in token_productions:
        return '`'+token.lower()+'`'
    if token in vesper.query.parse.reserved:
        return '"'+token.lower()+'"'
    tokenvar = getattr(vesper.query.parse, 't_'+token, None)
    if token == 'EQ':
        return '"="'
    elif tokenvar and isinstance(tokenvar, str):
        return '"'+tokenvar.strip('\\')+'"'
    else:
        return token

productions = {}
tree = {}
for name in dir(vesper.query.parse):
    if name.startswith('p_') and name != 'p_empty':        
        doc = getattr(vesper.query.parse, name).__doc__
        if not doc:
            continue
        for m in re.finditer('(.+?)\:((.+?)$((\s*\|.+?$)*))', doc, re.M):
            if not m:
                continue
            production, first, rules  = m.group(1).strip(), m.group(3), m.group(4) 
            rulelist = [first]
            if rules:
                rulelist.extend( re.split(r'\s*\|', rules) )
            
            def replace_production(match):
                token = match.group(1)
                tree.setdefault(production, set()).add(token)
                return '`'+token+'`'

            for rule in rulelist:
                rule = rule.strip()
                if not rule:
                    continue
                rule = re.sub('%prec [A-Z]+', '', rule)
                rule = re.sub('([a-z]+)', replace_production,  rule)
                rule = re.sub('[A-Z]+', replace_tokens, rule)
                productions.setdefault(production, []).append(rule)            

visited = set(['root'])
order = []
while visited:
    start = visited.pop()
    order.append(start)
    if start not in tree:
        continue
    children = tree[start]
    for child in children:
        if child not in order:
            visited.add(child)

productions.update( token_productions )
order.extend( token_productions.keys() )

#.. include:: directive is broken, so add include all text here:
print '''
.. :copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
.. :license: Dual licenced under the GPL or Apache2 licences, see LICENSE.

Formal Grammar
===================

This grammar file is machine generated.
'''

print ".. productionlist::"
for key in order:
    if key == 'empty':
        continue
    value = productions[key]
    print "    ", key.lower().ljust(14), ":", ('\n'+(20*' ')+': |').join(value)
    #for http://www-cgi.uni-regensburg.de/~brf09510/syntax.html:
    #print key.lower().ljust(14), ":", ('\n'+(20*' ')+'| ').join(value.replace('`','')),'.'
print '''

..  colophon: this doc was generated with "python doc/source/gengrammardoc.py > doc/source/grammar.rst"
'''

