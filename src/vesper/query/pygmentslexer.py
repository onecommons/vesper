# -*- coding: utf-8 -*-
#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
"""
    vesper.query.pygmentslexer
    ~~~~~~~~~~~~~~~~~~~

    Pygments lexer for JSONQL

"""

import re

from pygments.lexer import RegexLexer, include
from pygments.token import \
     Text, Comment, Operator, Keyword, Name, String, Number, Other, Punctuation, Literal

__all__ = ['JsonqlLexer']

class JsonqlLexer(RegexLexer):
    """
    For JSONql.
    """

    name = 'jsonql'
    aliases = ['jsonql','JSONql']
    filenames = ['*.jql', '*.jsonql']
    mimetypes = ['application/x-jsonql']

    flags = re.DOTALL|re.IGNORECASE
    tokens = {
        'commentsandwhitespace': [
            (r'\s+', Text),
            (r'//.*?\n', Comment.Single),
            (r'#.*?\n', Comment.Single),            
            (r'/\*.*?\*/', Comment.Multiline)
        ],
        'root': [
            include('commentsandwhitespace'),
            (r'\:[A-Za-z_$][\w_$]*', Name.Entity), #bindvar
            (r'[{(\[,:]', Punctuation),
            (r'[})\].]', Punctuation),
            (r'(NAMEMAP|MAYBE|WHERE|LIMIT|OFFSET|DEPTH|MERGEALL|'
             r'GROUP|ORDER|ASC|DESC|BY|OMITNULL)\b', Keyword.Reserved),
            (r'(true|false|null)\b', Keyword.Constant),
            #built-in functions
            (r'(sum|count|total|avg|min|max|number|string|bool|'
             r'if|follow|isbnode|isref|'
             r'upper|lower|trim|ltrim|rtrim)\b', Name.Builtin),
            #user-defined function:
            (r'[A-Za-z_$][\w_$]*(?=\()', Name.Function),
            (r'[0-9][0-9]*\.[0-9]+([eE][0-9]+)?[fd]?', Number.Float),
            (r'[0-9]+', Number.Integer),
            (r'"(\\\\|\\"|[^"])*?"', String.Double),
            (r"'(\\\\|\\'|[^'])*?'", String.Single),
            (r'\*', Name.Variable),
            (r'@<(\\\\|\\"|[^"])*?>', Literal), #refstring
            (r'@[^\s<}),\]]+', Literal), #object reference
            (r'<(\\\\|\\"|[^"])*?>', Name.Variable),#propstring
            (r'\?[A-Za-z_$][\w_$]*', Name.Label),
            (r'[+/%=!\-<>]',Operator),
            (r'(NOT|AND|OR|IN)', Operator.Word),
            (r'[A-Za-z_$][\w_$]*', Name.Variable),
        ]
    }
