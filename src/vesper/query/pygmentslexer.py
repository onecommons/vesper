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
     Text, Comment, Operator, Keyword, Name, String, Number, Other, Punctuation

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
            (r'[{(\[,:]', Punctuation),
            (r'[})\].]', Punctuation),
            (r'(NAMEMAP|MAYBE|WHERE|LIMIT|OFFSET|DEPTH|MERGEALL|'
             r'GROUPBY|ORDERBY|ASC|DESC|OMITNULL)\b', Keyword.Reserved),
            (r'(true|false|null)\b', Keyword.Constant),
            (r'(sum|count|total|avg)\b', Name.Builtin),            
            (r'[0-9][0-9]*\.[0-9]+([eE][0-9]+)?[fd]?', Number.Float),
            (r'[0-9]+', Number.Integer),
            (r'"(\\\\|\\"|[^"])*"', String.Double),
            (r"'(\\\\|\\'|[^'])*'", String.Single),
            (r'\*', Name.Variable),
            (r'<(\\\\|\\"|[^"])*>', Name.Variable),#propstring
            (r'\?[A-Za-z_$][\w_$]*', Name.Label),
            (r'\:[A-Za-z_$][\w_$]*', Name.Entity), #bindvar
            (r'[+/%=!\-<>]',Operator),
            (r'(NOT|AND|OR|IN|IS)', Operator.Word),
            (r'[A-Za-z_$][\w_$]*', Name.Variable),
        ]
    }
