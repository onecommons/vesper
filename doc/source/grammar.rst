
.. :copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
.. :license: Dual licenced under the GPL or Apache2 licences, see LICENSE.

Formal Grammar
===================

This grammar file is machine generated.

.. productionlist::
     root           : `topconstruct`
     topconstruct   : `dictconstruct`
                    : |`listconstruct`
                    : |`valueconstruct`
     dictconstruct  : "{" `label` "," `dictconstructlist` "}"
                    : |"{" `label` `dictconstructlist` "}"
                    : |"{" `dictconstructlist` "}"
     valueconstruct : "(" `expression` `constructoplist` ")"
                    : |"(" `expression` "," `constructoplist` ")"
                    : |"(" `expression` "," `constructoplist` "," ")"
     listconstruct  : "[" `label` "," `listconstructlist` "]"
                    : |"[" `label` `listconstructlist` "]"
                    : |"[" `listconstructlist` "]"
     constructoplist : `constructoplist` "," `constructop`
                    : |`constructop`
                    : |`empty`
                    : |`constructoplist` `constructop`
     dictconstructlist : `constructitemlist` `constructoplist`
                    : |`constructitemlist` "," `constructoplist`
                    : |`constructitemlist` "," `constructoplist` ","
     listconstructlist : `listconstructitemlist` `constructoplist`
                    : |`listconstructitemlist` "," `constructoplist`
                    : |`listconstructitemlist` "," `constructoplist` ","
     constructop    : "where" "(" `expression` ")"
                    : |"groupby" "(" `arglist` ")"
                    : |"limit" INT
                    : |"offset" INT
                    : |"depth" INT
                    : |"orderby" "(" `sortexplist` ")"
                    : |"mergeall"
                    : |"namemap" "=" `jsondict`
     listconstructitemlist : `listconstructitemlist` "," `listconstructitem`
                    : |`listconstructitem`
                    : |`empty`
     expression     : `atom`
                    : |`expression` "+" `expression`
                    : |`expression` "-" `expression`
                    : |`expression` "*" `expression`
                    : |`expression` "/" `expression`
                    : |`expression` "%" `expression`
                    : |`expression` "<" `expression`
                    : |`expression` "<=" `expression`
                    : |`expression` ">" `expression`
                    : |`expression` ">=" `expression`
                    : |`expression` "=" `expression`
                    : |`expression` "!=" `expression`
                    : |`expression` "in" `expression`
                    : |`expression` "and" `expression`
                    : |`expression` "or" `expression`
                    : |"(" `expression` ")"
                    : |`expression` "in" "(" `exprlist` ")"
                    : |`expression` "is" "null"
                    : |`expression` "is" "not" "null"
                    : |"maybe" `expression`
                    : |`expression` "not" "in" `expression`
                    : |`expression` "not" "in" "(" `exprlist` ")"
                    : |"not" `expression`
                    : |"-" `expression` 
                    : |"+" `expression` 
     jsondict       : "{" `jsondictlist` "}"
     arglist        : `arglist` "," `expression`
                    : |`arglist` "," `keywordarg`
                    : |`keywordarg`
                    : |`expression`
                    : |`empty`
     constructitemlist : `constructitemlist` "," `constructitem`
                    : |`constructitem`
                    : |`empty`
     sortexplist    : `sortexplist` "," `sortexp`
                    : |`sortexp`
                    : |`empty`
     listconstructitem : `expression`
     exprlist       : `exprlist` "," `expression`
                    : |`expression`
     keywordarg     : `name` "=" `expression`  
     atom           : `columnref`
                    : |`funccall`
                    : |`constant`
                    : |`join`
                    : |`bindvar`
                    : |"id"
                    : |`label`
     constant       : INT
                    : |FLOAT
                    : |`string`
                    : |"null"
                    : |"true"
                    : |"false"
     jsondictlist   : `jsondictlist` "," `jsondictitem`
                    : |`jsondictitem`
                    : |`empty`
     constructitem  : `expression` ":" `dictvalue`
                    : |"omitnull" `expression` ":" `dictvalue`
                    : |`barecolumnref`
                    : |"omitnull" `barecolumnref`
                    : |"maybe" `barecolumnref`
                    : |"[" `barecolumnref` "]"
                    : |"[" "omitnull" `barecolumnref` "]"
                    : |"[" "maybe" `barecolumnref` "]"
                    : |"id"
     barecolumnref  : `name`
                    : |"*"
                    : |`propstring`
     sortexp        : `expression`
                    : |`expression` "asc"
                    : |`expression` "desc"
     join           : "{" `expression` "}"
                    : |"{" `label` "," `expression` "}"
                    : |"{" `label` `expression` "}"
     funccall       : `funcname` "(" `arglist` ")"
     columnref      : `label` "." `columnreftrailer`
                    : |`columnreftrailer`
     jsondictitem   : `string` ":" `string`
                    : |`string` ":" `jsondict`
                    : |`name` ":" `string`
                    : |`name` ":" `jsondict`
     funcname       : `name`
                    : |`propstring`
     dictvalue      : `expression`
                    : |"[" `expression` "]"
                    : |`nestedconstruct`
                    : |"[" `nestedconstruct` "]"
     nestedconstruct : `dictconstruct`
                    : |`listconstruct`
     columnreftrailer : `barecolumnref`
                    : |`columnreftrailer` "." `barecolumnref`
     qstar          : `name` ":*"
     bindvar        : "@" `name`
     string         : """ jsonchars* """
                    : |"'" jsonchars* "'"
     qname          : `name` ":" `name`
     label          : "?" `name`
     propstring     : "<" jsonchars+ ">"
     name           : [A-Za-z_$][A-Za-z0-9_$]*


..  colophon: this doc was generated with "python doc/source/gengrammardoc.py > doc/source/grammar.rst"

