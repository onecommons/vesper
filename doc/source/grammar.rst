
.. :copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
.. :license: Dual licenced under the GPL or Apache2 licences, see LICENSE.

JsonQL grammar
===================


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
     exprlist       : `exprlist` "," `expression`
                    : |`expression`
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
     listconstructitem : "exclude" `arrayindexlist`
                    : |"exclude" `arrayindexlist` "when" `expression`
                    : |"include" `listconstruct`
                    : |`expression`
     constructitem  : `expression` ":" `dictvalue`
                    : |`barecolumnref`
                    : |"[" `barecolumnref` "]"
                    : |"id"
                    : |"exclude" `barecolumnreflist`
                    : |"exclude" `barecolumnreflist` "when" `expression`
                    : |"include" `dictconstruct`
     keywordarg     : `name` "=" `expression`  
     dictvalue      : "[" `construct` "]"
                    : |`construct`
                    : |`expression`
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
     barecolumnref  : `name`
                    : |`qname`
                    : |"*"
                    : |`propstring`
                    : |`qstar`
     sortexp        : `expression`
                    : |`expression` "asc"
                    : |`expression` "desc"
     join           : "{" `expression` "}"
     funccall       : `funcname` "(" `arglist` ")"
     arrayindexlist : `arrayindexlist` "," `arrayindex`
                    : |`arrayindex`
                    : |`empty`
     barecolumnreflist : `barecolumnreflist` "," `barecolumnref`
                    : |`barecolumnref`
                    : |`empty`
     arrayindex     : INT
                    : |"*"
     construct      : `dictconstruct`
                    : |`listconstruct`
     funcname       : `name`
                    : |`qname`
                    : |`propstring`
     columnref      : `label` "." `columnreftrailer`
                    : |`columnreftrailer`
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
