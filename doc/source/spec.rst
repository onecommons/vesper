
= constructing a JSON object, array or value =

.. productionlist::
 query: `constructobject` | `constructarray` | `constructvalue`

 constructobject : "{" [`label`]
                 :    (`objectitem` | `objectpair` | "*" [","])+
                 :    [`query_criteria`] 
                 :  "}"

 constructarray  : "[" [`label`]
                 :    (`arrayitem` [","])+
                 :    [`query_criteria`] 
                 : "]"

 constructvalue  : "(" 
                 :    `expression` 
                 :    [`query_criteria`] 
                 : ")"

 arrayitem : `expression` | "*" 
 
 objectitem : `propertyname` | "*"
 
 objectpair : `expression` ":" (`expression` | `constructarray` | `constructobject`)

 propertyname : NAME | "<" CHAR+ ">"
  
 query_criteria : ["WHERE(" `expression` ")"]
                : ["GROUPBY(" (`expression`[","])+ ")"]
                : ["ORDERBY(" (`expression` ["ASC"|"DESC"][","])+ ")"]
                : ["LIMIT" number]
                : ["OFFSET" number]
                : ["DEPTH" number]

.. productionlist::
 expression: `expression` "and" `expression`
             | `expression` "or" `expression`
             | "maybe" `expression`
             | "not" `expression`
             | `expression` `operator` `expression`
             | `join`
             | `atom`
             | "(" `expression` ")"
 
 operator : "+"|"-"|"*"|"/"|"%"|"="|"=="|"<"|"<="|">"|"=>"|["not"] "in"  

 join: "{" `expression` "}"

 atom : `label` | `propertyreference` | `constant` | `functioncall` | `bindvar`

 label : "?"NAME
 
 bindvar : ":"NAME

 propertyreference:: [`label`"."]`propertyname`["."`propertyname`]+

 functioncall:: NAME([`expression`[","]]+ [NAME"="`expression`[","]]+)

 constant : STRING | NUMBER | "true" | "false" | "null"
 
 comments : "#" CHAR* <end-of-line> 
          : | "//" CHAR* <end-of-line> 
          : | "/*" CHAR* "*/"

== * ==

* will expand all property defined on the object. 

But if the specifies contains explicity property name, it won't override the 

== dictionary items == 

expression : expression
[, expression : expression]*

== property lists == 

propertyname[,propertyname+]

this is shortcut for 

= constructing lists = 

= abbr and qname =

 id : replace
 type :
 prefix : replace
 default : prepend
  
 {
 foo 
 where (foo.type = 'blah' and foo = ?bar and {id=?bar and type='blah'} ) 
 }