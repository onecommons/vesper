pJSON (`persistent json`)
*************************

Introduction
============

pJSON is the JSON serialization format for reading and writing data into a Vesper datastore. 

The design goals of the format are:

* concise and familiar: add a minimal set of naming conventions on top of JSON to meet these goals.
* roundtrip-able: no loss of information or introduction of ambiguity when serializing pjson.
* self-describing data-types: there should be no need for a schema definitation to be able read or write the JSON.
* self-describing context: it should be possible to construct pjson document that is guaranteed not to conflict with an independently created document; specifically, allow for the namespacing of identifiers and properties.
* adaptable: to the extent possible without conflicting with the previous goals, allow pre-existing JSON data to be treated as pjson without modification.

Because the Vesper internal data model is based on the RDF data model, pjson can also be treated as a mapping of JSON to RDF, enabling to be used to convert arbitrary JSON to RDF or a user-friendly JSON format for RDF. This mapping is described in [XXX].

`pJSON` by example
==================

Here's a example of a simple pjson "document":

.. code-block:: javascript

  {
    "id" : "1",
    "string_property" : "a string",
    "number_property" : 1.0,
    "array_property" : ["value", 2, null],
    "object_property" : { "a property" : "a nested object" }
  }

This is just an ordinary JSON object with the exception that "id" property has a specific meaning in `pJSON`: it indicates that the object is persistent and uniquely identified by the value of that attribute. Each of the properties of the object will be associated with the corresponding persistent object. Any valid JSON value is valid. Note that the value of "object_property" is a JSON object but because that object doesn't have an "id" property it is treated like any other JSON value. 

The following example illustrates how to reference persistent objects in `pJSON`:

.. code-block:: javascript

  [{
    "id" : "1",
    "string_property" : "a string",
  },
  { 
    "id" : "2",
    "reference_property" : {"$ref" : "2"}
  }]

This example consists of an array of two persistent objects. The ``{"$ref" : "2"}`` in the second one references the first object. `pJSON` also provides a more concise way represent object references -- as strings instead of a reference object. The following example is identical the prior one:

.. code-block:: javascript

  [{
    "id" : "1",
    "string_property" : "value1",
  },
  { 
    "id" : "2",
    "reference_property" : "@2"
  }]

Any value that matches pattern like `@ref` are treated as an object reference. You can declare an different pattern with :ref:`referencepattern` property in the :ref:`namemap`.

`pJSON` doesn't define any datatypes other than what is provided by JSON but it does provide a way declare that value has a user-defined datatype that the datastore should be able to interpret. For example, here is object that has a value with the datetype labeled "date":

.. code-block:: javascript

  {
    "id" : "3",
    "date_property" : {"datatype": "date", "value" : "2010-04-01"}
  }

You can also define patterns for recognizing these datatypes but unlike object references 
there are no default patterns, so you must declare them in a:ref:`namemap`. The following example 
is equivalent to the previous one but uses a pattern for recognizing dates:

.. code-block:: javascript

  {
    "id" : "3",
    "date_property" : "2010-04-01",
    "namemap" : { 
                  "datatypepatterns" : { "date" : "(\\d\\d\\d\\d-\\d\\d-\\d\\d)" }
                }    
  }

The above example introduces the :ref:`namemap` property.
In addition to containing declarations of object reference and datatype patterns like the ones illustrated above, 
you use it to declare :ref:`propertypatterns` which provide a mechanism similar to XML namespaces.
You can also it to rename the reserved pJSON properties to prevent conflicts. The example renames the "id" property:

.. code-block:: javascript

  {
  "namemap" : { "id" : "oid" },
  "oid" : "1",
  "id" : "just another property"
  }
 
Now "oid" identifies the object and the property named "id" is treated like a regular property.

The final property with a pre-defined meaning in pJSON is the :ref:`context`. It provides a way to associate 
metadata about the object it appears in. The value of this property and how datastore intreprets it is user-defined. For example:

.. code-block:: javascript

  {
    "id" : "3",
    "foo" : "bar",
    "context" : "transaction-id:60e6b3c8-e01f-42e7-8cba-482580cda94c"
  }

pJSON reference
===============

.. _ref-pjson-document:

pJSON document
--------------

The following forms of JSON are valid `pJSON`:

1. If the JSON is an array, it is treated as a :ref:`top-level-object-array`.

2. If the JSON is an object and doesn't have a property named "pjson", it is treated as the sole item in a :ref:`top-level-object array`.

3. If the JSON is an object and has a property whose name is equals to *"pjson"*,
the value of that property must equal to *"0.9"* and it must contain a property named *"data"*
whose value must be an array of of objects that is treated as a :ref:`top-level-object-array`.
The object may optionally have :ref:`namemap` or :ref:`context` properties, which are applied to :ref:`top-level-object-array`. Any other properties are ignored. 

For example:

.. code-block:: javascript
  
  {
  "pjson" : "0.9",
  "data" : [ { "id" : "1" } ],
  "namemap" : { }
  }

It is an error if the JSON is not an array or object.

.. _top-level-object-array:

top-level object array
----------------------

The top-level object array contains JSON objects. Unlike nested objects, these objects are always treated as persistent even if the :ref:`pjson-id` is not present. In that case, the behavior is implementation defined; it could assign some autogenerated id or apply a policy to check if the object already exists in the store. 

If an object in a top-level array contains a property named *"pjson"*, it isn't processed as a persistent object. Instead, if the object has :ref:`namemap` or :ref:`context`, properties those properties are processed and applied to subsequent objects in the array. Also, it is an error if the "pjson" property's value is not equal to *"0.9"*. Any other properties in that object are ignored. 

.. _ref-pjson-id:

`id` property
-------------

If a JSON object has a property named "id", that object will correspond to a persistent object 
in the datastore that is uniquely identified by the value of the property and can be referenced by that id elsewhere in the :ref:`pjson-document`. It is implementation-defined how JSON objects without an "id" properties are stored, for example, they could stored as a regular values associated with the property of the closest ancestor (containing) JSON object that has an id, or they could be treated as persistent objects and assigned autogenerated ids.

When serializing an id property, if a :ref:`ref pattern<ref-patterns>` is defined and the id conforms to the pattern, it will be serialized as a reference [1]_. Otherwise the id will be serialized as is and :ref:`escaping<pjson-escaping>` if the result conflicts with the ref pattern [2]_. When parsing an id property, the parse will attempt to apply the ref pattern first. If it doesn't match, the value will parsed as an identifier.

.. _ref-pjson-ref-object:

`$ref` objects
--------------

If an object contains a property named *"$ref"* it is treated as a reference to the object with an :ref:`pjson-id` equal to value of that property. If any :ref:`id-patterns` or :ref:`shared-patterns` are specified, those are applied to the value. A `$ref` object may also optionally contain a :ref:`context` property.

.. _ref-ref-patterns:

reference patterns
------------------

If a :ref:`refpattern-property` is present in the :ref:`namemap`, it will be used to recognize JSON values as references. 
If a match is made on a value, it will treated as a reference to the object with the matching id.
If no :ref:`refpattern-property` is specified the default pattern `@((::)?URIREF)` will be used [3]_.
If any :ref:`idpatterns` or :ref:`sharedpatterns` are specified, those are applied to the reference that was extracted, so the ref pattern should match the result of applying those patterns.

Likewise, when serializing references from the datastore, the ref pattern will be applied after any id patterns
applied, including any :ref:`escaping <pjson-escaping>`, so the reference pattern needs to match 
the results of applying those id patterns, which might not be the same as data store's representation of the id.

.. _pjson-datatype-object:

`datatype` objects
-------------------

If an object contains an property named "datatype" it is treated as value with the specified datatype.
The object must also contain a property named "value", whose value will be the value used. 
The value of the `datatype` property can be either 'json', 'lang:' + *language code*, or a URI reference.
If it is "json", the datatype of the value will be inferred from the value. If it begins with "lang:", it labels the value
(which should be a string) with the given language code (but note that not all data stores will retain this label).
Any other value will be treated as non-JSON datatype whose interpretation is dependent on the data store.
A `datatype` object may also optionally contain a :ref:`context` property.

.. _ref-datatype-patterns:

`datatype patterns`
-------------------

If a :ref:`datatypepatterns-property` is present in the :ref:`namemap`, it will be used to recognize 
which JSON values are a custom datatype. If a match is made on a value, it will labeled with the specified datatype.

.. _ref-parse-pattern:

`parse patterns`
-----------------

A `parse pattern` can be either a string or a JSON object. 
If it is a string, it will be intrepreted as a `match pattern` as defined below.
If it is an JSON object, whose property names are interpreted is a `match pattern`
and whose value is intrepreted as a `replacement pattern`.

.. _ref-matchpattern:

`Match patterns`
~~~~~~~~~~~~~~~~

`Match patterns` may conform to this syntax:

*literal?*'('*regex*')'*literal?*

where *regex* is a string that will be treated as a regular expression and 
*literal* are optional strings [4]_. When processing JSON, any 
value that matches the *regex* will be treated a match.

If specified, the *literals* at the begin or end of the pattern also have to match 
but they are ignored as part of the object reference. Note that the parentheses
around the *regex* are required to delimitate the regex (even if no *literal* 
is specified) but ignored when matching values.

The regular expression syntax follows Javascript's regular expressions (but without the leading 
and trailing "/") except two special values can be included in the regex:
*ABSURI* and *URIREF*. The former will expand into regular expression matching
an absolute URL, the latter expands to regular expression that matches 
any string that looks like a relative URL and matches most strings that don't contain spaces 
or other punctuation characters not allowed in URLs.  
As an example, the default `refs` pattern is ``@((::)?URIREF)``.

If the match pattern does not match this syntax it will treated as a literal prefix 
and the regular expression will default to ".*", i.e. it will match everything after that. 
When matching a value against multiple pattern, patterns with non-emtpy literal prefixes are evaluated first.

`replacement patterns`
~~~~~~~~~~~~~~~~~~~~~~

The `replacement pattern`, if present, is used to tranform the value by replacing any occurences of 
the sequence "@@" in the replace pattern with the match obtained by the regex portion of the `match pattern`.
If a replacement pattern doesn't not contain a "@@", it will be appended at the end.

For example, this :ref:`datatypepatterns`:

``{"(\d\d\d\d-\d\d-\d\d)" : "@@T00:00:00Z"}``

will like match patterns like "2010-04-01" and pass "2010-04-01T00:00:00Z" to the datastore.

Use of the defaults for the match and replace patterns enables :ref:`propertypattern` that look very much like xml namespace declarations, for example, a :ref:`propertypattern` like this:  

.. code-block:: javascript

 { "html:" : "http://www.w3.org/1999/xhtml",
   "" : "http://example.org/myschema#"
 }

is equivalent to:

.. code-block:: javascript

 { "html:(.*)" : "http://www.w3.org/1999/xhtml@@",
  "(.*)" : "http://example.org/myschema#@@"
 }

and behaves in a similar manner to XML namespace decarations for a "html" prefix and the default namespace.

.. _ref-namemap:

`namemap` property
------------------

A `namemap` may contain the following properties: :ref:`refpattern`, :ref:`idpatterns`, :ref:`datatypepatterns`, :ref:`propertypatterns`, :ref:`sharedpatterns`, and :ref:`exclude`.

It may also contain any of the reserved `pjson` property names
(i.e. `id`, `$ref`, `namemap`, `datatype` and `context`)
If present, the value of the property is used as the reserved name.

The `namemap` will be applied to all properties and embedded objects 
contained within the JSON object that contains the property. 
If an embedded object has a "namemap" property, that namemap is merged with the parent namemap
by having any properties defined in child namemap add or replace parent namemap's property.

.. _ref-refpattern-property:

`refpattern` property
~~~~~~~~~~~~~~~~~~~~~

The value of the `refpattern` property is a :ref:`parse-pattern` or an empty string. If it is an empty string, reference pattern matching will be disabled. If the parse pattern is a JSON object, it must contain only one :ref:`matchpattern` as a property.

If a :ref:`idpatterns` are specified when parsing pJSON, it will be applied to the result the ref pattern. When serializing reference to pJSON, any specified :ref:`idpatterns` are applied before applying the refpattern to the object reference.

When serializing to pJSON, any object references that doesn't match the `refpattern` 
pattern will be serialized as :ref:`pjson-ref-object`.
Likewise, any values that is not an object reference but *does* match the `refs` 
pattern will be serialized as :ref:`pjson-datatype-object`.

.. _ref-idpatterns:

`idpatterns` property
~~~~~~~~~~~~~~~~~~~~~

XXX `idpatterns` allows persistent ids 

.. code-block:: javascript

  {
  "namemap" : {
   "idpatterns" : { "": "http://example.com/datastore#instance#" }
  },
  "id" : "1", 
  "a_ref" : "@2"
  }

http://example.com/datastore#instance#1 and http://example.com/datastore#instance#2

.. _ref-propertypatterns:

`propertypatterns`
~~~~~~~~~~~~~~~~~~~

XXX Provides a mechanism similar to XML namespaces.

.. _ref-sharedpatterns:

`sharedpatterns`
~~~~~~~~~~~~~~~~~~

XXX Applies to ids, references, and properties. It doesn't apply to datatype patterns.

.. code-block:: javascript

  {
  "namemap" : {
   "sharedpatterns" : { "": "http://myschema.com#",
      "rdf:": "http://w3c.org/RDF#",
      "(type|List)" : "http://w3c.org/RDF#",
    },
    "refpattern" : "<(ABSURI)>"
  },
  "prop1" : "@foo",
  "rdf:type" : "@rdf:List"
  }

.. _datatypepatterns-property:

`datatypepatterns`
~~~~~~~~~~~~~~~~~~

The value `datatypepatterns` is an object whose properties' names declare a datatype. The value of each property can either be a :ref:`parse-pattern` or an array of :ref:`parse-patterns`. The name of the property has the same meaning as the value of the `datatype` property of a :ref:`pjson-datatype-object`.

`exclude`
~~~~~~~~~

The value of the *"exclude"* property is a list of property names. If present, any property whose name matches one this list will be ignore when parsing the pJSON.

`context` property
------------------

The presence of a `context` property will assign that `context` to all 
properties and descendent objects contained within the JSON object.
The `context` property can also appear inside a `datatype` or `$ref` object.
In that case, the context will be applied to only that value.

.. _ref-pjson-escaping:

escaping properties and identifiers
-----------------------------------

Property names and id identifiers that begin with "::" (two colons) will have those leading colons removed when passed to the underlying datastore 
but remain present when processing :ref:`parse-patterns`. This provides an escape mechanism for representing reserved property names and avoiding false positives for :ref:`parse-patterns`. Some examples:

.. code-block:: javascript

  {
    "id" : "1",
    "::id" : "just another property",
    "::::doublecolonprop" : "the actual name of this property is ::doublecolonprop"
  }

This object has property named "id" which is escaped as "::id" to avoid conflicting with the reserved name "id". It also has a property named "::doublecolonprop", which must be written as "::::doublecolonprop" because the leading "::" will be removed. 

.. code-block:: javascript

  {
    "id" : "::foo",
    "a reference" : "@::bar",
    'namemap' : { "idpatterns" : { '': 'http://example.com/instanceA#' } }
  }

This object has an id whose value is "foo" and references an object with an id of "bar". If those values weren't proceeded with "::", the :ref:`idpatterns` would have applied and those ids would have been "http://example.com/instanceA#foo" and "http://example.com/instanceA#bar".

When serializing data into `pJSON` the serializer should automatically escape any property names that match a reserved `pJSON` property name or 
match any specified :ref:`propertypatterns` or :ref:`sharedpatterns`. Likewise it should escape any identifiers or object references which match 
any specified :ref:`idpatterns` or :ref:`sharedpatterns`.

..   .. automodule:: vesper.pjson
     :members:

.. [1] Design note: Ids are serialized using the :ref:`ref-patterns` to ensure that, when serializing as JSON objects, the value of the id property will equal references to that id. For example, `foo.id == bar.foo` when `foo` is a property whose value is a reference to `foo.id`.
.. [2] For example, given the default :ref:`ref pattern<ref-patterns>`, an id named *@1* will be serialized as *::@1* because *@@1* would not be recognized as a reference (because it doesn't match the ref pattern), but *@1* would mistakingly be parsed into a reference to *1*, so it needs to be escaped as *::@1*. 
.. [3] Design note: This pattern was chosen because it always reversible 
 -- so that the same `namemap` can be used when serializing `pjson` to generate 
 references from the object ids.
.. [4] Design note: default pattern of "@name" was chosen because it is concise,
 because the "@" intuitively implies a notion of referencing, and because the pattern is unusual enough
 that false positives from JSON created by non-pjson aware sources would be rare.