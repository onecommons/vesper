pjson (`persistent json`)
=========================

pjson is the JSON serialization format for reading and writing data into a Vesper datastore. 

The design goals of the format are:
 * concise and familiar: add the minimal set of naming conventions to JSON 
 * roundtrip-able: no loss of information or introduction of ambiguity 
    when serializing pjson.
 * self-describing data-types: there should be no need for a schema to read or write the JSON.
 * self-describing contexts: XML namespace support, attribution and versioning
 * adaptable: to extend possible without conflicting with the previous goals, allow pre-existing JSON data to be treated as pjson without modification.

Because the Vesper internal data model is based on the RDF data model, pjson can also be treated as a mapping of JSON to RDF, enabling to be used to convert arbitrary JSON to RDF or a user-friendly JSON format for RDF. This mapping is described in [XXX].

basic functionality
-------------------

`pjson` defines a set of naming conventions to designed to make it easy to persist JSON into and out of a datastore. Specifically, to recognize identifiers of objects, references to objects, and custom data types.

`pjson` is a set of property names and value patterns designed to make it easy 
to persist JSON. Its basic elements can be summarized as:

  * `id` property: Indicates the id (or key) of the JSON object it appears in.
  * A JSON object like `{"$ref" : "ref"}` or a value that matches pattern like `@ref`. 
     Indicates that "ref" is an object reference
  * A JSON object like `{"datatype": "datatype_name", value : "value" }`. 
     Parses the "value" is a representation of a non-JSON datatype named "datatype_name"

For example:

.. code-block:: javascript

  {
    "id" : "1",
    "property_1" : {"$ref" : "2"},
    "property_2" : "@2",
    "property_3" : {"datatype": "date", "value" : "2010-04-01"},
    "::id" : "just another property"
  }

This object has an id set to "1" which as two properties which both reference the same object identified by "2" and a 3rd property whose value is a "date" (pjson doesn't define any datatype'). It has a property name "id" which is escaped as "::id" to avoid conflicting with the reserved name "id".

namemap: defining alternative spellings
---------------------------------------

`namemap`  
   The value of the `namemap` property must be a `pjson` header object as 
   described above. That header will be applied to all properties and descendent objects 
   contained within the JSON object that contains the `namemap` property.

`pjson` also defines a header object that can be used to specify alternative 
names or patterns for those predefined.
These names and patterns can be changed by supplying a `namemap`. For example: 

.. code-block:: javascript

  {
  "namemap" : { "id" : "oid", 
                "refs" : "<([0-9]+)>",
                "datatypes" : { "date" : "(\\d\\d\\d\\d-\\d\\d-\\d\d\\)" }
              },
  "oid" : "1"
  "property_1" : "<2>",
  "property_2" : "<2>",
  "property_3" : "2010-04-01",
  "id" : "just another property"
  }

The header object must contain a property is name is *"pjson"* and value is *"0.9"*.

It may also contain any of the reserved `pjson` property names 
(i.e. `id`, `$ref`, `namemap`, `datatype` and `context`)
If present the value of the property is used as the reserved name.

`ref-pattern`
~~~~~~~~~~~~~

The value of `refs` can be either a string or a JSON object. If it is a string, 
it must either be empty or be a valid `match pattern`.

If `refs` is an empty string, pattern matching will be disabled.

If `refs` is an JSON object it must contain only one property. 
The property name will be treated as a ref pattern as described above,
and the property value will be used to generate the object reference.
The sequence "@@" will be replaced with the value of the regex match.
For example:

``{"<([-_a-zA-Z0-9])>" : "http://example.com/@@"}``

will treat values with like "<id1>" as an object reference with the value
"http://example.com/id1".

When serializing to JSON, any object reference that doesn't match the `refs` 
pattern will be serialized as an explicit ref object.
Likewise, any value that is not an object reference but *does* match the `refs` 
pattern will be serialized as an explicit data value.

`id-patterns`
~~~~~~~~~~~~~

`property-patterns`
~~~~~~~~~~~~~~~~~~~

.. code-block:: javascript

  {
  "namemap" : {
   "default-patterns" : { "": "http://myschema.com#",
      "rdf:": "http://w3c.org/RDF#",
      "(type|List)" : "http://w3c.org/RDF#",
    },
    "ref-pattern" : "<(ABSURI)>"
  },
  "prop1" : "@foo",
  "rdf:type" : "@rdf:List"
  }

'default-patterns`
~~~~~~~~~~~~~~~~~~

Applies to ids, references, and properties. It doesn't apply to datatype patterns.

'datatype-patterns`
~~~~~~~~~~~~~~~~~~~

``{ datatype : pattern }``
or
``{ datatype : [patterns] }``

`exclude`
~~~~~~~~~

Exclude properties whose name matches

`match patterns`
-----------------

Match patterns must conform to this syntax:
  
*literal?*'('*regex*')'*literal?*

where *regex* is a string that will be treated as a regular expression and 
*literal* are optional strings [1]_. When parsing JSON will treat any property
value that matches the *regex* as an object reference.

The *literal*s at the begin or end of the pattern also have to match if specified
but they are ignored as part of the object reference. Note that the parentheses
around the *regex* are required to delimitate the regex (even if no *literal* 
is specified) but ignored when pattern matching the value.

The regex follows the Javascript regular expressions (but without the leading 
and trailing "/") except two special values can be included in the regex:
*ABSURI* and *URIREF*. The former will expand into regular expression matching
an absolute URL, the latter expands to regular expression that matches 
relative URLs, which includes most strings that don't contain spaces or most
punctuation characters.  

The Ref pattern will be applied after any id pattern are 
applied, including the default "::" id pattern, so the pattern needs to match 
the results of the id pattern, which might not be the same as data store's 
representation of the id.

As an example, the default `refs` pattern is ``@((::)?URIREF)``.

`context` property
------------------

The presence of a `context` property will assign that `context` to all 
properties and descendent objects contained within the JSON object.
The `context` property can also appear inside a `datatype` or `$ref` object.
In that case, the context will be applied to only that value.

Additional Semantics
--------------------

 * A JSON object without an `id` will be associated with the closest ancestor 
   (containing) JSON object that has an id. 
   If the object is at the top level it will be assigned an anonymous id.
 * `datatype` property can be either 'json', 'lang:' + *language code*, or a URI reference.
    If "json", the value is treated as is. If it begins with "lang:", it labels the value
    (which should be a string) with the given language code. 
    Any other value will be treated as non-JSON datatype whose interpretation 
    is dependent on the data store.

.. automodule:: vesper.pjson
  :members:

.. [1] Design note: This pattern was chosen because it always reversible 
 -- so that the same `namemap` can be used when serializing `pjson` to generate 
 references from the object ids.
.. [2] Design note: default pattern of "@name" was chosen because its iss concise,
 the "@" intuitively implies a "reference", and because the pattern is unusal enough
 as false positives conflicting with JSON would be rare.