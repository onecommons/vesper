@Action
def testaction(kw, retval):
    #XXX: query = jql.execute( "[rdfs:comment where(rdfs:label=?_name)]", kw)
    if kw.get('_name') == 'foo':
        query = ['page content.']
    else:
        query = None
    
    template = '<html><body>%s</body></html>'
    if query:
        return template % query[0]
    else:
        kw['_status'] = 404
        return template % 'not found!'
                                        
actions = { 'http-request' : [testaction] 
        }

APPLICATION_MODEL='''<http://rx4rdf.sf.net/test/resource> <http://www.w3.org/2000/01/rdf-schema#label> "foo" .
<http://rx4rdf.sf.net/test/resource> <http://www.w3.org/2000/01/rdf-schema#comment> "page content." .
'''