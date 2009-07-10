class TestAction(SimpleAction):
    def match(self, kw):
        #XXX: query = jql.execute( "[rdfs:comment where(rdfs:label=?_name)]", kw)
        if kw.get('_name') == 'foo':
            query = ['page content.']
        else:
            query = None
        
        if query:
            return query[0]
        else:
            return 'not found!'
            
    def runAction(self, result, kw, contextNode, retVal):
        return '<html><body>'+result+'</body></html>'
                            
actions = { 'http-request' : [
                TestAction()
            ] 
        }

APPLICATION_MODEL='''<http://rx4rdf.sf.net/test/resource> <http://www.w3.org/2000/01/rdf-schema#label> "foo" .
<http://rx4rdf.sf.net/test/resource> <http://www.w3.org/2000/01/rdf-schema#comment> "page content." .
'''