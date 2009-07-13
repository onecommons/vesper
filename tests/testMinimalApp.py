@Action
def testaction(kw, retval):
    import jql
    #query = "[rdfs:comment where(rdfs:label='%s')]" % kw['_name'] #XXX bug in parser 
    #query = "{rdfs:comment, where(rdfs:label='%s')}" % kw['_name'] #XXX this bad syntax but need better error reporting
    query = "{rdfs:comment:*, where(rdfs:label='%s')}" % kw['_name']
    res = jql.runQuery(query, kw['__server__'].domStore.model)
    #print list(res) #XXX fails
    
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

APPLICATION_MODEL = [{ 'id' : 'a_resource', 
                      'rdfs:label' : 'foo', 
                       'rdfs:comment' : 'page content'
                    }]