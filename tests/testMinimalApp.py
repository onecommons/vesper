@Action
def testaction(kw, retval):
    query = "{comment:* where(label='%s')}" % kw['_name'] #XXX qnames are broken     
    s = kw['__server__'].domStore.model
    result = list(kw['__server__'].domStore.query(query))
    #print result
        
    template = '<html><body>%s</body></html>'
    if result:
        return template % result[0]['comment']
    else:
        kw['_status'] = 404
        return template % 'not found!'
                                        
actions = { 'http-request' : [testaction] 
        }

APPLICATION_MODEL = [{ 'id' : 'a_resource', 
                      'label' : 'foo', 
                       'comment' : 'page content.'
                    }]
