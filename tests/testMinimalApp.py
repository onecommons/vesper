@Action
def testaction(kw, retval):
    query = "{comment:* where(label='%s')}" % kw._name #XXX qnames are broken
    r = kw.__server__.dataStore.query(query)
    result = r['results']
        
    template = '<html><body>%s</body></html>'
    if result:
        return template % result[0]['comment']
    else:
        kw._status = 404
        return template % 'not found!'
                                        
actions = { 'http-request' : [testaction] 
        }

APPLICATION_MODEL = [{ 'id' : 'a_resource', 
                      'label' : 'foo', 
                       'comment' : 'page content.'
                    }]

createApp(actions=actions, APPLICATION_MODEL=APPLICATION_MODEL)