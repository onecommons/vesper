#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
@Action
def testaction(kw, retval):
    query = "{comment where(label='%s')}" % kw._name #XXX qnames are broken
    result = kw.__server__.defaultStore.query(query)
        
    template = '<html><body>%s</body></html>'
    if result:
        return template % result[0]['comment']
    else:
        kw._status = 404
        return template % 'not found!'
                                        
actions = { 'http-request' : [testaction] 
        }

application_model = [{ 'id' : 'a_resource', 
                      'label' : 'foo', 
                       'comment' : 'page content.'
                    }]

createApp(actions=actions, application_model=application_model)