#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
@Action
def updateAction(kw, retval):
    '''
    Run this action every request but should only add content the first time
    '''
    pjson = [{ 'id' : 'a_resource',
       'label' : 'foo',
      'comment' : 'page content.'
    }]
    kw['__server__'].dataStore.update(pjson)
    return retval

@Action
def queryAction(kw, retval):
    query = "{comment:* where(label='%s')}" % kw['_name'] #XXX qnames are broken         
    r = kw['__server__'].dataStore.query(query)
    result = r['results']
    #print result
        
    template = '<html><body>%s</body></html>'
    if result:
        return template % result[0]['comment']
    else:
        kw['_status'] = 404
        return template % 'not found!'

@Action 
def recordUpdates(kw, retval):
    kw['__server__'].updateResults = kw

@Action
def testLoadModelHook(kw, retVal):
    kw.__server__.loadModelHookCalled = True
    return retVal
             
actions = { 'http-request' : [updateAction, queryAction],
'after-commit' : [recordUpdates],
'load-model':[testLoadModelHook]
}

createApp(actions=actions, saveHistory='split')
