#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
import os
from vesper.web.route import Route, gensequence

@Action
def fromdatastore(kw, retval):
    from vesper import query as jql
    query = "{comment where(label='%s')}" % kw['_name']
    result = list(jql.runQuery(query, kw['__server__'].defaultStore.model))
        
    template = '<html><body>%s</body></html>'
    if result:
        return template % result[0]['comment']
    return retval

@Route('/static/{file:.+}')
def servefile(kw, retval):
    path = kw['urlvars']['file']
    if os.path.exists(path):
        return file(path)
    return retval

actions = { 'http-request' : lambda kw: gensequence(kw, default=fromdatastore)
        }

application_model = [{ 'id' : 'a_resource', 
                      'label' : 'foo', 
                       'comment' : 'page content.'
                    }]
                    
createApp(actions=actions, application_model=application_model)
