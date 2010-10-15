#from vesper.web.route import Route, servetemplate
from vesper.app import createApp

app = createApp(__name__, 'vesper.web.admin' 
  ,static_path=['static'] #put static file in "static" sub-directory
  ,template_path=['templates'] #put mako template file in "templates" sub-directory
  ,storage_path='helloworld.json'
  ,save_history='split' #turn on separate version store
)

if __name__ == '__main__':
    from vesper.web.baseapp import parseCmdLine
    parseCmdLine()
    app.run()
