configuration variables 
-----------------------

datastore configuration variables 
=================================

.. confval:: dataStoreFactory

  The class or factory function the Raccoon will call to instantiate the application's primary data store
  It is passed as keyword arguments the dictionary of the variables contained in the config file
  note that this is a callable object which may need to be imported into the config file

  Default: ``vesper.DataStore.BasicDataStore``

.. confval:: model_uri

    The base URI reference to be used when creating RDF resources    
    
    Default: 'http://' + socket.getfqdn() + '/'

    Example: ``model_uri='http://example.com/'``

.. confval:: STORAGE_PATH

    The location of the RDF model. Usually a file path but the appropriate value depends on 'modelFactory'
    default is '' 
    STORAGE_PATH = 'mywebsite.nt'

.. confval:: transactionLog
 
    The path of the transactionLog. The transactionLog records in NTriples format a log 
    of the statements added and removed from the model along with comments on when and by whom.
    Note: the default file store uses this format so there is not much reason to use this if you are using the default
    
    default is '' (no transactionLog)
    
    ``transactionLog='/logs/auditTrail.nt'``

.. confval:: STORAGE_TEMPLATE

    A string containing NTriples that is used when 
    the file specified by STORAGE_PATH is not found
    
    STORAGE_TEMPLATE='''
    _:itemdispositionhandlertemplate <http://rx4rdf.sf.net/ns/wiki#name> "item-disposition-handler-template" .
    _:itemdispositionhandlertemplate <http://rx4rdf.sf.net/ns/wiki#revisions> _:itemdispositionhandlertemplate1List .
    _:itemdispositionhandlertemplate <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://rx4rdf.sf.net/ns/archive#NamedContent> .
    '''
.. confval:: APPLICATION_MODEL 

    A string containing NTriples that are added to the RDF model
    but are read-only and not saved to disc. Use for structural components such as the schema.
    
    APPLICATION_MODEL='''<http://rx4rdf.sf.net/ns/wiki#item-format-zml'> <http://www.w3.org/2000/01/rdf-schema#label> "ZML" .'''

.. confval:: modelFactory

    The class or factory function used by RxPathDomStore to load or create a new RDF document or database
    note that this is a callable object which may need to be imported into the config file
    
    default is RxPath.IncrementalNTriplesFileModel
    
    modelFactory=RxPath.RedlandHashBdbModel

.. confval:: VERSION_STORAGE_PATH

    The location of a separate RDF model for storing the history of changes to the database.
    Usually a file path but the appropriate value depends on 'versionModelFactory'
    
    default is '' (history not stored separately)
    
    VERSION_STORAGE_PATH = 'mywebsite.history.nt'

.. confval:: versionModelFactory

    The class or factory function used by RxPathDomStore to load or create the version history RDF database
    #note that this is a callable object which may need to be imported into the config file
    
    default is whatever 'modelFactory' is set to
    
    ``versionModelFactory=RxPath.RedlandHashBdbModel``

.. confval:: useFileLock 

    If True `vesper.app` will use interprocess file lock when committing 
    a transaction. Alternately useFileLock can be a reference to a class or factory
    function that conforms to the glock.LockFile interface.

    Default is False
    
    ``useFileLock=True #enable``

.. confval:: saveHistory 

    Default: ``saveHistory = False``

.. confval:: storageTemplateOptions
 
    Default: ``storageTemplateOptions=None``

.. confval:: modelOptions 

    Default: ``modelOptions=None``

.. confval:: CHANGESET_HOOK 

    Default: ``CHANGESET_HOOK=None``

.. confval:: trunkId 

    Default: ``trunkId = '0A'``

.. confval:: branchId 

    Default: ``branchId = None``                  

web configuration variables 
=================================

EXEC_CMD_AND_EXIT, firepython_enabled
httpserver , 
.. confval:: PORT 

    Default: ``PORT=8000``

.. confval:: logconfig 

   A string that is either a log configuration or apath to a log configuration file

   Default: ``logconfig=None``

.. confval:: httpserver 

  A class that WSGI server

  Default: ``httpserver=wsgiref.simple_server``

.. confval:: STORAGE_URL 

  A string that is either a log configuration or apath to a log configuration file

  Default: ``STORAGE_URL='mem:``

.. confval:: EXEC_CMD_AND_EXIT 

  A string that is either a log configuration or apath to a log configuration file

.. confval:: wsgi_middleware 

   A string that is either a log configuration or apath to a log configuration file

   Default: ``wsgi_middleware=None``
   
   Example: ``import firepython.middleware; wsgi_middleware = firepython.middleware.FirePythonWSGI``

.. confval:: RECORD_REQUESTS 

  Any HTTP requests made are saved to a file. They can be played-back using the ``DEBUG_FILENAME``
  option.

.. confval:: DEBUG_FILENAME 

   If specified, the given file containing a history of requests recorded by ``RECORD_REQUESTS``
   is played back before starting the server.

.. confval:: static_path

    A string or list specifying the directories that will be searched when resolving static URLs

    Default: the current working directory of the process running the app

    Example: ``static_path = 'static'``

.. confval:: template_path

    A string or list specifying the directories that will be searched when resolving static URLs

    Default: the current working directory of the process running the app

    Example: ``template_path = 'templates'``
  
.. confval:: defaultPageName

    The name of the page to be invoke if the request URL doesn't include a path 
    e.g. http://www.example.org/ is equivalent to http://www.example.org/index 
    
    default is: 'index.html'
    
    `defaultPageName='home.html'`

.. confval:: DEFAULT_MIME_TYPE

    The MIME type sent on any request that doesn't set its own mimetype 
    and Raccoon can't guess its MIME type
    default is '' (not set)
    DEFAULT_MIME_TYPE='text/plain'

.. confval:: MODEL_RESOURCE_URI

    The resource that represents the model this instance of the application is running
    it can be used to assertions about the model itself, e.g its location or which application created it
    default is the value of BASE_MODEL_URI
    MODEL_RESOURCE_URI = 'http://example.org/rhizomeapp/2/20/2004'

.. confval:: defaultExpiresIn

    What to do about Expires HTTP response header if it 
    hasn't already set by the application. If it's value is 0 or None the header 
    will not be sent, otherwise the value is the number of seconds in the future 
    that responses should expire. To indicate that they already expired set it to -1;
    to indicate that they never expires set it to 31536000 (1 year).
    default is 3600 (1 hour)
    defaultExpiresIn = 0 #disable setting the Expires header by default

.. confval:: useEtags 

    If True, If-None-Match request headers are honors and an etag based 
    on a MD5 hash of the response content will be set with every response
    
    default is True
    useEtags = False #disable

advanced configuration variables 
================================

These setting variables are only necessary when developing a new Raccoon application

.. confval:: appName

  A short name for this application, must be unique within the current ``vesper.app`` process

  Default: `"root"
  `
  Example: ``appName = 'root'``

.. confval:: cmd_usage

      A string used to display the command-line usage help::
      
         cmd_usage = '''--import [dir] [--recurse] [--format format] [--disposition disposition]
                --export dir [--static]'''

.. confval:: actions

      A dictionary that is the heart of an application running on Raccoon 
      The key is the name of the trigger and the value is list of Actions that are invoked in that order
      Raccoon currently uses these triggers:
       * 'http-request' is invoked by RequestProcessor.handleRequest (for http requests) and by the 'site:' URL resolver
       * 'load-model' is invoked after a model is loaded
       * 'run-cmds' is invoked on start-up to handle command line arguements
       * 'before-add' and 'before-remove' is invoked every time a statement is added or removed
       * 'before-new' is invoked when a new resource is added
       * 'before-prepare' is invoked at the end of a transaction but trigger still has a chance to modify it
       * 'before-commit' is invoked when transaction frozen and about to be committed, one last chance to abort it
       * 'after-commit' is invoked after a transaction is completed successfully 
       * triggerName + '-error' is invoked when an exception is raised while processing a trigger
      see Action class for more info::
         
           actions = { 'http-request' : [Action(['.//myNs:contents/myNs:ContentTransform/myNs:transformed-by/*',], 
                                                __server__.processContents, matchFirst = False, forEachNode = True)],
                  'run-cmds' : [ Action(["$import", '$i'], lambda result, kw, contextNode, retVal, rhizome=rhizome: 
                                      rhizome.doImport(result[0], **kw)),
                                 Action(['$export', '$e'], lambda result, kw, contextNode, retVal, rhizome=rhizome: 
                                      rhizome.doExport(result[0], **kw)),
                              ],
                  'load-model' : [ FunctorAction(rhizome.initIndex) ],
                }

.. confval:: DEFAULT_TRIGGER 

      Used by Requestor objects and the "site:" URL resolver as the trigger to use to invoke a request
      default is 'http-request'
      DEFAULT_TRIGGER='http-request'

.. confval:: globalRequestVars

      A list of request metadata variables that should be preserved 
      when invoking callActions() (e.g. to invoke templates or an error handler)
      default is [] (but `vesper.app`  will always adds the following: 
      '_name', '_noErrorHandling', '__current-transaction', and '__readOnly')

      globalRequestVars = [ '__account', '_static'] 

.. confval:: getPrincipleFunc

      A function that is called to retrieve the 
      application-specific Principal (in the security sense) object 
      for the current request context.
      It takes one argument that is the dictionary of metadata for the current request
      default: lambda kw: '' 
      getPrincipleFunc = lambda kw: kw.get('__account','')

.. confval:: validateExternalRequest

      A function that is called when receiving an external request (e.g. an http request)
      It is called before invoking runActions(). Use it to make sure the request 
      doesn't contain metadata that could dangerously confuse request processing.
      Its signature looks like:
      ``def validateExternalRequest(kw)``
      where `kw` is the request metadata dictionary (which can be modified if necessary).
      It should raise raccoon.NotAuthorized if the request should not be processed.
      
      default is lambda *args: True
      
      ``validateExternalRequest=rhizome.validateExternalRequest``

.. confval:: SECURE_FILE_ACCESS

    Limits URLs access to only the directories reachable through `static_path` or `templates_path`

    default is True

    SECURE_FILE_ACCESS = True

.. confval:: ACTION_CACHE_SIZE

    Sets the maximum number of items to be stored in the Action cache. Set to 0 to disable.

    default is 0

    ACTION_CACHE_SIZE=1000
