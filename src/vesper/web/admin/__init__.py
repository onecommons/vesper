#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
from vesper.web.route import Route, servetemplate
from vesper.utils import attrdict
from vesper.app import createApp, getCurrentApp
from vesper.backports import json

import logging
logging.basicConfig()

import os.path, glob, re, os
def beforeConfigHook(config):
    '''
    Adds a config option called "playbackTests" for providing a lists of 
    playback scripts to run when qaplayback.html is opened.
    
    On startup, the app will check if each playback script begins with a 
    comment block that follows this pattern:
    
    /*
    url: index.html
    storage-template: test.json
    store-name: test
    */
    
    If "url" is present that URL will be opened when the script is run.
    If "storage-template" or "store-name" is present an in-memory store will be created,
    using the template, if specified. 
    '''
    if not config.get('playbackTests'):
        return

    tests = config['playbackTests']
    pages = []
    if not isinstance(tests, (list,tuple)):
        if os.path.isdir(tests):
            tests = glob.glob(os.path.join(tests,'*.js'))
        else:
            tests = [tests]

    for name in tests:
        #extract metadata from comment at top of script
        script = open(name).read()
        match = re.match(r"/\*(.+?)\*/", script, re.S)
        if match:
            metadata = match.group(1)
            match = re.search(r'^url:[ \t]*(\S+)[ \t]*$', metadata, re.M)
            url = match and match.group(1)
            match = re.search(r'^storage-template:[ \t]*(\S+)[ \t]*$', metadata, re.M)
            storage = match and match.group(1)
            if storage and not os.path.isabs(storage):
                storage = os.path.abspath( os.path.join(os.path.dirname(name), storage))
            match = re.search(r'^store-name:[ \t]*(\S+)[ \t]*$', metadata, re.M)
            storename = match and match.group(1)
        else:
            url, storage, storename = '', '', ''
        if storage and not storename:
            storename = os.path.splitext(os.path.split(name)[1])[0]
        if storename:
            if 'stores' not in config:
                config['stores'] = { 'default' : config }            
            store = config['stores'][storename] = dict(storage_url='mem:')
            if storage:
                store['storage_template_path']=storage
            if url:
                #hack: assumes a route like {db}/{path:.*\.html}
                url = '/' + storename + (url[0] != '/' and '/' or '') + url
        pages.append( (name, url or '') )

    def path2url(name):
        path = os.path.abspath(name)
        static_path = config['static_path']
        for dir in static_path:
            dir = os.path.abspath(dir)
            if path.startswith(dir):
                path = path[len(dir):]
                break
        else:
            #not found on static_path
            dir, path = os.path.split(path)
            assert dir
            static_path.append(dir)

        path = os.path.join('/static', path)
        if os.sep != '/':
            path = path.replace(os.sep,'/')
        return path

    config['playbackScripts'] = [(path2url(name), url) for name, url in pages]
    config['testplayback'] = True

app = createApp(__name__, 'vesper.web.baseapp'
              ,static_path=['static']
              ,template_path=['templates']
              ,beforeConfigHook=beforeConfigHook
              ,storeDefaults = dict(
                model_options=dict(serializeOptions=dict(indent=2))
               )
)



#add routes after createApp if you want them to run after base app's routes
Route(r'{path:.+\.html}')(servetemplate)

# entry point from setuptools console_scripts, called with no args
def console_main():
    app.run()

if __name__ == "__main__":
    console_main()