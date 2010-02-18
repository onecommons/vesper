from vesper.web.route import Route, servetemplate
from vesper.utils import attrdict
from vesper.app import loadApp, getCurrentApp, _current_configpath
from vesper.backports import json

Route('{path:.+}.html')(servetemplate)

import logging
logging.basicConfig()

app = loadApp(__name__, 'vesper.web.baseapp'
              ,static_path=['static','']
              ,template_path=['templates']
              ,STORAGE_PATH="app-store-rev.mjson"
              ,modelOptions=dict(serializeOptions=dict(indent=2))
              ,currentMod = __name__
              #,saveHistory='split'
              #,firepython_enabled = 1
)

def parseCmdLine():
    from vesper.web.baseapp import parseCmdLine as baseParse
    baseParse()
    a = getCurrentApp()
    assert a == app, "app confusion!"
    app.run()

if __name__ == "__main__":
    parseCmdLine()