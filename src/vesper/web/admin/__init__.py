#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
from vesper.web.route import Route, servetemplate
from vesper.utils import attrdict
from vesper.app import createApp, getCurrentApp
from vesper.backports import json

Route('{path:.+}.html')(servetemplate)

import logging
logging.basicConfig()

app = createApp(__name__, 'vesper.web.baseapp'
              ,static_path=['static']
              ,template_path=['templates']
              ,storage_path="app-store-rev.mjson"
              ,model_options=dict(serializeOptions=dict(indent=2))
              ,currentMod = __name__
              #,save_history='split'
              #,firepython_enabled = 1
)

# entry point from setuptools console_scripts, called with no args
def console_main():
    from vesper.web.baseapp import parseCmdLine
    parseCmdLine()
    # a = getCurrentApp()
    # assert a == app, "app confusion!"
    app.run()

if __name__ == "__main__":
    console_main()