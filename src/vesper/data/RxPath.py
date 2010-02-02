'''
    An implementation of RxPath.
    Loads and saves the DOM to a RDF model.

    Copyright (c) 2003 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
'''
from vesper import utils
from vesper.data.RxPathUtils import *
from vesper.data.RxPathModel import *
from vesper.data.RxPathSchema import *

import logging
log = logging.getLogger("RxPath")
