"""
    Copyright (c) 2003 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
"""

__version__ = '0.7.0'

import logging

#for pythons older than 2.5:
try:
    all = all
    any = any
except NameError:
    def all(iterable):
         for element in iterable:
             if not element:
                 return False
         return True

    def any(iterable):
         for element in iterable:
             if element:
                 return True
         return False