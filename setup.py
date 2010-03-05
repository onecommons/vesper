#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.

import ez_setup
ez_setup.use_setuptools()
from setuptools import setup, find_packages
import sys, os

install_requires = ['ply', 'routes', 'mako']
#XXX add optional libraries: pyyaml, stomp.py
PACKAGE_NAME = 'vesper'
pyver = sys.version_info[:2]
if pyver < (2,4):
    print "Sorry, %s requires version 2.4 or later of Python" % PACKAGE_NAME
    sys.exit(1)        
if pyver < (2,5):
  install_requires.extend(['wsgiref'])
if pyver < (2,6):
  install_requires.extend(['simplejson'])

# data_files generation derived from django setup.py
data_files = []
root_dir = os.path.dirname(__file__)
if root_dir != '':
  os.chdir(root_dir)

# add python files
vesper_dir = 'src/vesper'
for dirpath, dirnames, filenames in os.walk(vesper_dir):
  # Ignore dirnames that start with '.'
  for i, dirname in enumerate(dirnames):
      if dirname.startswith('.') or 'mako_modules' in dirname: 
        del dirnames[i]
  if '__init__.py' in filenames:
      continue
  elif filenames:
      # need to omit leading 'src/' from dirpath on the destination
      destpath = dirpath
      if destpath.startswith("src/"):
          destpath = destpath[4:]
      data_files.append([destpath, 
        [os.path.join(dirpath, f) 
            for f in filenames if f != '.DS_Store']
      ])

# XXX needs a way to specify wildcards to filter out files/directories
def build_file_paths(dir, data):
    for dirpath, dirnames, filenames in os.walk(dir):
        # print dirpath, dirnames, filenames
        for i, dirname in enumerate(dirnames):
            if dirname.startswith('.'):
                del dirnames[i]
        if filenames:
            data.append([dirpath, [os.path.join(dirpath, f) for f in filenames]])

# add docs and examples
build_file_paths('doc', data_files)
build_file_paths('examples', data_files)

setup(
    name = PACKAGE_NAME,
    version = "0.0.1",
    package_dir = {'': 'src'},
    packages = find_packages('src'),
    data_files = data_files,
    install_requires = install_requires,
    zip_safe=False,

   entry_points = {
        'console_scripts': [
            'vesper-admin = vesper.web.admin:console_main',
        ],
    },

)