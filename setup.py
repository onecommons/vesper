import ez_setup
ez_setup.use_setuptools()
from setuptools import setup, find_packages
import sys, os

install_requires = ['ply', 'routes']
#XXX add: optional mako, pyyaml, stomp.py
PACKAGE_NAME = 'vesper'
pyver = sys.version_info[:2]
if pyver < (2,4):
    print "Sorry, %s requires version 2.4 or later of Python" % PACKAGE_NAME
    sys.exit(1)        
if pyver < (2,5):
  install_requires.extend(['wsgiref'])
if pyver < (2,6):
  install_requires.extend(['simplejson'])

# copied from django setup.py
# Compile the list of packages available, because distutils doesn't have
# an easy way to do this.
def fullsplit(path, result=None):
    """
    Split a pathname into components (the opposite of os.path.join) in a
    platform-neutral way.
    """
    if result is None:
        result = []
    head, tail = os.path.split(path)
    if head == '':
        return [tail] + result
    if head == path:
        return result
    return fullsplit(head, [tail] + result)

packages, data_files = [], []
root_dir = os.path.dirname(__file__)
if root_dir != '':
  os.chdir(root_dir)
vesper_dir = 'src/vesper'

for dirpath, dirnames, filenames in os.walk(vesper_dir):
  # Ignore dirnames that start with '.'
  for i, dirname in enumerate(dirnames):
      if dirname.startswith('.'): del dirnames[i]
  if '__init__.py' in filenames:
      packages.append('.'.join(fullsplit(dirpath)))
  elif filenames:
      # need to omit leading 'src/' from dirpath on the destination
      destpath = dirpath
      if destpath.startswith("src/"):
          destpath = destpath[4:]
      data_files.append([destpath, [os.path.join(dirpath, f) for f in filenames]])

setup(
    name = PACKAGE_NAME,
    version = "0.0.1",
    package_dir = {'': 'src'},
    packages = find_packages('src'),
    data_files = data_files,
    #py_modules = ['sjson', 'raccoon', 'htmlfilter'],
    install_requires = install_requires,
    zip_safe=False,

   entry_points = {
        'console_scripts': [
            'vesper-admin = vesper.web.admin:console_main',
        ],
    },

)