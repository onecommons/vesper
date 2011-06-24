from vesper.data.store import basic 
import sys

if len(args) == 2:
  source = sys.argv[1]
  dest = sys.argv[2]
else:
  print 'usage: copystore.py [source] [dest]'
  sys.exit(-1)

stmts, format, fsize, mtime = basic.loadFileStore(source)
store = basic.MemStore(stmts)
format = basic.guessFileType(dest)
if not format:
  print 'error: unknown format for', dest
  sys.exit(-1)  
outputfile = open(dest, 'wt')
try:
  stmts = store.getStatements()
  basic.serializeRDF_Stream(stmts, outputfile, format)
finally:
  outputfile.close()

