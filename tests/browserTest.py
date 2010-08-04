#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
import webbrowser, unittest
import multiprocessing, random
from vesper.utils import Uri
from vesper.backports import json
from vesper import app
from vesper.web.route import Route

def startVesperInstance(port, queue):
    @app.Action
    def sendServerStartAction(kw, retVal):
        # print "startReplication callback!"
        queue.put('server ready')
    
    @Route('testresult')#, REQUEST_METHOD='POST')
    def handleTestresult(kw, retval):
        queue.put(json.loads(kw._postContent))
        kw._responseHeaders['Content-Type'] = 'application/json'
        return '"OK"'
    
    print "creating vesper instance on port %d)" % (port)
    app.createApp(__name__, 'vesper.web.baseapp', port=port, storage_url="mem://", 
        static_path=['.'], 
        actions = {'load-model':[sendServerStartAction]}
    ).run()
    # blocks forever

class BrowserTestRunnerTest(unittest.TestCase):

    def startServer(self):
        port = 5555 #random.randrange(5000,9999)
        queue = multiprocessing.Queue()        
        serverProcess = multiprocessing.Process(target=startVesperInstance, args=(port,queue))
        serverProcess.start()        
        return serverProcess, queue, port

    def testBrowserTests(self):
        serverProcess, queue, port = self.startServer()
        urls = ['db_tests.html','binder_tests.html']
        try: 
            queue.get(True, 5) #raise Queue.EMPTY if server isn't ready in 5 second 
            for name in urls:
                url = 'http://localhost:%d/static/%s' % (port, name)
                print 'running ', url
                webbrowser.open(url)
                testResults = queue.get(True, 20) #raise Queue.EMPTY if browser unittests haven't finished in 20 seconds
                print '%(total)d total, %(passed)d passed %(failed)d failed %(ignored)d ignored' % testResults
                self.assertEqual(testResults['passed'], testResults['total'])
        finally:
            if not keepRunnng:
                serverProcess.terminate()
            else:
                try:
                    serverProcess.join() #block
                except:
                    serverProcess.terminate()

keepRunnng = False
if __name__ == '__main__':
    import sys
    keepRunnng = '--run' in sys.argv
    if keepRunnng:
        sys.argv.remove('--run')
    unittest.main()
