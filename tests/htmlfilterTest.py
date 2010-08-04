#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
"""
    htmlfilter unit tests
"""
import unittest
from vesper.utils import htmlfilter

class TestLinkFixer(htmlfilter.HTMLFilter):
    def __init__(self, out):
        htmlfilter.HTMLFilter.__init__(self, out)
                    
    def needsFixup(self, tag, name, value):
        return value and value.startswith('foo')

    def doFixup(self, tag, name, value, hint):
        return 'bar'
    
class htmlfilterTestCase(unittest.TestCase):

    def runLinkFixer(self, fixerFactory, contents, result):
        import StringIO
        out = StringIO.StringIO()
        fixlinks = fixerFactory(out)
        fixlinks.feed(contents)
        fixlinks.close()
        #print out.getvalue()
        self.failUnless(result == out.getvalue())

    def testLinkFixer(self):
        contents='''<?xml version=1.0 standalone=true ?>
        <!doctype asdf>
        <test link='foo' t='1'>        
        <!-- comment -->
        <![CDATA[some < & > unescaped! ]]>
        some content&#233;more content&amp;dsf<a href='foo'/>
        </test>'''
        result = '''<?xml version=1.0 standalone=true ?>
        <!doctype asdf>
        <test link='bar' t='1'>        
        <!-- comment -->
        <![CDATA[some < & > unescaped! ]]>
        some content&#233;more content&amp;dsf<a href='bar'/>
        </test>'''
        self.runLinkFixer(TestLinkFixer, contents, result)

    def testBlackListHTMLSanitizer(self):        
        contents = '''<html>
        <head>
        <style>
        #test {
            border: 1px solid #000000;
            padding: 10px;
            background-image: url('javascript:alert("foo");')
        }
        </style>
        </head>
        <body id='test'>
        <span onmouseover="dobadthings()">
        <a href="javascript:alert('foo')">alert</a>
        </span>
        </body>
        </html>'''
        result = '''<html>
        <head>
        <style></style>
        </head>
        <body id='test'>
        <span onmouseover="">
        <a href="">alert</a>
        </span>
        </body>
        </html>'''
        #self.runLinkFixer(BlackListHTMLSanitizer, contents, result)
        #test malformed entity references
        #see http://weblogs.mozillazine.org/gerv/archives/007538.html
        #todo: still broken inside PCDATA
        #contents = '''<style>background-image: url(&#106ava&#115cript&#58aler&#116&#40&#39Oops&#39&#41&#59)</style>
        contents = '''<img src="&#106ava&#115cript&#58aler&#116&#40&#39Oops&#39&#41&#59" />'''
        #results = '''<style></style>
        result = '''<img src="" />'''
        self.runLinkFixer(htmlfilter.BlackListHTMLSanitizer, contents, result)

    def testHTMLTruncator(self):        
        def makeTruncator(out):
            fixer = htmlfilter.HTMLTruncator(out)
            fixer.maxWordCount = 3
            return fixer

        contents = '''
        <body>
        <div>
        one two
        three
        four
        </div>
        </body>
        '''

        result = '''
        <body>
        <div>
        one two
        three
        </div></body>'''
        
        self.runLinkFixer(makeTruncator, contents, result)

        contents = '''
        <body>
        <div>
        one two
        three
        </div>
        </body>
        '''
        self.runLinkFixer(makeTruncator, contents, result)
        
        contents = '''
        <html>
        <head>
        <title>text inside the head element should not count</title>
        </head>
        <body>
        <div>
        one two
        three
        four
        </div>
        </body>
        </html>
        '''

        result = '''
        <html>
        <head>
        <title>text inside the head element should not count</title>
        </head>
        <body>
        <div>
        one two
        three
        </div></body></html>'''

        self.runLinkFixer(makeTruncator, contents, result)

        #<div class='summary'> let's the user explicitly declare what to include in the summary
        contents = '''
        <html>
        <head>
        <title>text inside the head element should not count</title>
        </head>
        <body>
        <div class='summary'>
        <div>
        one two
        three
        four
        </div>
        </div>
        </body>
        </html>
        '''

        result = '''
        <html>
        <head>
        <title>text inside the head element should not count</title>
        </head>
        <body>
        <div class='summary'>
        <div>
        one two
        three
        four
        </div>
        </div></body></html>'''

        self.runLinkFixer(makeTruncator, contents, result)
                
if __name__ == '__main__':
    import sys
    try:
        test=sys.argv[sys.argv.index("-r")+1]
        tc = htmlfilterTestCase(test)
        getattr(tc, test)() #run test
    except (IndexError, ValueError):
        unittest.main()

