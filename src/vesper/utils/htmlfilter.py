"""
    This module allows the filtering XML and HTML files
    such that byte for byte, the source content is untouched
    except the filtered parts.

    Includes classes for fixing up links, santizing HTML, and
    truncating content.

    Copyright (c) 2005 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
"""
import HTMLParser, re, sys
#fix bug in HTMLParser, need to handle comments in cdata tags like <script>
HTMLParser.interesting_cdata = re.compile(r'<(/|\Z|!--)')

class HTMLFilter(HTMLParser.HTMLParser, object):
    def __init__(self, out):
        HTMLParser.HTMLParser.__init__(self)
        self.out = out
        self.tagStack = []
                
    def needsFixup(self, tag, name, value):
        '''
        This method is called for each attribute, element content,
        processor instruction, doctype declaration or comment.
        
        tag is the current element's name (or None if not inside an element)
        name is the name of the attribute (or None if not called on an attribute)
        value that may need fixing up.

        You can determine the context in which the method is called from its arguments:
        
        each attribute: tag, name and value (value will be None when encounting HTML compact attributes)
        in element content: tag and value (Note: in non-wellformed HTML, the tag may be incorrect.)
        each comment: value (starts with '<!--')
        each doctype declaration: value (starts with '<!')
        each processor instruction: value (starts with '<?')

        If a non-zero value is returned, doFixup will be called, with the return value passed in as a hint.
        '''
        return False

    def doFixup(self, tag, name, value, hint):
        '''
        See needsFixup for an explanation of the parameters.
        '''
        return value

    def fixupStartTag(self, tag, attrs, tagtext):
        '''
        Implement this if you need to process the full start tag instead each attribute.
        Returns a pair:
            The first returns value is the new tag.
            The second return value is a boolean that indicates whether each attribute should be processed.
        '''
        return tagtext, True

    def fixupEndTag(self, tag, tagtext):
        return tagtext
        
    def handle_starttag(self, tag, attrs):
        self.tagStack.append(tag)
        tagtext = self.get_starttag_text()
        tagtext, fixupAttributes = self.fixupStartTag(tag, attrs, tagtext)
        if fixupAttributes:
            changes = []
            for name, value in attrs:
                hint = self.needsFixup(tag, name, value)
                if hint:
                    newvalue = self.doFixup(tag, name, value, hint)
                    changes.append( (value, newvalue) )
            for old, new in changes:            
                tagtext = tagtext.replace(old, new) #todo: might lead to unexpected results
        self.out.write(tagtext)

    # Overridable -- finish processing of start+end tag: <tag.../>
    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        if self.tagStack:
            self.tagStack.pop()
        
    # Overridable -- handle end tag
    def handle_endtag(self, tag):
        #for html missing end tags:
        while self.tagStack:
           lastTag = self.tagStack.pop()
           if lastTag == tag:
               break
        tagtext = self.fixupEndTag(tag, self.endtag_text)
        self.out.write(tagtext)    

    # Overridable -- handle data
    def handle_data(self, data):
        if self.tagStack:
            tag = self.tagStack[-1]
        else:
            tag = None
        hint = self.needsFixup(tag, None, data)
        if hint:
            data = self.doFixup(tag, None, data, hint)
        self.out.write(data)    

    # Overridable -- handle character reference
    def handle_charref(self, name):
        data = '&#'+name+';'
        hint = self.needsFixup(None, None, data)
        if hint:
            data = self.doFixup(None, None, data, hint)                
        self.out.write(data)    

    # Overridable -- handle entity reference
    def handle_entityref(self, name):
        data = '&'+name+';'
        hint = self.needsFixup(None, None, data)
        if hint:
            data = self.doFixup(None, None, data, hint)                
        self.out.write(data)    

    # Overridable -- handle comment
    def handle_comment(self, data):
        if self.tagStack:
            tag = self.tagStack[-1]
        else:
            tag = None
        data = '<!--'+data+'-->'
        hint = self.needsFixup(tag, None, data)
        if hint:
            data = self.doFixup(tag, None, data, hint)                
        self.out.write(data)    

    # Overridable -- handle declaration
    def handle_decl(self, data):
        data = '<!'+data+'>'
        hint = self.needsFixup(None, None, data)
        if hint:
            data = self.doFixup(None, None, data, hint)        
        self.out.write(data)    

    # Overridable -- handle processing instruction
    def handle_pi(self, data):
        data = '<?'+data+'>' #final ? is included in data 
        hint = self.needsFixup(None, None, data)
        if hint:
            data = self.doFixup(None, None, data, hint)        
        self.out.write(data)    

    def unescape(self, s):
        '''
        This does nothing!!
        We disable this by overriding it so that we get the real attribute value.
        Use reallyUnescape() if you need this functionality
        '''
        return s

    charref = re.compile('&#([0-9]+|[xX][0-9a-fA-F]+);?')    
    def reallyUnescape(self, s):
        s = HTMLParser.HTMLParser.unescape(self, s)
        #HTMLParser.unescape doesn't unescape character references, do those too
        def getRefValue(match):
            val = match.group(1)            
            if val[0] == 'x' or val[0] == 'X':
                return unichr(int(val[1:], 16))
            else:
                return unichr(int(val))
        s = re.sub(self.charref, getRefValue, s)
        return s    

    # we need to override this internal function because xml needs to preserve the case of the end tag
    #Internal -- parse endtag, return end or -1 if incomplete
    def parse_endtag(self, i):
        rawdata = self.rawdata
        assert rawdata[i:i+2] == "</", "unexpected call to parse_endtag"
        match = HTMLParser.endendtag.search(rawdata, i+1) # >
        if not match:
            return -1
        j = match.end()
        match = HTMLParser.endtagfind.match(rawdata, i) # </ + tag + >
        if not match:
            self.error("bad end tag: %s" % `rawdata[i:j]`)
        self.endtag_text = match.string[match.start():match.end()]
        tag = match.group(1)        
        self.handle_endtag(tag.lower())
        if sys.version_info[:2] > (2,2):
            self.clear_cdata_mode() #this line is in the 2.3 version of HTMLParser.parse_endtag
        return j

    in_cdata_section = False
    def updatepos(self, i, j):
        #htmlparser doesn't support CDATA sections hence this terrible hack
        #which rely on the fact that this will be called right after
        #parse_declaration (which calls unknown_decl)
        if self.in_cdata_section:
            self.handle_data(self.rawdata[i:j])
            self.in_cdata_section = False
        return HTMLParser.HTMLParser.updatepos(self, i, j)
        
    def unknown_decl(self, data):
        if data.startswith('CDATA['):
            self.in_cdata_section = True
        else:
            return HTMLParser.HTMLParser.unknown_decl(self, data)

import StringIO
def getRootElementName(string):
    class StopParsing(Exception): pass
    
    class FindRootElementName(HTMLFilter):
        nsURI = ''
        prefix = ''
        local = ''
        
        def handle_starttag(self, tag, attrs):
            parts = tag.split(':',1)
            if len(parts) == 1:
                self.prefix = ''; self.local = parts[0]
            else:
                self.prefix, self.local = parts

            nsattr = self.prefix and 'xmlns:'+self.prefix or 'xmlns'
            for name, value in attrs:
                if name == nsattr:
                    self.nsURI = value
                    
            raise StopParsing
    try:
        gre = FindRootElementName(StringIO.StringIO())
        gre.feed(string)
    except StopParsing:
        pass
    return gre.nsURI, gre.prefix, gre.local

class BlackListHTMLSanitizer(HTMLFilter):
    '''
    Filters outs attribute and elements that match the blacklist.
    If an element's name matches the element black list, its begin and end tags will be stripped out;
    however, the element's content will remain.

    The goals of the default filters are to:
    * prevent javascript from being executed.
    * prevent dangerous objects from be embedded (e.g. using <iframe>, <object>)
    They does not prevent:
    * Using CSS to change the look of page elements outside the user's page real estate
    * Embedding (potentially dangerous or unacceptable) external images
    However, external stylesheets are banned because they may can contain
    Javascript (in the form of a javascript: URLs or IE's "behavior" and "expression()" and Mozilla's "-moz-binding" rules)
    '''
    __super = HTMLFilter #setting this (_BlackListHTMLSanitizer__super) let us chain to another HTMLFilter using inheritance        
    
    SANITIZE = 'sanitize'
    allowPIs = False
    blacklistedElements = ['script','bgsound', 'iframe', 'frame', 'object',
                           'param', 'embed','applet']
    blacklistedAttributes = dict( [(re.compile(name), re.compile(value)) for name, value in 
           {'src|href|link|lowsrc|url|usemap|background|action': '(javascript|vbscript|data):.*',
            'rel':r'.*stylesheet.*', #ban stylesheet links to avoid external stylesheets (which may contain javascript urls)
            'style': r'.*((javascript|vbscript|data|behavior|-moz-binding)\s*:|expression\s*\().*', 
            'http-equiv|on.*': '.*', #disallow these attributes
            }.items()] )
    #scan for content that appears either as text or in a comment
    blacklistedContent = {
        #avoid url() with javascript, data, etc.; behavior or -moz-binding rules and don't let external stylesheets be imported
        re.compile('style'): re.compile(r'(javascript|vbscript|data|behavior|-moz-binding):|@import|expression\s*\(')
        } 

    def onStrip(self, tag, name, value):
        '''
        Override this to do something (e.g. log a warning or raise an error)
        '''
        
    def handle_starttag(self, tag, attrs):
        tag = tag.split(':')[-1] #we ignore namespace prefixes
        if not tag in self.blacklistedElements:            
            return self.__super.handle_starttag(self, tag, attrs)
        self.onStrip(tag, None, None)
                
    def handle_endtag(self, tag):
        tag = tag.split(':')[-1] #we ignore namespace prefixes
        self.currentValue = ''
        if not tag in self.blacklistedElements:
            return self.__super.handle_endtag(self, tag)

    def handle_pi(self, data):
        if not self.allowPIs and data[:3] != 'xml':
            self.onStrip(None, None, data)
        else:
            return self.__super.handle_pi(self, data)
        
    def needsFixup(self, tag, name, value):
        if name: #its an attribute
            name = name.split(':')[-1] #we ignore namespace prefixes
            for namepattern, valuepattern in self.blacklistedAttributes.items():
                if re.match(namepattern,name):
                    if valuepattern is None:
                        return self.SANITIZE
                    value = self.reallyUnescape(value)
                    if re.match(valuepattern,value):
                        return self.SANITIZE
        else:
            if tag is not None: #text
                tag = tag.split(':')[-1] #we ignore namespace prefixes
                for tagpattern, contentpattern in self.blacklistedContent.items():
                    if re.match(tagpattern,tag):
                        if re.search(contentpattern, value):
                            return self.SANITIZE
        return self.__super.needsFixup(self, tag, name, value)

    def doFixup(self, tag, name, value, hint):
        if hint == self.SANITIZE:
            self.onStrip(tag, name, value)
            return ''
        else:
            return self.__super.doFixup(self, tag, name, value, hint)

class WhiteListHTMLSanitizer(HTMLFilter):
    '''
    Filters outs attribute and elements that don't match the whitelist.
    If an element's name matches the element black list, its begin and end tags will be stripped out;
    however, the element's content will remain.
    '''
    __super = HTMLFilter #setting this (_WhiteListHTMLSanitizer__super) let us chain to another HTMLFilter using inheritance        
    
    SANITIZE = 'sanitize'
    allowPIs = False
    whitelistedElements = []
    whitelistedAttributes = {}
    #scan for content that appears either as text or in a comment
    whitelistedContent = {} 

    def onStrip(self, tag, name, value):
        '''
        Override this to do something (e.g. log a warning or raise an error)
        '''
        
    def handle_starttag(self, tag, attrs):
        tag = tag.split(':')[-1] #we ignore namespace prefixes
        if tag in self.whitelistedElements:            
            return self.__super.handle_starttag(self, tag, attrs)
        self.onStrip(tag, None, None)
                
    def handle_endtag(self, tag):
        tag = tag.split(':')[-1] #we ignore namespace prefixes
        self.currentValue = ''
        if tag in self.whitelistedElements:
            return self.__super.handle_endtag(self, tag)

    def handle_pi(self, data):
        if not self.allowPIs and data[:3] != 'xml':
            self.onStrip(None, None, data)
        else:
            return self.__super.handle_pi(self, data)
        
    def needsFixup(self, tag, name, value):
        if name: #its an attribute
            name = name.split(':')[-1] #we ignore namespace prefixes
            match = False
            for namepattern, valuepattern in self.whitelistedAttributes.items():
                if re.match(namepattern,name):
                    if valuepattern: #check attribute value also
                        value = self.reallyUnescape(value)
                        if not re.match(valuepattern,value):
                            continue                    
                    match = True
                    break
            
            if not match:
                return self.SANITIZE
        return self.__super.needsFixup(self, tag, name, value)

    def doFixup(self, tag, name, value, hint):
        if hint == self.SANITIZE:
            self.onStrip(tag, name, value)
            return ''
        else:
            return self.__super.doFixup(self, tag, name, value, hint)

def truncateText(text, maxwords, maxlines=-1, wordCount=0, lineCount=0):
    reachedMax = False
    words = []
    for m in re.finditer(r'(\w+|\A)(\W+|\Z)', text):
        if maxlines > -1 and '\n' in m.group(2):
            lineCount+=1
        if m.group(1).strip():
            wordCount += 1
        words.append(m.group(0))
        if wordCount == maxwords or maxlines == lineCount:
            reachedMax = True
            break        
    if words:
       #if there were no matches, preserve the orginal text
       text =  ''.join(words)
    return text, wordCount, lineCount, reachedMax

class HTMLTruncator(HTMLFilter):
    '''
    In addition, HTMLTruncator will look for a tag pattern to allow 
    the html to explicitly declare what to include in the summary
    (by default, <div class='summary'> ).
    To disable this, set self.summaryTag = ''
    '''
    __super = HTMLFilter #setting this (_HTMLTruncator__super) let us chain to another HTMLFilter using inheritance
    maxWordCount = 0xFFFFF
    maxLineCount = 0xFFFFF
    summaryTag = 'div'
    ignoreTags = ['HEAD', 'head']
    preserveSpaceTags = ['PRE', 'pre']

    #state:    
    noMore = False
    wordCount = 0
    lineCount = 0
    inSummary = 0
    
    def _stop(self):
        self.noMore = True
        while self.tagStack:
           lastTag = self.tagStack.pop()
           self.out.write('</'+lastTag+'>')

    def isSummaryTag(self, tag, attrs):
        for name, value in attrs:
            if name == 'class' and value == 'summary':
                return True
        return False

    def isLineBreakTag(self, tag, attrs):
        return (tag.lower() in ['p', 'br', 'div', 'tr', 'th',
                                'li', 'dd', 'pre', 'blockquote']
                or tag.lower()[0] == 'h') #hr or h1..h6
            
    def handle_starttag(self, tag, attrs):
        if not self.noMore:
            if tag.lower() == self.summaryTag and (self.inSummary
                                  or self.isSummaryTag(tag, attrs)):
                self.inSummary += 1
            if not self.inSummary and self.isLineBreakTag(tag, attrs):
                if self.lineCount == self.maxLineCount:
                    self._stop()
                    return
                self.lineCount += 1
            return self.__super.handle_starttag(self, tag, attrs)

    def handle_endtag(self, tag):
        if self.inSummary and tag == self.summaryTag:
            self.inSummary -= 1
            if not self.inSummary:
                self._stop()
            
        if not self.noMore:
            return self.__super.handle_endtag(self, tag)
    
    def handle_charref(self, name):
        if not self.noMore:
            return self.__super.handle_charref(self, name)

    # Overridable -- handle entity reference
    def handle_entityref(self, name):
        if not self.noMore:
            return self.__super.handle_entityref(self, name)

    # Overridable -- handle data
    def handle_data(self, data):
        if self.noMore:
            return
        #only count text that isn't contained by tag in self.ignoreTags
        if not self.inSummary and not [tag for tag in self.ignoreTags
                                            if tag in self.tagStack]:            
            if [tag for tag in self.preserveSpaceTags
                            if tag in self.tagStack]:
                #if the text is contained in a tag in self.preserveSpaceTags, 
                #count line breaks too
                maxlines = self.maxLineCount
            else:
                maxlines = -1
                
            data, self.wordCount, self.lineCount, self.noMore = truncateText(
             data, self.maxWordCount, maxlines,self.wordCount, self.lineCount)

            if self.noMore:
                self.__super.handle_data(self,data)
                self._stop()
                return
        return self.__super.handle_data(self,data)                        

    def handle_comment(self, data):
        if not self.noMore:
            return self.__super.handle_comment(self, data)

    def handle_pi(self, data):
        if not self.noMore:
            return self.__super.handle_pi(self, data)
