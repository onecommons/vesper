from mako import runtime, filters, cache
UNDEFINED = runtime.UNDEFINED
__M_dict_builtin = dict
__M_locals_builtin = locals
_magic_number = 5
_modified_time = 1266023843.69226
_template_filename='/Users/admin/projects/vesper/rhizome-rename/src/vesper/web/admin/templates/index.html'
_template_uri='index.html'
_template_cache=cache.Cache(__name__, _modified_time)
_source_encoding='utf-8'
_exports = []


# SOURCE LINE 3

from vesper.backports import json


def _mako_get_namespace(context, name):
    try:
        return context.namespaces[(__name__, name)]
    except KeyError:
        _mako_generate_namespaces(context)
        return context.namespaces[(__name__, name)]
def _mako_generate_namespaces(context):
    # SOURCE LINE 2
    ns = runtime.Namespace('layout', context._clean_inheritance_tokens(), templateuri='layout.html', callables=None, calling_uri=_template_uri, module=None)
    context.namespaces[(__name__, 'layout')] = ns

def render_body(context,**pageargs):
    context.caller_stack._push_frame()
    try:
        __M_locals = __M_dict_builtin(pageargs=pageargs)
        layout = _mako_get_namespace(context, 'layout')
        __M_writer = context.writer()
        __M_writer(u'\n')
        # SOURCE LINE 5
        __M_writer(u'\n\n')
        def ccall(caller):
            def head():
                context.caller_stack._push_frame()
                try:
                    __M_writer = context.writer()
                    # SOURCE LINE 9
                    __M_writer(u'\n  <title>vesper admin</title>\n  ')
                    return ''
                finally:
                    context.caller_stack._pop_frame()
            def style():
                context.caller_stack._push_frame()
                try:
                    __M_writer = context.writer()
                    # SOURCE LINE 13
                    __M_writer(u'\n  ')
                    # SOURCE LINE 14
                    __M_writer(filters.decode.utf8(layout.panelCss()))
                    __M_writer(u'\n  .ui-icon-refresh {\n      display:none;\n  }\n  .panel-queryarea {\n      background-color: whitesmoke;\n      padding: 6px;\n      padding-bottom: 12px; \n      margin-bottom:12px;\n      overflow-y: hidden;\n      -moz-border-radius-bottomleft: 5px;\n      -moz-border-radius-bottomright: 5px;    \n      -webkit-border-bottom-left-radius: 5px;\n      -webkit-border-bottom-right-radius: 5px;        \n\n      -moz-box-shadow: 3px 3px 5px rgba(0, 0, 0, 0.5);\n      -webkit-box-shadow: 3px 3px 5px rgba(0, 0, 0, 0.5);\n  }\n  #query {\n    width: 100%;\n  }\n  .query-options {\n      float:right;\n      padding-right:20px;\n  }\n  .resultbox {\n      padding: 6px;\n      -moz-border-radius: 5px;\n      -webkit-border-radius: 5px;        \n\n      -moz-box-shadow: 3px 3px 5px rgba(0, 0, 0, 0.5);\n      -webkit-box-shadow: 3px 3px 5px rgba(0, 0, 0, 0.5);\n  }\n  #error {\n    display:none;\n    background-color:lightsalmon;\n    margin-top: 12px;\n    margin-bottom: 12px;\n  }  \n  #explainresults {\n    display:none;\n    background-color:palegreen;\n    margin-top: 12px;\n  }\n  #debugresults {\n    display:none;\n    background-color:lightsteelblue;\n    margin-top: 12px;\n  }\n  ')
                    return ''
                finally:
                    context.caller_stack._pop_frame()
            def sidebar():
                context.caller_stack._push_frame()
                try:
                    __M_writer = context.writer()
                    # SOURCE LINE 65
                    __M_writer(u'\n      sidebar!!!\n  ')
                    return ''
                finally:
                    context.caller_stack._pop_frame()
            def headerContents():
                context.caller_stack._push_frame()
                try:
                    __M_writer = context.writer()
                    # SOURCE LINE 70
                    __M_writer(u'\n    Query&nbsp;<span id="headerquery"></span>\n    ')
                    return ''
                finally:
                    context.caller_stack._pop_frame()
            def script():
                context.caller_stack._push_frame()
                try:
                    __M_writer = context.writer()
                    # SOURCE LINE 99
                    __M_writer(u"\n    <script type='text/javascript'>\n      ")
                    def ccall(caller):
                        def body():
                            __M_writer = context.writer()
                            return ''
                        return [body]
                    caller = context.caller_stack._get_caller()
                    context.caller_stack.nextcaller = runtime.Namespace('caller', context, callables=ccall(caller))
                    try:
                        # SOURCE LINE 101
                        __M_writer(filters.decode.utf8(layout.initPanelJs()))
                    finally:
                        context.caller_stack.nextcaller = None
                    __M_writer(u'\n      \n      $(document).ready(function(){\n          $(\'#query-button\').click(function() {\n              var q = {captureErrors:true};\n              q.query = $(\'#query\').val();\n              q.explain = $(\'#opt-explain\').is(\':checked\');\n              q.forUpdate = $(\'#opt-forupdate\').is(\':checked\');\n              q.debug = $(\'#opt-debug\').is(\':checked\');\n              $(\'#queryresults\').dbQuery(q, function(data) {\n                  if (data.results) {\n                      $(\'#queryresults\').text(JSON.stringify(data.results, null, 4));                      \n                      $(\'#headerquery\').text(" - Showing " + data.results.length + " objects");                        \n                  }\n                  if (data.errors && data.errors.length > 0) {\n                      $(\'#error .precontent\').text(JSON.stringify(data.errors, null, 4));\n                      $(\'#error\').show();\n                  } else {\n                      $(\'#error\').hide();\n                  }\n                  if (data.explain) {\n                      $(\'#explainresults .precontent\').text(data.explain);\n                      $(\'#explainresults\').show();\n                  } else {\n                      $(\'#explainresults\').hide();\n                  }\n                  if (data.debug) {\n                      $(\'#debugresults .precontent\').text(data.debug);\n                      $(\'#debugresults\').show();\n                  } else {\n                      $(\'#debugresults\').hide();                      \n                  }\n              });\n          });\n      });\n      \n    </script>\n  ')
                    return ''
                finally:
                    context.caller_stack._pop_frame()
            def body():
                __M_writer = context.writer()
                # SOURCE LINE 7
                __M_writer(u'\n\n  ')
                # SOURCE LINE 11
                __M_writer(u'\n\n  ')
                # SOURCE LINE 63
                __M_writer(u'\n\n  ')
                # SOURCE LINE 67
                __M_writer(u'\n\n  ')
                def ccall(caller):
                    def headerContents():
                        context.caller_stack._push_frame()
                        try:
                            __M_writer = context.writer()
                            # SOURCE LINE 70
                            __M_writer(u'\n    Query&nbsp;<span id="headerquery"></span>\n    ')
                            return ''
                        finally:
                            context.caller_stack._pop_frame()
                    def body():
                        __M_writer = context.writer()
                        # SOURCE LINE 69
                        __M_writer(u'\n    ')
                        # SOURCE LINE 72
                        __M_writer(u'\n  <div id="queryinput" class="panel-queryarea">\n  Enter your query: <textarea id=\'query\'></textarea>\n  <button id=\'query-button\'>Go</button>\n  <div class="query-options">\n    <label><input type="checkbox" id="opt-explain">Explain</label>&nbsp;&nbsp;\n    <label><input type="checkbox" id="opt-forupdate">For Update</label>\n    <label><input type="checkbox" id="opt-debug">Debug</label>    \n  </div>\n  </div>\n  <div id=\'error\' class="resultbox">\n  <b>Error:</b><hr>\n  <div class="precontent">&nbsp;</div>\n  </div>  \n  <b>Results:</b>\n  <div id=\'queryresults\' class="precontent">\n  </div>\n  <div id=\'explainresults\' class="resultbox">\n  <b>Explain:</b><hr>\n  <div class="precontent">&nbsp;</div>\n  </div>\n  <div id=\'debugresults\' class="resultbox">\n  <b>Debug:</b><hr>\n  <div class="precontent">&nbsp;</div>\n  </div>  \n  ')
                        return ''
                    return [body,headerContents]
                caller = context.caller_stack._get_caller()
                context.caller_stack.nextcaller = runtime.Namespace('caller', context, callables=ccall(caller))
                try:
                    # SOURCE LINE 69
                    __M_writer(filters.decode.utf8(layout.renderPanel(panelId='main-panel')))
                finally:
                    context.caller_stack.nextcaller = None
                # SOURCE LINE 97
                __M_writer(u'\n\n  ')
                # SOURCE LINE 138
                __M_writer(u'\n\n')
                return ''
            return [body,head,style,sidebar,headerContents,script]
        caller = context.caller_stack._get_caller()
        context.caller_stack.nextcaller = runtime.Namespace('caller', context, callables=ccall(caller))
        try:
            # SOURCE LINE 7
            __M_writer(filters.decode.utf8(layout.layout()))
        finally:
            context.caller_stack.nextcaller = None
        return ''
    finally:
        context.caller_stack._pop_frame()


