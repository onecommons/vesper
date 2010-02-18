from mako import runtime, filters, cache
UNDEFINED = runtime.UNDEFINED
__M_dict_builtin = dict
__M_locals_builtin = locals
_magic_number = 5
_modified_time = 1266024092.829072
_template_filename='/Users/admin/projects/vesper/rhizome-rename/src/vesper/web/admin/templates/layout.html'
_template_uri='layout.html'
_template_cache=cache.Cache(__name__, _modified_time)
_source_encoding=None
_exports = ['renderPanel', 'renderEntry', 'layout', 'initPanelJs', 'panelCss']


# SOURCE LINE 1

class call(object):
    def __init__(self,obj):
        self.obj = obj
        
    def __getattr__(self, name):
        func = getattr(self.obj, name, None)
        if func:
            return func()
        else:
            return ''        
    


def render_body(context,**pageargs):
    context.caller_stack._push_frame()
    try:
        __M_locals = __M_dict_builtin(pageargs=pageargs)
        __M_writer = context.writer()
        # SOURCE LINE 13
        __M_writer(u'\n')
        # SOURCE LINE 101
        __M_writer(u'\n\n')
        # SOURCE LINE 196
        __M_writer(u'\n\n')
        # SOURCE LINE 208
        __M_writer(u'\n\n')
        # SOURCE LINE 233
        __M_writer(u'\n\n')
        # SOURCE LINE 401
        __M_writer(u'\n')
        return ''
    finally:
        context.caller_stack._pop_frame()


def render_renderPanel(context,headerContents='',panelId='',bodyClass=''):
    context.caller_stack._push_frame()
    try:
        caller = context.get('caller', UNDEFINED)
        __M_writer = context.writer()
        # SOURCE LINE 198
        __M_writer(u'\n<div ')
        # SOURCE LINE 199
        __M_writer(filters.decode.utf8(panelId and 'id='+panelId))
        __M_writer(u' class=\'panel\'>\n    <div class=\'panel-header\'>\n        <span class="ui-icon ui-icon-refresh" style=\'float: right\'></span>\n        <span class=\'panel-header-content\'>')
        # SOURCE LINE 202
        __M_writer(filters.decode.utf8(headerContents or call(caller).headerContents))
        __M_writer(u"</span>\n    </div>\n    <div class='panel-body ")
        # SOURCE LINE 204
        __M_writer(filters.decode.utf8(bodyClass))
        __M_writer(u"'>\n      ")
        # SOURCE LINE 205
        __M_writer(filters.decode.utf8(caller.body()))
        __M_writer(u'\n    </div>\n</div>\n')
        return ''
    finally:
        context.caller_stack._pop_frame()


def render_renderEntry(context,i,id,editcontents):
    context.caller_stack._push_frame()
    try:
        caller = context.get('caller', UNDEFINED)
        __M_writer = context.writer()
        # SOURCE LINE 210
        __M_writer(u"       \n  <form class='entry' name='entry-")
        # SOURCE LINE 211
        __M_writer(filters.decode.utf8(i))
        __M_writer(u"' itemid='")
        __M_writer(filters.decode.utf8(id))
        __M_writer(u'\' accept-charset="UTF-8">\n')
        # SOURCE LINE 212
        if id:
            # SOURCE LINE 213
            __M_writer(u"    <input type='hidden' name='id' value='")
            __M_writer(filters.decode.utf8(id))
            __M_writer(u"' />\n")
        # SOURCE LINE 215
        __M_writer(u"  <div class='entry-edit' style='display:none'>\n   <button class='entry-save-button' disabled='true'>save</button>\n   <button class='entry-preview-button'>preview</button>\n   <button class='entry-delete-button'>delete</button>\n   <button class='entry-canceledit-button'>cancel</button>\n   <textarea name='contents' class='expand' style='width: 100%'>")
        # SOURCE LINE 220
        __M_writer(filters.decode.utf8(editcontents))
        __M_writer(u"</textarea>\n   <div class='preview-contents'></div>   \n  </div>\n  <div class='item")
        # SOURCE LINE 223
        __M_writer(filters.decode.utf8(i%2 and " altband" or ""))
        __M_writer(u"'>")
        __M_writer(filters.decode.utf8(call(caller).renderItemProperties))
        __M_writer(u"\n      <!--\n      contents are block elements (p) so bullet doesnt appear inline as we want\n      -->\n      <span class='item-bullet'>&bull;</span>\n      <div class='item-contents'>\n      ")
        # SOURCE LINE 229
        __M_writer(filters.decode.utf8(caller.body()))
        __M_writer(u'      \n      </div>\n  </div>\n  </form>\n')
        return ''
    finally:
        context.caller_stack._pop_frame()


def render_layout(context):
    context.caller_stack._push_frame()
    try:
        caller = context.get('caller', UNDEFINED)
        errors = context.get('errors', UNDEFINED)
        __M_writer = context.writer()
        # SOURCE LINE 14
        __M_writer(u'\n<html>\n<head>\n<meta http-equiv="Content-Type" content="text/html; charset=utf-8">\n')
        # SOURCE LINE 18
        __M_writer(filters.decode.utf8(call(caller).head))
        __M_writer(u'\n\n<link rel="stylesheet" type="text/css" href="static/reset-fonts-grids.css">\n<link rel="stylesheet" type="text/css" href="static/base.css">\n\n<link rel="stylesheet" href="http://ajax.googleapis.com/ajax/libs/jqueryui/1.7.2/themes/ui-lightness/jquery-ui.css" type="text/css" media="all" />\n<link rel="stylesheet" type="text/css" href="static/generic.css" />\n\n<style type="text/css">\n\n.precontent {\n white-space: pre; word-wrap: break-word; /* for ie */\n white-space: pre-wrap; /* everyone else */\n}\n\n#yui-main .yui-b {\n    border-right: 3px dotted black;\n    padding-right: 5px;    \n    min-height: 400px;\n}\n\n.rightcol {\n min-height: 400px;\n}\n\n#hd {\n padding: 10px;\n}\n\n#bd {\n min-height: 300px;\n}\n\n.header-title {\nfont-variant:small-caps;\nfont-weight:bold;\nfont-size: 1.5em;\ncolor:#8D4A2C;\n}\n\n')
        # SOURCE LINE 58
        __M_writer(filters.decode.utf8(call(caller).style))
        __M_writer(u'\n</style>\n\n</head>\n<body>\n    \n    <!-- doc3 == 100% -->\n<div id=\'doc3\' class="yui-t4">\n  <div id="hd" class="header-title">      \n    <div>')
        # SOURCE LINE 67
        __M_writer(filters.decode.utf8(call(caller).header))
        __M_writer(u'</div>\n')
        # SOURCE LINE 68
        if errors:
            # SOURCE LINE 69
            __M_writer(u"        <div class='error'>Error:\n        ")
            # SOURCE LINE 70
            __M_writer(filters.decode.utf8('<br/>'.join(errors)))
            __M_writer(u'\n        </div>\n')
        # SOURCE LINE 73
        __M_writer(u'  </div>\n  <div id="bd">\n      <div id="yui-main">          \n          <div class="yui-b">          \n          ')
        # SOURCE LINE 77
        __M_writer(filters.decode.utf8(caller.body()))
        __M_writer(u'\n          </div>\n      </div>\n      <div class="yui-b rightcol">          \n         ')
        # SOURCE LINE 81
        __M_writer(filters.decode.utf8(call(caller).sidebar))
        __M_writer(u'\n      </div>      \n</div>   \n     <div id="ft">\n        ')
        # SOURCE LINE 85
        __M_writer(filters.decode.utf8(call(caller).footer))
        __M_writer(u'\n    </div>\n</div>\n\n')
        # SOURCE LINE 91
        __M_writer(u'<script src="http://ajax.googleapis.com/ajax/libs/jquery/1.3.2/jquery.js"></script>\n<script src="http://ajax.googleapis.com/ajax/libs/jqueryui/1.7.2/jquery-ui.js" type="text/javascript"></script>\n<script type="text/javascript" src="static/jquery.hresize.js"></script>\n<script type="text/javascript" src="static/js/data.js"></script>\n<script type="text/javascript" src="static/js/json2.js"></script>\n\n    ')
        # SOURCE LINE 97
        __M_writer(filters.decode.utf8(call(caller).script))
        __M_writer(u'        \n</body>\n</html>\n\n')
        return ''
    finally:
        context.caller_stack._pop_frame()


def render_initPanelJs(context,refreshPanel='null'):
    context.caller_stack._push_frame()
    try:
        caller = context.get('caller', UNDEFINED)
        __M_writer = context.writer()
        # SOURCE LINE 235
        __M_writer(u'\n/** begin generic panel/item code **/\n    \nfunction startModifyChecking($this){    \n    if (!$(this).data(\'modifiedChecker\')) {\n        var timerId = setInterval(function() {\n            setModified($this, $this.val() !=  $this.data(\'originalValue\'));\n        }, 300);\n        $(this).data(\'modifiedChecker\', timerId);\n    }\n}\n\nfunction stopModifyChecking($this){    \n    var timerId = $(this).data(\'modifiedChecker\');\n    if (timerId) {\n        //do a final check:\n        setModified($this, $this.val() !=  $this.data(\'originalValue\'));\n        clearInterval(timerId);\n        $(this).removeData(\'modifiedChecker\');\n    }\n}\n\nfunction setModified($this, changed) {\n   var container = $this.closest(\'.entry\')\n   $(\'.modifiedIndicator\', container).text(changed?\'*\':\'\');    \n   $(\'.entry-save-button\', container)[0].disabled = !changed;\n}\n\nfunction show() { \n  startModifyChecking($(this)); \n}\n\nfunction hide() { \n  stopModifyChecking($(this));\n}\n\nfunction toggleEditEntry() {\n    var container = $(this).closest(\'.entry\');\n    $(\'.item,.entry-edit\', container).toggle();\n    $(\'.preview-contents\', container).html(\'\');\n }\n\nfunction editEntry(event) {\n    event.preventDefault();\n    toggleEditEntry.call(this);\n    $(this).closest(\'.entry\').find(\'.entry-edit textarea\')[0].focus();\n    return false;\n}\n\nfunction makePanels(selector, refreshCallback) {\n    var panels = $(selector);\n\n    panels.addClass("ui-helper-clearfix ui-widget")\n                .find(".panel-header")\n                    .addClass("ui-widget-header")\n                    .prepend(\'<span class="ui-icon showhide-button ui-icon-triangle-1-s"></span>\')\n                ;//.end().find(".portlet-content").addClass("ui-widget-content");\n\n    /*\n    panels.wrapInner("<div class=\'panel-body\'></div>").prepend(\n        \'<div class="panel-header"><span class="ui-icon ui-icon-triangle-1-s"></span></div>\');\n   */\n\n    $(\'.panel-body\', panels).hresize(); //{ minHeight: 100})\n\n    $(\'.panel-header .showhide-button\', panels).click(function() {\n        var content = $(this).closest(\'.panel-header\').next();\n        if (content.is(\':hidden\')) {\n            $(this).removeClass(\'ui-icon-triangle-1-e\').addClass(\'ui-icon-triangle-1-s\');\n            content.show(\'blind\', 50);\n        } else {\n            $(this).removeClass(\'ui-icon-triangle-1-s\').addClass(\'ui-icon-triangle-1-e\');\n            content.hide(\'blind\', 50);\n        }\n        return false;\n    });\n\n    if (refreshCallback)\n        $(\'.panel-header .ui-icon-refresh\').click(refreshCallback);\n}\n\nvar gItemSaveQuery = "{ * where (id=@this)  }";\n\nfunction initItems(context) {\n   context = context || document;\n\n   $(\'.entry-edit textarea\', context).focus(show).blur(hide);\n   \n   $(\'.entry-edit textarea\', context).each(function() { \n        $(this).data(\'originalValue\', $(this).val()); \n    } );\n   \n   $(\'.item\', context).click(editEntry);\n   $(\'.item-properties, .item-contents a\').click(function(event) { event.stopPropagation(); } );\n')
        # SOURCE LINE 330
        __M_writer(u"   $('.entry-edit-button', context).click(editEntry);\n   \n   $('.entry-canceledit-button', context).click(function(event) {\n       event.preventDefault();\n       //restore original\n       var $entry = $(this).closest('.entry');\n       if ($entry.find('.entry-save-button:enabled').length &&\n          !confirm('lose changes?')) {\n              return;\n       }\n       var textarea = $entry.find('.entry-edit textarea');       \n       textarea.val( textarea.data('originalValue') );\n       textarea[0].blur();\n       setModified(textarea, false);\n       toggleEditEntry.call(this);\n   });\n\n   $('.entry-cancelcreate-button', context).click(function(event) {\n       event.preventDefault();\n       var $entry = $(this).closest('.entry');\n       if ($entry.find('.entry-create-button:enabled').length &&\n          !confirm('lose changes?')) {\n              return;\n       }\n       $entry.find('.preview-contents').html('');\n       $entry[0].reset();        \n       $entry.find('.tagfield,.bit-box').remove();       \n   });\n   \n   $('.entry-save-button', context).click(function(event) {\n     event.preventDefault();         \n     $(this).closest('.entry').dbBegin().dbSave().dbQuery(gItemSaveQuery,\n       function(data) {\n         console.log('saved entry', data);\n         if (!data.errors || !data.errors.length) { //query action returns errors             \n             var textarea = $(this).find('.entry-edit textarea');         \n             textarea.data('originalValue', textarea.val());         \n             setModified($(this), false);\n             toggleEditEntry.call(this);\n             if (data.results.length) {\n                $('.item-contents', this).html(data.results[0].html);     \n                $('.item-contents a', this).click(function(event) { event.stopPropagation(); } );        \n            }\n          }\n        }).dbCommit();\n   });\n\n   $('.entry-delete-button', context).click(function(event) {\n     event.preventDefault();       \n     if (!confirm('delete this entry?'))\n        return;\n     var entry = $(this).closest('.entry');\n     //XXX really delete!\n     var data = { id : entry.attr('itemid'), type : '@deleted-post'};\n     entry.dbSave(data, function(data) {\n        if (!data.errors) {\n            loadPanel();\n        }\n     });\n   });\n\n   /** end generic panel/item code **/  \n   ")
        # SOURCE LINE 392
        __M_writer(filters.decode.utf8(caller.body()))
        __M_writer(u"\n}\n\n$(document).ready(function(){\n    initItems( document );\n    makePanels('.panel', ")
        # SOURCE LINE 397
        __M_writer(filters.decode.utf8(refreshPanel))
        __M_writer(u');\n     $(".yui-b").sortable({ tolerance: \'pointer\', connectWith: \'.yui-b\', handle : \'.panel-header\' });\n    //see http://www.b-hind.eu/jquery/index.php    \n});\n')
        return ''
    finally:
        context.caller_stack._pop_frame()


def render_panelCss(context):
    context.caller_stack._push_frame()
    try:
        __M_writer = context.writer()
        # SOURCE LINE 103
        __M_writer(u'\n/** begin panel and item components **/\n\n.panel {\n    padding: 0px 2em .2em .5em;\n    margin: .5em 0 .5em 0;\n/*\n    background-color: lightblue;\n    -moz-border-radius: 5px;\n    -webkit-border-radius: 5px;\n    -moz-box-shadow: 3px 3px 5px rgba(0, 0, 0, 0.5);\n    -webkit-box-shadow: 3px 3px 5px rgba(0, 0, 0, 0.5);\n*/\n}\n\n.panel-header {\n    cursor: move;\n    \n    background-color: orange;\n\n    -moz-border-radius-topleft: 5px;\n    -moz-border-radius-topright: 5px;    \n    -webkit-border-top-left-radius: 5px;\n    -webkit-border-top-right-radius: 5px;        \n\n    -moz-box-shadow: 3px 3px 5px rgba(0, 0, 0, 0.5);\n    -webkit-box-shadow: 3px 3px 5px rgba(0, 0, 0, 0.5);\n\n}\n\n.panel-body {\n    background-color: #E2EDF4;\n    padding: 6px;\n    padding-bottom: 12px;    \n    overflow: hidden;\n    -moz-border-radius-bottomleft: 5px;\n    -moz-border-radius-bottomright: 5px;    \n    -webkit-border-bottom-left-radius: 5px;\n    -webkit-border-bottom-right-radius: 5px;        \n\n    -moz-box-shadow: 3px 3px 5px rgba(0, 0, 0, 0.5);\n    -webkit-box-shadow: 3px 3px 5px rgba(0, 0, 0, 0.5);\n}\n\n.ui-sortable-placeholder { border: 1px dotted black; \n    visibility: visible !important; \n    height: 50px !important; \n    background-color: #EEEEEE;\n}\n\n.ui-sortable-placeholder * { visibility: hidden; }\n\n.panel-header .ui-icon { \n    float: left;\n    cursor: pointer;\n}\n\n.panel .ui-resizable-s {\n bottom:7px;\n height:12px;\n cursor: row-resize;\n}\n\n.item {\n    min-height: 3em;\n}\n\n.item-bullet {\n   float: left; \n   padding-right: .25em;\n   font-size: 1em;\n}\n\n.item-properties {\n    float: right;\n    white-space: normal;\n    -moz-border-radius-topleft: 3px;\n    -moz-border-radius-bottomleft: 10px;\n    -webkit-border-top-left-radius: 3px;\n    -webkit-border-bottom-left-radius: 10px;\n    padding: 0px 0px .2em .5em;\n    max-width: 6em;\n    max-width: 12em;\n    /* background-color: lightgrey; */\n}\n\n.item-contents { display: inline; }\n\n.altband {\n    background-color: white;\n}\n\n/** end panel and item components **/\n')
        return ''
    finally:
        context.caller_stack._pop_frame()


