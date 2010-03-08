/*
 * Copyright 2009-2010 by the Vesper team, see AUTHORS.
 * Dual licenced under the GPL or Apache2 licences, see LICENSE.
 */
(function($) {

$.widget("ui.hresize", $.extend({}, $.ui.mouse, {

    _init: function() {

        var self = this, o = this.options;
        this.element.addClass("ui-resizable ui-hresize");

        $.extend(this, {
            originalElement: this.element,
            _helper: null
        });

        var axis = $('<div class="ui-resizable-handle ui-resizable-s"><span class="ui-icon ui-icon-grip-dotted-horizontal" style="margin:2px auto"></span></div>');
        this.element.append(axis); 
        this.handle = axis;
        $('.ui-resizable-handle', this.element).disableSelection();
        $('.ui-resizable-handle .ui-icon', this.element).disableSelection();
        
        //Initialize the mouse interaction
        this._mouseInit();
    },

    destroy: function() {

        this._mouseDestroy();

        var _destroy = function(exp) {
            $(exp).removeClass("ui-resizable ui-hresize ui-resizable-disabled ui-resizable-resizing")
                .removeData("resizable").unbind(".resizable").find('.ui-resizable-handle').remove();
        };

        _destroy(this.originalElement);

        return this;
    },

    _mouseStart: function(event) {

        var o = this.options, iniPos = this.element.position(), el = this.element;

        this.resizing = true;
        this.documentScroll = { top: $(document).scrollTop(), left: $(document).scrollLeft() };

        this.helper = this.element;

        var curleft = num(this.helper.css('left')), curtop = num(this.helper.css('top'));

        if (o.containment) {
            curleft += $(o.containment).scrollLeft() || 0;
            curtop += $(o.containment).scrollTop() || 0;
        }

        //Store needed variables
        this.offset = this.helper.offset();
        this.position = { left: curleft, top: curtop };
        this.size = this._helper ? { width: el.outerWidth(), height: el.outerHeight() } : { width: el.width(), height: el.height() };
        this.originalSize = this._helper ? { width: el.outerWidth(), height: el.outerHeight() } : { width: el.width(), height: el.height() };
        this.originalPosition = { left: curleft, top: curtop };
        this.sizeDiff = { width: el.outerWidth() - el.width(), height: el.outerHeight() - el.height() };
        this.originalMousePosition = { left: event.pageX, top: event.pageY };       

        var cursor = $('.ui-resizable-s').css('cursor');
        $('body').css('cursor', cursor == 'auto' ? 'row-resize' : cursor);

        el.addClass("ui-resizable-resizing");
        return true;        
    },

    _mouseCapture: function(event) {
        if (!this.options.disabled && 
            $(event.target).parent().andSelf().index( $(this.handle)[0]) > -1) {
            return true;
        }
        return false;
    }, 
    
    _mouseDrag: function(event) {
        //Increase performance, avoid regex
        var el = this.helper, o = this.options, props = {},
            self = this, smp = this.originalMousePosition;

        var dx = (event.pageX-smp.left)||0, dy = (event.pageY-smp.top)||0;

        function trigger(event, dx, dy) {
            return { height: this.originalSize.height + dy };
        }

        // Calculate the attrs that will be change
        var data = trigger.apply(this, [event, dx, dy]), ie6 = $.browser.msie && $.browser.version < 7, csdif = this.sizeDiff;

        data = this._respectSize(data, event);

        el.css({
            top: this.position.top + "px", 
            height: this.size.height + "px"
        });

        this._updateCache(data);

        // calling the user callback at the end
        this._trigger('resize', event, this.ui());

        return false;       
    },
    
    _mouseStop: function(event) {

        this.resizing = false;
        var o = this.options, self = this;

        $('body').css('cursor', 'auto');

        this.element.removeClass("ui-resizable-resizing");

        return false;
    },

    _updateCache: function(data) {
        var o = this.options;
        this.offset = this.helper.offset();
        if (isNumber(data.left)) this.position.left = data.left;
        if (isNumber(data.top)) this.position.top = data.top;
        if (isNumber(data.height)) this.size.height = data.height;
        if (isNumber(data.width)) this.size.width = data.width;
    },

    _respectSize: function(data, event) {

        var el = this.helper, o = this.options, 
                ismaxw = isNumber(data.width) && o.maxWidth && (o.maxWidth < data.width), ismaxh = isNumber(data.height) && o.maxHeight && (o.maxHeight < data.height),
                    isminw = isNumber(data.width) && o.minWidth && (o.minWidth > data.width), isminh = isNumber(data.height) && o.minHeight && (o.minHeight > data.height);

        if (isminw) data.width = o.minWidth;
        if (isminh) data.height = o.minHeight;
        if (ismaxw) data.width = o.maxWidth;
        if (ismaxh) data.height = o.maxHeight;

        // fixing jump error on top/left - bug #2330
        var isNotwh = !data.width && !data.height;
        if (isNotwh && !data.left && data.top) data.top = null;
        else if (isNotwh && !data.top && data.left) data.left = null;

        return data;
    },
        
    ui: function() {
        return {
            originalElement: this.originalElement,
            element: this.element,
            helper: this.helper,
            position: this.position,
            size: this.size,
            originalSize: this.originalSize,
            originalPosition: this.originalPosition
        };
    }
    
}));

$.extend($.ui.hresize, {
    version: "1.8a1",
    eventPrefix: "resize",
    defaults: $.extend({}, $.ui.mouse.defaults, {
        animate: false,
        animateDuration: "slow",
        animateEasing: "swing",
        containment: false,
        maxHeight: null,
        maxWidth: null,
        minHeight: 10,
        minWidth: 10,
        zIndex: 1000
    })
});

var num = function(v) {
    return parseInt(v, 10) || 0;
};

var isNumber = function(value) {
    return !isNaN(parseInt(value, 10));
};

})(jQuery);
