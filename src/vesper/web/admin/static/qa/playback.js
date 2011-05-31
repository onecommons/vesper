/*
 * jquery.simulate - simulate browser mouse and keyboard events
 *
 * Copyright 2011, AUTHORS.txt (http://jqueryui.com/about)
 * Dual licensed under the MIT or GPL Version 2 licenses.
 * http://jquery.org/license
 *
 */

 //alternative: https://github.com/FGRibreau/jQuery-plugin-fireEvent (supports blur and change, works around IE 7/8 bug generating click events, probably this one:
 //http://social.msdn.microsoft.com/Forums/en-US/ieextensiondevelopment/thread/e01fa8f0-7b00-4b57-bf64-fd9b952307d4/

;(function($) {

$.fn.extend({
	simulate: function(type, options) {
		return this.each(function() {
			var opt = $.extend({}, $.simulate.defaults, options || {});
			new $.simulate(this, type, opt);
		});
	}
});

$.simulate = function(el, type, options) {
	this.target = el;
	this.options = options;

	if (/^(drag|sendKeys)$/.test(type)) {
		this[type].apply(this, [this.target, options]);
	} else {
		this.simulateEvent(el, type, options);
	}
}

$.extend($.simulate.prototype, {
	simulateEvent: function(el, type, options) {
		var evt = this.createEvent(type, options);
		this.dispatchEvent(el, type, evt, options);
		return evt;
	},
	createEvent: function(type, options) {
		if (/^mouse(over|out|down|up|move)|(dbl)?click$/.test(type)) {
			return this.mouseEvent(type, options);
		} else if (/^key(up|down|press)$/.test(type)) {
			return this.keyboardEvent(type, options);
		}
	},
	mouseEvent: function(type, options) {
		var evt;
		var e = $.extend({
			bubbles: true, cancelable: (type != "mousemove"), view: window, detail: 0,
			screenX: 0, screenY: 0, clientX: 0, clientY: 0,
			ctrlKey: false, altKey: false, shiftKey: false, metaKey: false,
			button: 0, relatedTarget: undefined
		}, options);

		var relatedTarget = $(e.relatedTarget)[0];

		if ($.isFunction(document.createEvent)) {
			evt = document.createEvent("MouseEvents");
			evt.initMouseEvent(type, e.bubbles, e.cancelable, e.view, e.detail,
				e.screenX, e.screenY, e.clientX, e.clientY,
				e.ctrlKey, e.altKey, e.shiftKey, e.metaKey,
				e.button, e.relatedTarget || document.body.parentNode);
		} else if (document.createEventObject) {
			evt = document.createEventObject();
			$.extend(evt, e);
			evt.button = { 0:1, 1:4, 2:2 }[evt.button] || evt.button;
		}
		return evt;
	},
	keyboardEvent: function(type, options) {
		var evt;

		var e = $.extend({ bubbles: true, cancelable: true, view: window,
			ctrlKey: false, altKey: false, shiftKey: false, metaKey: false,
			keyCode: 0, charCode: 0
		}, options);

		if ($.isFunction(document.createEvent)) {
			try {
				evt = document.createEvent("KeyEvents");
				evt.initKeyEvent(type, e.bubbles, e.cancelable, e.view,
					e.ctrlKey, e.altKey, e.shiftKey, e.metaKey,
					e.keyCode, e.charCode);
			} catch(err) {
				evt = document.createEvent("Events");
				evt.initEvent(type, e.bubbles, e.cancelable);
				$.extend(evt, { view: e.view,
					ctrlKey: e.ctrlKey, altKey: e.altKey, shiftKey: e.shiftKey, metaKey: e.metaKey,
					keyCode: e.keyCode, charCode: e.charCode
				});
			}
		} else if (document.createEventObject) {
			evt = document.createEventObject();
			$.extend(evt, e);
		}
		if ($.browser.msie || $.browser.opera) {
			evt.keyCode = (e.charCode > 0) ? e.charCode : e.keyCode;
			evt.charCode = undefined;
		}
		return evt;
	},
    sendKeys: function(el, options) {
        var chars = options.buffer;
        delete options.buffer;
        if (document.activeElement != el) {
            //XXX probably should record and send focus events instead of this
            $(el).focus();
        }
        options = options || {};
        var isString = typeof chars == 'string';
        for (var i=0; i < chars.length; i++) {
            if (isString)
                options.charCode = chars.charCodeAt(i);
            else
                options.charCode = chars[i]; 
            this.simulateEvent(el, 'keypress', options);
        }
    },
	dispatchEvent: function(el, type, evt) {
		if (el.dispatchEvent) {
			el.dispatchEvent(evt);
		} else if (el.fireEvent) {
			el.fireEvent('on' + type, evt);
		}
		return evt;
	},

	drag: function(el) {
		var self = this, center = this.findCenter(this.target), 
			options = this.options,	x = Math.floor(center.x), y = Math.floor(center.y), 
			dx = options.dx || 0, dy = options.dy || 0, target = this.target;
		var coord = { clientX: x, clientY: y };
		this.simulateEvent(target, "mousedown", coord);
		coord = { clientX: x + 1, clientY: y + 1 };
		this.simulateEvent(document, "mousemove", coord);
		coord = { clientX: x + dx, clientY: y + dy };
		this.simulateEvent(document, "mousemove", coord);
		this.simulateEvent(document, "mousemove", coord);
		this.simulateEvent(target, "mouseup", coord);
		this.simulateEvent(target, "click", coord);
	},
	findCenter: function(el) {
		var el = $(this.target), o = el.offset();
		return {
			x: o.left + el.outerWidth() / 2,
			y: o.top + el.outerHeight() / 2
		};
	}
});

$.extend($.simulate, {
	defaults: {
		speed: 'sync'
	},
	VK_TAB: 9,
	VK_ENTER: 13,
	VK_ESC: 27,
	VK_PGUP: 33,
	VK_PGDN: 34,
	VK_END: 35,
	VK_HOME: 36,
	VK_LEFT: 37,
	VK_UP: 38,
	VK_RIGHT: 39,
	VK_DOWN: 40
});

})(jQuery);


(function(window) {
//add to global namespace
window.simulate = function(type, selector, options) {
    if (window.gRecorder && localStorage.getItem('recorder.recording')) {
        console.log('skipping playback cuz in record more');
        return; //don't playback while recording
    }
    try {
        var target = $(selector);
    } catch (e) {
        ok(false, "bad selector: " + selector);
        return;
    }        
    console.log('playback on', type, selector, options, target);
    equal(target.length, 1, selector);
    target.simulate(type, options);
    
    if (type == 'chars') {
        target.sendKeys(chars, options);
    } else {
        
    }
},

window.sendKeys = function(chars, selector, options) {
  options = options || {};
  options.buffer = chars;
  return simulate('sendKeys', selector, options);
},

window.verifyLogMsg = function(msg) {
    console.log('playback verifying', msg);
    playback.msgQueue.push(msg);
    checkLogMessage(msg);
}

//logging pushes message to loggedQueue
//verifyLogMsg pushes message to expectedQueue

//logging checks if message on expectedQueue, pops if found, else pushes to loggedQueue
//verifyLogMsg checks if on loggedQueue, pops if found else pushed to expectedQueue (note: possibly inaccurate if duplicate log message are sent)

window.checkLogMessage = function (msg) {
    if (playback && playback.msgQueue) {
        for (var i = playback.msgQueue.length; i; ) {
            var cur = playback.msgQueue[ --i ];
            if (msg == cur) {
                playback.msgQueue.pop();
                return true;
            }
        }
    }
    return false;
}

})(this);
