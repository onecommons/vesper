$.event.real_handle = $.event.handle;
$.event._last_event = {};
$.event.handle = function(event) {
  if (localStorage.getItem('recorder.recording') && $.event._last_event[event.type] !== event) {
    $.event._last_event[event.type] = event;
    var jEvent = $.event.fix( event || window.event );
    //if (window.console) console.log('ev', jEvent.type, arguments);
    if (/^mouse(over|out|down|up|move)|(dbl)?click|key(up|down|press)|focus$/.test(jEvent.type)) {
        gRecorder.writeEvent(jEvent);
    }
  }
  return $.event.real_handle.apply(this, Array.prototype.slice.call(arguments));
};

$(window).bind("keypress keyup keydown", function() {});

window.konsole = {
    superk : window.konsole || window.console,
    
    log : function(msg) {
        var args = Array.prototype.slice.call(arguments);
        if (this.superk)
            this.superk.log.apply(this.superk, args);

        var msg = args.length != 1 && args || args[0];
        try {
            JSON.stringify(msg); //make sure this will succeed later
        } catch (e) {
            if (window.console) console.log('could not stringify log message:', args);
            return;
        }
        
        if (localStorage.getItem('recorder.recording')) {
            gRecorder.writeLogAssertion(msg);
        }
    },
    assert : window.konsole && window.konsole.assert || window.console && window.console.assert || 
      (function(expr, msg) { if (!expr) { debugger; } })
};

var gRecorder = (function() {
  function generateUniqueSelector(path) {
    //derived from http://davecardwell.co.uk/javascript/jquery/plugins/jquery-getpath/    
    // The first time this function is called, path won't be defined.
    if ( typeof path == 'undefined' ) path = '';

    // If this element is <html> we've reached the end of the path.
    if ( this.is('html') )
        return 'html' + path;
    
    // Determine the IDs and path.
    var id  = this.get(0).hasAttribute('id') && this.attr('id');
    if (id)
        return "#" + id + path;
    
    var klass = this.attr('class');

    // Add the element name.
    var cur = this.get(0).nodeName.toLowerCase();

    // Add any classes.
    if ( klass && klass.trim())
        cur += '.' + klass.split(/[\s\n]+/).join('.');

    var siblings = this.parent().children(cur);
    if (siblings.length > 1) { 
        cur += ':eq(' + siblings.index(this[0]) + ')';
    }
    // Recurse up the DOM.
    return generateUniqueSelector.call(this.parent(), ' > ' + cur + path);
  }

  var props = "altKey bubbles button cancelable charCode clientX clientY ctrlKey detail keyCode metaKey screenX screenY shiftKey wheelDelta which".split(" ");        

  var defaults = {
		bubbles: true, detail: 0,
		screenX: 0, screenY: 0, clientX: 0, clientY: 0,
		ctrlKey: false, altKey: false, shiftKey: false, metaKey: false,
		button: 0, keyCode: 0, charCode: 0
   };

 function getPath() { return location.pathname + location.search; }
 
return {
    writeEvent: function(jEvent) {
        var selector = generateUniqueSelector.call( $(jEvent.target) );
        var options = {}; 
        for ( var i = props.length, prop; i; ) {
            prop = props[ --i ];
            if (jEvent[ prop ] != defaults[prop])
                options[ prop ] = jEvent[ prop ];
        }
        //XXX options.view and options.relatedTarget need to be window accessor or name and selector, respectively
        
        gRecorder.sendMsg( { 
            type : "simulate",
            selector : selector,
            eventtype : jEvent.type,
            options : options 
        });
    },

    sendMsg: function(msg) {
        msg.path = getPath();
        localStorage.removeItem('recorder.event');
        localStorage.setItem('recorder.event', JSON.stringify(msg));
        if (window.console) console.log('sending msg', msg.type, msg.eventtype, msg);
    },
    
    writeLine: function(line) {
        gRecorder.sendMsg({type: "scriptline", line : line});
    },

    writeLogAssertion: function(msg) {        
        gRecorder.sendMsg({type: "log", msg: msg});
    }
  };
})();