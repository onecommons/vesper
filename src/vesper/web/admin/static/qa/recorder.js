$.event.real_handle = $.event.handle;
$.event.handle = function(event) {
  if (localStorage.getItem('recorder.recording')) {
    var jEvent = $.event.fix( event || window.event );
    if (/^mouse(over|out|down|up|move)|(dbl)?click|key(up|down|press)$/.test(jEvent.type)) {            
        gRecorder.writeEvent(jEvent);
    }
  }
  return $.event.real_handle.apply(this, Array.prototype.slice.call(arguments));
}

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
            path : getPath(),
            eventtype : jEvent.type,
            options : options 
        });
    },

    sendMsg: function(msg) {
        localStorage.removeItem('recorder.event');
        localStorage.setItem('recorder.event', JSON.stringify(msg));
        console.log(msg.type, msg.eventtype, msg);
    },
    
    writeLine: function(line) {
        gRecorder.sendMsg({type: "scriptline", path: getPath(), line : line});
    },

    writeLogAssertion: function(msg) {
        gRecorder.writeLine('verifyLogMsg(' + JSON.stringify(msg) + ');');
    }
  };
})();