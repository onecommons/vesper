var Assert = YAHOO.util.Assert;
var ArrayAssert = YAHOO.util.ArrayAssert;

function assertJsonEqual(o1, o2) {
  Assert.areEqual(JSON.stringify(o1), JSON.stringify(o2));
}

/** Asserts raise an error when they fail and asyncronous won't be running 
 *  within the TestRunner try/catch block. 
 *  offthread() runs code with assertions its own try/catch block and
 *  passes the exception to TestCase.resume() if it fails (which terminates the TestCase)
 *  
 *  @param done: If true, call resume() after executing
 */
YAHOO.tool.TestCase.prototype.offthread = function(func, done) {
  try {
    func();
    if (done) {
        this.resumeOffthread();
    }        
  } catch (e) {
    if (window.console) console.log('offset thread error', e);
    this.offthreadError = e;
    this.resumeOffthread();
  }
};

YAHOO.tool.TestCase.prototype.resumeOffthread = function() {
  if (!YAHOO.tool.TestRunner._cur || this !== YAHOO.tool.TestRunner._cur.parent.testObject) {
    //this can happen if a Assert failed thus aborting the current testcase before this callback runs
    if (window.console) console.log('wait already ran', YAHOO.tool.TestRunner);
    return;
  } else {
    if (window.console) console.log('resumeOffthread called', YAHOO.tool.TestRunner);
  }        
  var testcase = this;
  //call resume() to switch back to the test runner's main thread (which is blocked in wait())
  this.resume(function() {
     var offthreadError = testcase.offthreadError;
     if (window.console) console.log('resumeOffThread running', offthreadError);
     if (!YAHOO.lang.isUndefined(testcase.offthreadError)) 
        delete testcase.offthreadError;
     Assert.isUndefined(offthreadError);         
  });
};

YAHOO.tool.TestCase.prototype.checkResponse = function(data, shouldSucceed, failMsg, expectedErrorCode) {
  this.offthread(function() {
     if (shouldSucceed) {
       var msg = 'checkResponse: unexpected error ' + failMsg;
       if (window.console) {
          if (!YAHOO.lang.isUndefined(data.error)) 
            console.log(msg, data);
       }
       Assert.isUndefined(data.error, msg);
     } else {
       var msg = 'checkResponse: unexpected success ' + failMsg;
       if (window.console) {
         if (!YAHOO.lang.isObject(data.error)) 
            console.log(msg, data);
       }
       Assert.isObject(data.error, msg);
       if (expectedErrorCode) {
          Assert.areEqual(data.error.code, expectedErrorCode, 
              'checkResponse: unexpected error code: ' + data.error.code);
       }
     }
  });
};
    
