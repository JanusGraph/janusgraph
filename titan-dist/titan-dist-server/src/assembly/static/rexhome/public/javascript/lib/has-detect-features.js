(function(has, addtest, cssprop, undefined){

    var STR = "string",
        FN = "function"
    ;

    // FIXME: isn't really native
    // miller device gives "[object Console]" in Opera & Webkit. Object in FF, though. ^pi
    addtest("native-console", function(g){
        return ("console" in g);
    });

    addtest("native-xhr", function(g){
        return has.isHostType(g, "XMLHttpRequest");
    });

    addtest("native-cors-xhr", function(g){
        return has("native-xhr") && ("withCredentials" in new XMLHttpRequest);
    });

    addtest("native-xhr-uploadevents", function(g){
        return has("native-xhr") && ("upload" in new XMLHttpRequest);
    });

    addtest("activex", function(g){
        return has.isHostType(g, "ActiveXObject");
    });

    addtest("activex-enabled", function(g){
        var supported = null;
        if(has("activex")){
            try{
                supported = !!new ActiveXObject("htmlfile");
            }catch(e){
                supported = false;
            }
        }
        return supported;
    });

    addtest("native-navigator", function(g){
        return ("navigator" in g);
    });

    /**
     * Geolocation tests for the new Geolocation API specification:
     * This test is a standards compliant-only test; for more complete
     * testing, including a Google Gears fallback, please see:
     *   http://code.google.com/p/geo-location-javascript/
     * or view a fallback solution using google's geo API:
     *   http://gist.github.com/366184
     */
    addtest("native-geolocation", function(g){
        return has("native-navigator") && ("geolocation" in g.navigator);
    });

    addtest("native-crosswindowmessaging", function(g){
        return ("postMessage" in g);
    });

    addtest("native-orientation",function(g){
        return ("ondeviceorientation" in g);
    });

    /**
     * not sure if there is any point in testing for worker support
     * as an adequate fallback is impossible/pointless
     *
     * ^rw
     */
    addtest("native-worker", function(g){
        return ("Worker" in g);
    });

    addtest("native-sharedworker", function(g){
        return ("SharedWorker" in g);
    });

    addtest("native-eventsource", function(g){
        return ("EventSource" in g);
    });

    // non-browser specific
    addtest("eval-global-scope", function(g){
        var fnId = "__eval" + Number(new Date()),
            supported = false;

        // catch indirect eval call errors (i.e. in such clients as Blackberry 9530)
        try{
            g.eval("var " + fnId + "=true");
        }catch(e){}

        supported = (g[fnId] === true);
        if(supported){
            try{
                delete g[fnId];
            }catch(e){
                g[fnId] = undefined;
            }
        }
        return supported;
    });

    // in chrome incognito mode, openDatabase is truthy, but using it
    //   will throw an exception: http://crbug.com/42380
    // we create a dummy database. there is no way to delete it afterwards. sorry.
    addtest("native-sql-db", function(g){
        var dbname = "hasjstestdb",
            supported = ("openDatabase" in g);

        if(supported){
            try{
                supported = !!openDatabase( dbname, "1.0", dbname, 2e4);
            }catch(e){
                supported = false;
            }
        }
        return supported;
    });

    // FIXME: hosttype
    // FIXME: moz and webkit now ship this prefixed. check all possible prefixes. ^pi
    addtest("native-indexeddb", function(g){
        return ("indexedDB" in g);
    });


    addtest("native-localstorage", function(g){
      //  Thanks Modernizr!
      var supported = false;
      try{
        supported = ("localStorage" in g) && ("setItem" in localStorage);
      }catch(e){}
      return supported;
    });

    addtest("native-sessionstorage", function(g){
      //  Thanks Modernizr!
      var supported = false;
      try{
        supported = ("sessionStorage" in g) && ("setItem" in sessionStorage);
      }catch(e){}
      return supported;
    });

    addtest("native-history-state", function(g){
        // added the "onpopstate" check because it doesn't appear to be present in FF4
        // even though this says otherwise https://developer.mozilla.org/en/DOM/window.onpopstate
        // am i misreading something???
        return ("history" in g) && ("pushState" in history) && ("onpopstate" in g);
    });

    addtest("native-websockets", function(g){
        return ("WebSocket" in g);
    });

})(has, has.add, has.cssprop);