/** 
 * Credit to http://code.google.com/p/jquery-jsonviewer-plugin/ as the basis for this JSON Viewer
 */
(function($) {

    $.fn.jsonviewer = function(settings) {

        var config =
        {
            'typePrefix': false,
            'jsonName': 'unknown',
            'jsonData': null,
            'ident' : '12px',
            'innerPadding': '2px',
            'outerPadding': '4px',
            'debug' : false,
            'showId' : false,
            'overrideCss' : {
                'highlight':'json-widget-highlight',
                'header':'json-widget-header',
                'content' :'json-widget-content'
            },
            'highlight' : true,
            'showToolbar' : true
        };
        
        if (settings) $.extend(config, settings);
        
        this.each(function(key, element) {
            format_value(element, config['jsonName'], config['jsonData'], config, config['showToolbar']);
        });
        
        return this;

    };

    function format_value(element, name, data, config, showToolbar) {
        //debug('name=' + name + "; data=" + data);

        // we don't want to render metadata in the json viewer.  these are standard rexster properties that
        // would muddy up the view.  one exception is $alpha which seems to be a jit related bit of meta data
        // that gets injected at render time of the visualization.
    	var isMetaData = name === "_type" || (name === "_id" && !config['showId']) || name === "_outV" || name === "_inV" || name === "_label" || name === "$alpha";
    	if (!isMetaData) {
	        var v = new TypeHandler(data);
	        var typePrefix = v.type().charAt(0);
	        var container = $('<div/>');
	        $(container).appendTo(element);
	        $(container).addClass('json-widget').css({'padding': config['outerPadding'], 'padding-left': config['ident'] });
	        
	        // highlight on hover
	        if (config.highlight === true) {
                $(container).hover(function(event) {
                    $(container).children().toggleClass(config.overrideCss.highlight);
                });
            }
	        
	        if (v.type() == "object" || v.type() == "array") {
	        	var header = $('<div/>');
		        $(header).appendTo(container);
		        $(header).addClass(config.overrideCss.header + ' ui-corner-top')
		            .css({ 'cursor': 'hand', //'float': 'left',
		                'text-align': 'left'
		            });
		        $(header).text('' + (config['typePrefix'] ? "(" + typePrefix + ")" : "") + name);
	        	
		        $(header).click(function(event) {
		        	$(header).next().toggleClass('ui-helper-hidden');
		            return false;
		        });
		        
	            var content = $('<div/>');
	            $(content).appendTo(container);
	            $(content).addClass(config.overrideCss.content + ' ui-corner-bottom')
	            	.css({ 'white-space': 'nowrap', 'padding': config['innerPadding'] });
	            for (name in data) { format_value(content, name, data[name], config, false); }
	            
	            if (showToolbar && config.toolbar != undefined) {
			        $(config.toolbar).appendTo(content);
	            }
	        }
	        else {
	            var content = $('<div/>');
	            $(content).appendTo(container);
	            $(content).css({ 'overflow': 'hidden', 'white-space': 'nowrap' });
	            $(content).text(name + ': ' + data);
	        }
    	}
    };


    // number, boolean, string, object, array, date
    function TypeHandler(value) {
        this._type = this.get_type(value);
    };

    TypeHandler.prototype.type = function() { return this._type; }

    TypeHandler.prototype.get_type = function(value) {
        var base_type = typeof value;
        var result = "unsupported"
        switch (base_type) {
            case "number": result = base_type; break;
            case "string": result = base_type; break;
            case "boolean": result = base_type; break;
            case "object":
                if (Number.prototype.isPrototypeOf(value)) { result = "number"; break; }
                if (String.prototype.isPrototypeOf(value)) { result = "string"; break; }
                if (Date.prototype.isPrototypeOf(value)) { result = "date"; break; }
                if (Array.prototype.isPrototypeOf(value)) { result = "array"; break; }
                if (Object.prototype.isPrototypeOf(value)) { result = "object"; break; }
        }
        return result;
    };

    //
    // private function for debugging
    //
    function debug(msg) {
        if (window.console && window.console.log)
            window.console.log('debug(jv): ' + msg);
    };
})(jQuery);