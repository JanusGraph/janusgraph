/**
 * Manages the Gremlin Terminal. Migrated from Webling (https://github.com/xedin/webling).  
 * 
 * Credit to Neo Technology (http://neotechnology.com/) for most of the code related to the 
 * Gremlin Terminal in Rexster.  Specifically, this code was borrowed from 
 * https://github.com/neo4j/webadmin and re-purposed for Rexster's needs.
 * 
 * Refitted from webling - [https://github.com/xedin/webling]
 * Original header comments below. 
 * 
 * TryMongo
 * Original version from: Kyle Banker (http://www.kylebanker.com)
 * Rerewritten to fit gremlin needs by: Pavel A. Yaskevich
 * Date: September 1, 2009
 * (c) Creative Commons 2010
 * http://creativecommons.org/licenses/by-sa/2.5/
 */

// Readline class to handle line input.
var ReadLine = function(options, history) {
  this.options      = options || {};
  this.htmlForInput = this.options.htmlForInput;
  this.inputHandler = function(h, v, scope) { 
	/*
    if(v == 'help') {
      h.insertResponse('Coming soon');
      h.newPromptLine();
      return null;
    }
    */
    
    req = '';

    if(scope == true) {
      for(i = 0; i < h.scopeHistory.length; i++) {
          req += h.scopeHistory[i] + "\n";
      }
      req += "end\n";
    } else {
      req = v;
    }

    var state = history.getApplicationState();
    $.ajax({
            data: { code : req, "g" : state.graph },
            type: "POST",
            url: '/doghouse/exec',
            contentType: "application/x-www-form-urlencoded;charset=UTF-8",
            success: function(value) {
              h.insertResponse(value.replace(/\n/g, "<br />"));

              // Save to the command history...
              if((lineValue = $.trim(v)) !== "") {
                h.history.push(lineValue);
                h.historyPtr = h.history.length;
              }

              h.scopeHistory = [];
              h.newPromptLine();
            }});
  };

  this.terminal     = $(this.options.terminalId || "#terminal");
  this.lineClass    = this.options.lineClass || '.readLine';
  this.history      = [];
  this.historyPtr   = 0;
  this.scopeHistory = [];
  this.initialize();
};

ReadLine.prototype = {
  initialize: function() {
    this.addInputLine();
  },

  newPromptLine: function() {
    this.activeLine.value = '';
    this.activeLine.attr({disabled: true});
    this.activeLine.next('.spinner').remove();
    this.activeLine.removeClass('active');
    this.addInputLine(this.depth);
  },

  // Enter a new input line with proper behavior.
  addInputLine: function(depth) {
    stackLevel = depth || 0;
    this.terminal.append(this.htmlForInput(stackLevel));
    var ctx = this;
    ctx.activeLine = $(this.lineClass + '.active');

    // Bind key events for entering and navigating history.
    ctx.activeLine.bind("keydown", function(ev) {
      switch (ev.keyCode) {
        case EnterKeyCode:
          ctx.processInput(this.value); 
          break;
        case UpArrowKeyCode: 
          ctx.getCommand('previous');
          ev.preventDefault();
          break;
        case DownArrowKeyCode: 
          ctx.getCommand('next');
          ev.preventDefault();
          break;
      }
    });

    $(document).bind("keydown", function(ev) {
      ctx.activeLine.focus();
    });

    this.activeLine.focus();
  },

  // Returns the 'next' or 'previous' command in this history.
  getCommand: function(direction) {
    if(this.history.length === 0) {
      return;
    }
    this.adjustHistoryPointer(direction);
    this.activeLine[0].value = this.history[this.historyPtr];

    var lenOfCommand = this.activeLine[0].value.length;
    $(this.activeLine[0]).selectRange(lenOfCommand, lenOfCommand);
  },

  // Moves the history pointer to the 'next' or 'previous' position. 
  adjustHistoryPointer: function(direction) {
    if(direction == 'previous') {
      if(this.historyPtr - 1 >= 0) {
        this.historyPtr -= 1;
      }
    } else {
      if(this.historyPtr + 1 < this.history.length) {
        this.historyPtr += 1;
      }
    }
  },

  // Return the handler's response.
  processInput: function(value) {
    if($.trim(value) == '') {
      this.newPromptLine();
      return null;
    }

    /*
    if($.trim(value) == 'end') {
      this.depth--;
      if(this.depth == 0) { 
        this.inputHandler(this, value, true);
        return false;
      }
    }
    */

    this.scopeHistory.push(value);
    
    this.inputHandler(this, value);
  },

  insertResponse: function(response) {
    if(response !== "") {
      this.activeLine.parent().append("<p class='response'>" + response + "</p>");
    }
  },

  // Simply return the entered string if the user hasn't specified a smarter handler.
  mockHandler: function(inputString) {
    return function() {
      this._process = function() { return inputString; };
    };
  }
};

$htmlFormat = function(obj) {
  return tojson(obj, ' ', ' ', true);
}

$.fn.selectRange = function(start, end) {
    return this.each(function() {
        if(this.setSelectionRange) {
            this.focus();
            this.setSelectionRange(start, end);
        } else if(this.createTextRange) {
            var range = this.createTextRange();
            range.collapse(true);
            range.moveEnd('character', end);
            range.moveStart('character', start);
            range.select();
        }
    });
};

var DefaultInputHtml = function(stack) {
    var linePrompt = "";
    for(var i=0; i <= stack; i++) {
      linePrompt += "<span class='prompt'>gremlin&gt;</span>";
    }
    return "<div class='line'>" +
           linePrompt +
           "<input type='text' class='readLine active' />" +
           "<img class='spinner' src='/doghouse/img/spinner.gif' style='display:none;' /></div>";
}

var EnterKeyCode      = 13;
var UpArrowKeyCode    = 38;
var DownArrowKeyCode  = 40;

define(
    [
        "rexster/history",
        "rexster/ajax",
        "rexster/template/template",
        "rexster/history"
    ],
    function (history, ajax, template, history) {
        // public methods
        return {
            initTerminal : function(onInitComplete){
                $("#panelGremlinMenuGraph").empty();
                $("#terminal .line").remove();

                $("#gremlinVersion").text("Gremlin " + GREMLIN_VERSION);

                ajax.getGraphs(function(result){

                    var ix = 0,
                        max = 0,
                        graphs = [],
                        state = history.getApplicationState();

                    // construct a list of graphs that can be pushed into the graph menu
                    max = result.graphs.length;
                    for (ix = 0; ix < max; ix += 1) {
                        graphs.push({ "menuName": result.graphs[ix], "panel" : "gremlin" });
                    }

                    template.applyMenuGraphTemplate(graphs, $("#panelGremlinMenuGraph"));

                    $("#panelGremlinMenuGraph").find("div").unbind("hover");
                    $("#panelGremlinMenuGraph").find("div").hover(function() {
                        $(this).toggleClass("ui-state-hover");
                    });

                    $("#panelGremlinMenuGraph").find("div").unbind("click");
                    $("#panelGremlinMenuGraph").find("div").click(function(evt) {
                        evt.preventDefault();

                        $("#panelGremlinMenuGraph").find(".graph-item").removeClass("ui-state-active");
                        $(this).addClass("ui-state-active");

                        var selectedLink = $(this).find("a");
                        var uri = selectedLink.attr('href');
                        history.historyPush(uri);

                    });

                    // check the state, if it is at least two items deep then the state
                    // of the graph is also selected and an attempt to make the graph active
                    // should be made.
                    if (state.hasOwnProperty("graph")) {
                        $("#panelGremlinMenuGraph").find(".graph-item").removeClass("ui-state-active");
                        $("#panelGremlinMenuGraph").find("#graphItemgremlin" + state.graph).addClass("ui-state-active");

                        if (onInitComplete != undefined) {
                            onInitComplete();
                        }
                    }

                    // if the state does not specify a graph then select the first one.
                    if (!state.hasOwnProperty("graph")) {
                        $("#panelGremlinMenuGraph").find("#graphItemgremlin" + graphs[0].menuName).click();
                        if (onInitComplete != undefined) {
                            onInitComplete();
                        }
                    }

                    var terminal = new ReadLine({htmlForInput: DefaultInputHtml}, history);
                });
            }
        };
    });
