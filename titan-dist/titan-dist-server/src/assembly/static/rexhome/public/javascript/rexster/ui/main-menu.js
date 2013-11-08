define(
    [
        "rexster/history",
        "rexster/template/template",
        "rexster/ui/graph",
        "rexster/ui/terminal",
        "jquery-ui",
        "elastic"
    ],
    function (history, template, graph, terminal) {
        var currentPanel = null;

        function slideGraphPanel(newState) {
            var options = { direction:"right" },
                different = isPanelDifferent("mainGraph");

            // hide the current panel as a new one has been selected
            if (currentPanel != undefined && currentPanel != null && different) {
                currentPanel.hide("slide", options, 500);
            }

            if (newState != undefined) {
                history.historyPush(newState);
            }

            // show the graph panel
            graph.initGraphList(function(){
                if (different) {
                    currentPanel = $("#mainGraph");
                    $("#footer").fadeOut();
                    $("#slideHolder").prepend(currentPanel);
                    currentPanel.delay(500).show("slide", null, function() {
                        $("#footer").fadeIn();
                        Elastic.refresh();
                    });
                }
            });
        }

        function slideGremlinPanel(newState) {
            var options = { direction:"right" },
            different = isPanelDifferent("mainGremlin");;

            // hide the current panel as a new one has been selected
            if (currentPanel != undefined && currentPanel != null && different) {
                currentPanel.hide("slide", options);
            }

            if (newState != undefined) {
                history.historyPush(newState);
            }

            terminal.initTerminal(function(){
                if (different) {
                    currentPanel = $("#mainGremlin");
                    $("#footer").fadeOut();
                    $("#slideHolder").prepend(currentPanel);
                    currentPanel.delay(500).show("slide", null, function() {
                        $("#footer").fadeIn();
                        Elastic.refresh();
                    });
                }
            });
        }

        function isPanelDifferent(newPanel) {
            var diff = true;

            if (currentPanel != undefined && currentPanel != null) {
                diff = currentPanel.attr("id") != newPanel;
            }

            return diff;
        }

        // public methods
        return {
	
            initMainMenu : function(){

                var menuItems = [
                                  {"id":"Dashboard", "menuName":"Dashboard", "disabled":true},
                                  {"id":"Graph", "menuName":"Browse", "checked":true},
                                  {"id":"Gremlin", "menuName":"Gremlin"}
                                ],
                     state = {};

                state = history.getApplicationState();

                $("#radiosetMainMenu").empty();

                if (state != undefined && state.menu != undefined) {
                    if (state.menu == "graph") {
                        menuItems[1].checked = true;
                        menuItems[2].checked = false;
                    } else if (state.menu == "gremlin") {
                        menuItems[1].checked = false;
                        menuItems[2].checked = true;
                    }
                }

                template.applyMainMenuTemplate(menuItems, "#radiosetMainMenu");

                $("#radiosetMainMenu").buttonset();

                $("#radioMenuGraph").unbind("click");
                $("#radioMenuGraph").click(function() {
                    slideGraphPanel("/doghouse/main/graph");
                    return false;
                });

                $("#radioMenuGremlin").unbind("click");
                $("#radioMenuGremlin").click(function() {
                    slideGremlinPanel("/doghouse/main/gremlin");
                    return false;
                });

                if (state.menu === "graph") {
                    if (state.hasOwnProperty("graph")) {
                        // the state is defined for the graph so no need to initialize
                        slideGraphPanel();
                    } else {
                        // no graph is set so let it default the first one
                        $("#radioMenuGraph").click();
                    }
                } else if (state.menu === "gremlin") {
                    if (state.hasOwnProperty("graph")) {
                        slideGremlinPanel();
                    } else {
                        $("#radioMenuGremlin").click();
                    }
                }
            }
        };
});