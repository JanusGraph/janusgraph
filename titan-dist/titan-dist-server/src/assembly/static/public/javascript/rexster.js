require(
    [
        "domReady",
        "rexster/history",
        "rexster/template/template",
        "rexster/ui/main-menu",
        "order!has",
        "order!has-detect-features"
    ],
    function (domReady, history, template, mainMenu) {
        domReady(function () {

            // only make this feature available to browsers that support it
            if (has("native-history-state")) {
                window.onpopstate = function(event) {
                    var popped = ('state' in window.history), initialURL = location.href
                    $(window).bind('popstate', function(event) {
                      // Ignore inital popstate that some browsers fire on page load
                      var initialPop = !popped && location.href == initialURL
                      popped = true
                      if ( initialPop ) return

                        restoreApplication();
                    });
                };
            }

            function restoreApplication() {
                // compile the templates
                template.initTemplates();

                // build the main menu.  this action will initialize the
                // first enabled panel
                mainMenu.initMainMenu();
            }


            // determine if the state is already established
            var state = history.getApplicationState();
            if (!state.hasOwnProperty("menu")) {
                // since there is no menu selected initialized the graph page first.
                history.historyPush("/doghouse/main/graph");
            }

            restoreApplication();
        });
    });