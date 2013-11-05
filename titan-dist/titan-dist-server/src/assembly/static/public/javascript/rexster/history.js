define(
    [
        "jquery-url"
    ],
    function () {
        var currentState = null;

        // public methods
        return {
	
            /**
             * Pushes a URI into the browser history and parses it to current state.
             */
            historyPush : function(uri) {
                currentState = uri;
                if (has("native-history-state")) {
                    window.history.pushState({"uri":uri}, '', uri);
                }
            },

            /**
             * Gets the current application state given the current URI.
             *
             * It is important that changes to browser history happen prior to getting
             * state as the state is read from the current URI.
             */
            getApplicationState : function() {
                return tryReadStateFromUri();
            }
        };

        function tryReadStateFromUri() {
                var encodedState = jQuery.url.setUrl(location.href),
                    state = {};

                if (!has("native-history-state")) {
                    encodedState = jQuery.url.setUrl(currentState);
                }

                if (encodedState.segment() >= 6) {
                    state.objectId = encodedState.segment(5);
                }

                if (encodedState.segment() >= 5) {
                    state.browse = {
                        element : encodedState.segment(4),
                        start : 0,
                        end : 10,
                        index : {
                            key : null,
                            value : null
                        }
                    };

                    if (encodedState.param("rexster.offset.start") != null && encodedState.param("rexster.offset.end")) {
                        state.browse.start = encodedState.param("rexster.offset.start");
                        state.browse.end = encodedState.param("rexster.offset.end");
                    }

                    if (encodedState.param("rexster.index.key") != null
                        && encodedState.param("rexster.index.value") != null) {
                        state.browse.index.key = encodedState.param("rexster.index.key");
                        state.browse.index.value = encodedState.param("rexster.index.value");
                    }
                }

                if (encodedState.segment() >= 4) {
                    state.graph = encodedState.segment(3);
                }

                if (encodedState.segment() >= 3) {
                    state.menu = encodedState.segment(2);
                }

                return state;
            }
});
	