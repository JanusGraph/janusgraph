define(
    [
        "rexster/server"
    ],
    function (server) {

        var rexsterMimeType = "application/vnd.rexster-v1+json";
        var baseUri = "";

        // currently only one of these
        baseUri = server.getBaseUri(0);

        // public methods
        return {

            /**
             * Get a list of graphs.
             *
             * @param onSuccess	{Function} The action that occurs on a successful REST call.
             * @param onFail 	{Function} The action that occurs on a failed REST call.
             */
            getGraphs : function(onSuccess, onFail){
                $.ajax({
                      url: baseUri,
                      accepts:{
                        json: rexsterMimeType
                      },
                      type: "GET",
                      dataType:"json",
                      success: onSuccess,
                      error: onFail
                    });
            },

            /**
             * Get a specific graph.
             *
             * @param graphName {String} The name of the graph.
             * @param onSuccess	{Function} The action that occurs on a successful REST call.
             * @param onFail 	{Function} The action that occurs on a failed REST call.
             */
            getGraph : function(graphName, onSuccess, onFail){
                $.ajax({
                      url: baseUri + graphName,
                      accepts:{
                        json: rexsterMimeType
                      },
                      type: "GET",
                      dataType:"json",
                      success: onSuccess,
                      error: onFail
                    });
            },

            /**
             * Get a list of vertices for a specific graph.
             *
             * @param graphName {String} the name of the graph.
             * @param start		{int} The first vertex to return in the set.
             * @param end		{int} The last vertex to return in the set.
             * @param onSuccess	{Function} The action that occurs on a successful REST call.
             * @param onFail 	{Function} The action that occurs on a failed REST call.
             */
            getVertices : function(graphName, start, end, key, value, onSuccess, onFail){
                var uri = baseUri + graphName + "/vertices?rexster.offset.start=" + start + "&rexster.offset.end=" + end;
                if (key != null && value != null) {
                    uri = uri + "&key=" + key + "&value=" + value;
                }

                $.ajax({
                      url: uri,
                      accepts:{
                        json: rexsterMimeType
                      },
                      type: "GET",
                      dataType:"json",
                      success: onSuccess,
                      async:false,
                      error: onFail
                    });
            },

            /**
             * Get a list of edges for a specific graph.
             *
             * @param graphName {String} the name of the graph.
             * @param start		{int} The first edge to return in the set.
             * @param end		{int} The last edge to return in the set.
             * @param onSuccess	{Function} The action that occurs on a successful REST call.
             * @param onFail 	{Function} The action that occurs on a failed REST call.
             */
            getEdges : function(graphName, start, end, key, value, onSuccess, onFail){
                var uri = baseUri + graphName + "/edges?rexster.offset.start=" + start + "&rexster.offset.end=" + end;
                if (key != null && value != null) {
                    uri = uri + "&key=" + key + "&value=" + value;
                }

                $.ajax({
                      url: uri,
                      accepts:{
                        json: rexsterMimeType
                      },
                      type: "GET",
                      dataType:"json",
                      success: onSuccess,
                      async:false,
                      error: onFail
                    });
            },

            getVertexEdges : function(graphName, vertex, onSuccess, onFail, asynchronous) {
                $.ajax({
                      url: baseUri + graphName + "/vertices/" + encodeURIComponent(vertex) + "/bothE",
                      accepts:{
                        json: rexsterMimeType
                      },
                      type: "GET",
                      dataType:"json",
                      success: onSuccess,
                      async:asynchronous,
                      error: onFail
                    });
            },

            getVertexInEdges : function(graphName, vertex, onSuccess, onFail, asynchronous) {
                $.ajax({
                      url: baseUri + graphName + "/vertices/" + encodeURIComponent(vertex) + "/inE",
                      accepts:{
                        json: rexsterMimeType
                      },
                      type: "GET",
                      dataType:"json",
                      success: onSuccess,
                      async:asynchronous,
                      error: onFail
                    });
            },

            getVertexOutEdges : function(graphName, vertex, onSuccess, onFail, asynchronous) {
                $.ajax({
                      url: baseUri + graphName + "/vertices/" + encodeURIComponent(vertex) + "/outE",
                      accepts:{
                        json: rexsterMimeType
                      },
                      type: "GET",
                      dataType:"json",
                      success: onSuccess,
                      async:asynchronous,
                      error: onFail
                    });
            },

            getVertexElement : function(graphName, vertex, onSuccess, onFail, asynchronous) {
                $.ajax({
                      url: baseUri + graphName + "/vertices/" + encodeURIComponent(vertex),
                      accepts:{
                        json: rexsterMimeType
                      },
                      type: "GET",
                      dataType:"json",
                      success: onSuccess,
                      async:asynchronous,
                      error: onFail
                    });
            },

            getEdgeElement : function(graphName, edge, onSuccess, onFail, asynchronous) {
                $.ajax({
                      url: baseUri + graphName + "/edges/" + encodeURIComponent(edge),
                      accepts:{
                        json: rexsterMimeType
                      },
                      type: "GET",
                      dataType:"json",
                      success: onSuccess,
                      async:asynchronous,
                      error: onFail
                    });
            },

            getVertexBoth : function(graphName, vertex, onSuccess, onFail, asynchronous) {
                $.ajax({
                      url: baseUri + graphName + "/vertices/" + encodeURIComponent(vertex) + "/both",
                      accepts:{
                        json: rexsterMimeType
                      },
                      type: "GET",
                      dataType:"json",
                      success: onSuccess,
                      async:asynchronous,
                      error: onFail
                    });
            },

            getKeyIndices : function(graphName, keyType, onSuccess, onFail, asynchronous) {
                $.ajax({
                      url: baseUri + graphName + "/keyindices/" + keyType,
                      accepts:{
                        json: rexsterMimeType
                      },
                      type: "GET",
                      dataType:"json",
                      success: onSuccess,
                      async:asynchronous,
                      error: onFail
                    });
            }
        };
    });