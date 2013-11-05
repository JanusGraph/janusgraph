define(
    [
        "jit"
    ],
    graphViz = function () {
        // private variables
        var rgraph;
        var graphId;
        var graphLabeler;
        var centerId = null;
        var zoomLevel = 0;

        // constructor
        var Constr = function (id, jsonData, handlers, labeler) {
            graphId = id;
            graphLabeler = labeler;

            // init RGraph
            rgraph = new $jit.RGraph({
                // Where to append the visualization
                injectInto: id,

                // Optional: create a background canvas that plots
                // concentric circles.
                background: {
                    CanvasStyles: {
                        strokeStyle: "#4f3900"
                    }
                },

                // Add navigation capabilities:
                // zooming by scrolling and panning.
                Navigation: {
                    enable: true,
                    panning: true,
                    zooming: 20
                },

                // Set Node and Edge styles.
                Node: {
                    overridable: true,
                    color: "#c79801"
                },

                Edge: {
                    overridable: true,
                    color: "#ffd800",
                    lineWidth: 1.5
                },

                Events: {
                    enable: true,
                    enableForEdges: true,
                    type: "Native",  // Must be "Native" to trigger mouse enter/leave for edges.
                    onClick: function (node, eventInfo, e) {
                        if (typeof node != "boolean") {
                            if (node.nodeFrom) {
                                if (typeof handlers.onEdgeClick != "undefined") {
                                    handlers.onEdgeClick(node.nodeFrom, node.nodeTo);
                                }
                            }
                            else {
                                if (typeof handlers.onNodeClick != "undefined") {
                                    handlers.onNodeClick(node);
                                }
                            }
                        }
                    },
                    onRightClick: function (node, eventInfo, e) {
                        if (typeof node != "boolean") {
                            if (!node.nodeFrom) {
                                if (typeof handlers.onNodeRightClick != "undefined") {
                                    handlers.onNodeRightClick(node);
                                }
                            }
                        }
                    },
                    onMouseWheel: function (delta, e) {
                        zoomLevel += delta;
                    }
                },

                onBeforePlotNode: function (node) {
                    if (typeof graphLabeler != "undefined") {
                        graphLabeler.setNode(node);
                    }
                },

                onBeforePlotLine: function (adj) {
                    if (zoomLevel < 0) {
                        adj.setData("lineWidth", 1.5 * (1 + zoomLevel / 30));
                    }
                },

                // Add the name of the node in the correponding label.
                // This method is called once, on label creation.
                onCreateLabel: function (domElement, node) {
                    domElement.innerHTML = node.name;
                },

                // Change some label dom properties.
                // This method is called each time a label is plotted.
                onPlaceLabel: function (domElement, node) {
                    var style = domElement.style;

                    style.display = "";
                    style.cursor = "default";
                    style.fontSize = "0.8em";
                    style.color = "#ccc";

                    if (node._depth <= 1) {
                        style.fontSize = "0.7em";
                        style.color = "#ccc";

                    } else if(node._depth == 2){
                        style.fontSize = "0.6em";
                        style.color = "#494949";

                    } else {
                        style.fontSize = "0.5em";
                        style.color = "#494949";

                    }

                    if (typeof graphLabeler != "undefined") {
                        graphLabeler.setLabel(style, node);
                    }

                    var left = parseInt(style.left);
                    var w = domElement.offsetWidth;
                    style.left = (left - w / 2) + "px";
                },

                onComplete: function () {
                    if (centerId != null) {
                        rgraph.onClick(centerId);
                        centerId = null;
                    }
                }
            });

            if (typeof graphLabeler != "undefined") {
                graphLabeler.updateData(jsonData);
            }

            // load JSON data
            rgraph.loadJSON(jsonData);
        };

        Constr.prototype = {
            constructor: graphViz,
            version: "1.0",

            animate: function () {
                if (typeof rgraph != "undefined") {
                    // trigger small animation
                    rgraph.graph.eachNode(function (n) {
                        var pos = n.getPos();
                        pos.setc(-200, -200);
                    });

                    rgraph.compute("end");

                    rgraph.fx.animate({
                        modes: ["polar"],
                        duration: 2000
                    });
                }
            },

            sum: function (jsonData) {
                //toolTip.hide();

                if (typeof graphLabeler != "undefined") {
                    graphLabeler.updateData(jsonData);
                }

                if (typeof rgraph != "undefined") {
                    rgraph.op.sum(jsonData, {
                        type: "fade:seq",
                        duration: 1000,
                        hideLabels: true,
                        transition: $jit.Trans.Quart.easeOut
                    });
                }
            },

            center: function (id) {
                if (typeof rgraph != "undefined") {
                    rgraph.onClick(id);
                }
            },

            centerOnComplete: function (id) {
                centerId = id;
            },

            reset: function () {
                $('#' + graphId).empty();

                if (typeof rgraph != "undefined") {
                    rgraph.canvas.clear();
                }

                centerId = null;

                if (typeof graphLabeler != "undefined") {
                    graphLabeler.reset();
                }
            },

            getCenterNode: function () {
                var centerNode = rgraph.graph.getClosestNodeToOrigin();
                return centerNode;
            },

            getNodes: function (centerId) {
                if (typeof rgraph != "undefined") {
                    rgraph.graph.computeLevels(centerId);
                    return rgraph.graph.nodes;
                }
            }
        };

        // return the constructor
        // to be assigned to the new namespace
        return Constr;
    }
);