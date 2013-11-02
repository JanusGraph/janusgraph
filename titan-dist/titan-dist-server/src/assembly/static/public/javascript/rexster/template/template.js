define(
    [
        "dust"
    ],
    function () {
        var templater = {};

        templater.templateNameMainMenuItem = "mainMenuItem";
        templater.templateNameMenuGraph = "menuGraph";
        templater.templateNameListVertices = "listVertices";
        templater.templateNameListVertexViewInEdgeList = "listVertexViewInEdgeList";
        templater.templateNameListVertexViewOutEdgeList = "listVertexViewOutEdgeList";
        templater.templateNameListExtensionList = "listExtensionList";

        templater.init = function() {
            // expects {id, menuName, [checked], [disabled]}
            var templateMainMenuMarkup = '{#menuItems}<input type="radio" id="radioMenu{id}" value="{id}" name="radioMenu" {?checked}checked="checked"{/checked} {?disabled}disabled="disabled"{/disabled}/><label for="radioMenu{id}">{menuName}</label>{/menuItems}';
            var dustMainMenuItemCompiled = dust.compile(templateMainMenuMarkup, templater.templateNameMainMenuItem);
            dust.loadSource(dustMainMenuItemCompiled);

            // expects {menuName, panel}
            var templateMenuGraph = '{#menuItems}<div id="graphItem{panel}{menuName}" class="graph-item ui-state-default ui-corner-all" style="cursor:pointer;padding:2px;margin:1px"><a href="/doghouse/main/{panel}/{menuName}">{menuName}</a></div>{/menuItems}';
            var dustMenuGraphCompiled = dust.compile(templateMenuGraph, templater.templateNameMenuGraph);
            dust.loadSource(dustMenuGraphCompiled);

            // expects {_id}
            var templateListVertices = '{#vertices}<li class="column"><a href="http://www.google.com">{_id}</a></li>{/vertices}';
            var dustListVerticesCompiled = dust.compile(templateListVertices, templater.templateNameListVertices);
            dust.loadSource(dustListVerticesCompiled);

            // expects {_label, _inV, _outV, _id, currentGraphName}
            var templateListVertexViewInEdgeList = '{#edges}<li>{_outV} - <a href="/doghouse/main/graph/{currentGraphName}/edges/{_id}">{_label}</a> - <a href="/doghouse/main/graph/{currentGraphName}/vertices/{_inV}">{_inV}</a></li>{/edges}';
            var dustListVertexViewInEdgeListCompiled = dust.compile(templateListVertexViewInEdgeList, templater.templateNameListVertexViewInEdgeList);
            dust.loadSource(dustListVertexViewInEdgeListCompiled);

            // expects {_label, _outV, _inV, _id, currentGraphName}
            var templateListVertexViewOutEdgeList = '{#edges}<li><a href="/doghouse/main/graph/{currentGraphName}/vertices/{_outV}">{_outV}</a> - <a href="/doghouse/main/graph/{currentGraphName}/edges/{_id}">{_label}</a> - {_inV}</li>{/edges}';
            var dustListVertexViewOutEdgeListCompiled = dust.compile(templateListVertexViewOutEdgeList, templater.templateNameListVertexViewOutEdgeList);
            dust.loadSource(dustListVertexViewOutEdgeListCompiled);

            // expects {href, title, parameters[]}
            var templateListExtensionList = '{#extensions}<h3><a href="#" {?description}title="{description}"{/description}>{title}</a></h3><div><form><fieldset><label for="extensionUri">Extension URI</label><input type="text" name="extensionUri" class="text ui-widget-content ui-corner-all" value="{href}" />{#parameters}<label for="{name}">{name}</label><input type="text" name="{name}" class="text ui-widget-content ui-corner-all" title="{description}" />{/parameters}</fieldset></form><a href="#" title="{title}">Execute</a></div>{/extensions}';
            var dustListExtensionListCompiled = dust.compile(templateListExtensionList, templater.templateNameListExtensionList);
            dust.loadSource(dustListExtensionListCompiled);

        }

        // public methods
        return {
            /**
             * Constructs <li> values for an array of vertices.
             *
             * @param data 		{Array} The list of vertex objects to render.
             * @param selector	{Object} A jQuery selector or element to append the template to.
             */
            applyListVerticesTemplate : function(data, selector) {
                if (data.length > 0) {
                    dust.render(templater.templateNameListVertices, {vertices :data}, function(err, out) {
                        $(out).appendTo(selector);
                    });
                } else {
                    dust.render(templater.templateNameListVertices, {vertices :[{ "_id":"No vertices in this graph"}]}, function(err, out) {
                        $(out).appendTo(selector);
                    });
                }
            },

            /**
             * Constructs graph menu values for an array of graphs.
             *
             * @param data 		{Array} The list of graph objects to render.
             * @param selector	{String} A jQuery selector or element to append the template to.
             */
            applyMenuGraphTemplate : function(data, selector) {
                if (data.length > 0) {
                    dust.render(templater.templateNameMenuGraph, {menuItems :data}, function(err, out) {
                        $(out).appendTo(selector);
                    });
                } else {
                    // TODO: need something here if nothing is configured ???
                }
            },

            /**
             * Constructs main menu.
             *
             * @param data 		{Array} The list of menu item objects to render.
             * @param selector	{String} A jQuery selector or element to append the template to.
             */
            applyMainMenuTemplate : function(data, selector) {
                dust.render(templater.templateNameMainMenuItem, {menuItems :data}, function(err, out) {
                    $(out).appendTo(selector);
                });
            },

            applyListVertexViewOutEdgeListTempate : function(data, selector) {
                dust.render(templater.templateNameListVertexViewOutEdgeList, {edges :data}, function(err, out) {
                    $(out).appendTo(selector);
                });
            },

            applyListVertexViewInEdgeListTempate : function(data, selector) {
                dust.render(templater.templateNameListVertexViewInEdgeList, {edges :data}, function(err, out) {
                    $(out).appendTo(selector);
                });
            },

            applyListExtensionList : function(data, selector) {
                dust.render(templater.templateNameListExtensionList, {extensions :data.filter(function(extension){ return extension.op === "GET" })}, function(err, out) {
                    $(out).appendTo(selector);
                });
            },

            initTemplates : templater.init
        }
});