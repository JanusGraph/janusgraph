package com.thinkaurelius.titan.tinkerpop.gremlin;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Imports {

    private static final List<String> imports = new ArrayList<String>();

    static {
        // titan
        imports.add("com.thinkaurelius.titan.core.*");
        imports.add("org.apache.commons.configuration.*");

        // gremlin
        imports.add("com.tinkerpop.gremlin.*");
        imports.add("com.tinkerpop.gremlin.java.*");
        imports.add("com.tinkerpop.gremlin.pipes.*");
        imports.add("com.tinkerpop.gremlin.pipes.filter.*");
        imports.add("com.tinkerpop.gremlin.pipes.transform.*");

        // blueprints
        imports.add("com.tinkerpop.blueprints.*");
        imports.add("static com.tinkerpop.blueprints.Direction.*");
        imports.add("static com.tinkerpop.blueprints.TransactionalGraph.Conclusion.*");
        imports.add("com.tinkerpop.blueprints.impls.*");
        imports.add("com.tinkerpop.blueprints.impls.tg.*");
        /*imports.add("com.tinkerpop.blueprints.impls.neo4j.*");
        imports.add("com.tinkerpop.blueprints.impls.orient.*");
        imports.add("com.tinkerpop.blueprints.impls.dex.*");
        imports.add("com.tinkerpop.blueprints.impls.rexster.*");
        imports.add("com.tinkerpop.blueprints.impls.neo4jbatch.*");
        imports.add("com.tinkerpop.blueprints.impls.sail.*");
        imports.add("com.tinkerpop.blueprints.impls.sail.impls.*");*/
        imports.add("com.tinkerpop.blueprints.util.*");
        imports.add("com.tinkerpop.blueprints.util.io.*");
        imports.add("com.tinkerpop.blueprints.util.io.gml.*");
        imports.add("com.tinkerpop.blueprints.util.io.graphml.*");
        imports.add("com.tinkerpop.blueprints.util.io.graphson.*");
        imports.add("com.tinkerpop.blueprints.util.wrappers.*");
        imports.add("com.tinkerpop.blueprints.util.wrappers.batch.*");
        imports.add("com.tinkerpop.blueprints.util.wrappers.batch.cache.*");
        imports.add("com.tinkerpop.blueprints.util.wrappers.event.*");
        imports.add("com.tinkerpop.blueprints.util.wrappers.event.listener.*");
        imports.add("com.tinkerpop.blueprints.util.wrappers.id.*");
        imports.add("com.tinkerpop.blueprints.util.wrappers.readonly.*");
        imports.add("com.tinkerpop.blueprints.util.wrappers.partition.*");
        imports.add("com.tinkerpop.blueprints.oupls.sail.*");
        imports.add("com.tinkerpop.blueprints.oupls.jung.*");

        // pipes
        imports.add("com.tinkerpop.pipes.*");
        imports.add("com.tinkerpop.pipes.branch.*");
        imports.add("com.tinkerpop.pipes.filter.*");
        imports.add("com.tinkerpop.pipes.sideeffect.*");
        imports.add("com.tinkerpop.pipes.transform.*");
        imports.add("com.tinkerpop.pipes.util.*");
        imports.add("com.tinkerpop.pipes.util.iterators.*");
        imports.add("com.tinkerpop.pipes.util.structures.*");
    }

    public static List<String> getImports() {
        return Imports.imports;
    }
}
