package com.thinkaurelius.faunus.tinkerpop.gremlin;

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
        //imports.add("org.apache.commons.configuration.*");

        // faunus
        imports.add("org.apache.hadoop.hdfs.*");
        imports.add("org.apache.hadoop.conf.*");
        imports.add("org.apache.hadoop.fs.*");
        imports.add("org.apache.hadoop.util.*");
        imports.add("com.thinkaurelius.faunus.*");
        imports.add("com.thinkaurelius.faunus.tinkerpop.gremlin.*");

        imports.addAll(com.tinkerpop.gremlin.Imports.getImports());

    }

    public static List<String> getImports() {
        return Imports.imports;
    }
}