package com.thinkaurelius.faunus.tinkerpop.gremlin;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Imports {

    private static final List<String> imports = new ArrayList<String>();
    private static final List<String> evaluates = new ArrayList<String>();

    static {

        // hadoop
        imports.add("org.apache.hadoop.hdfs.*");
        imports.add("org.apache.hadoop.conf.*");
        imports.add("org.apache.hadoop.fs.*");
        imports.add("org.apache.hadoop.util.*");
        imports.add("org.apache.hadoop.io.*");
        imports.add("org.apache.hadoop.io.compress.*");
        imports.add("org.apache.hadoop.mapreduce.lib.input.*");
        imports.add("org.apache.hadoop.mapreduce.lib.output.*");

        // faunus
        imports.add("com.thinkaurelius.faunus.*");
        imports.add("com.thinkaurelius.faunus.formats.*");
        imports.add("com.thinkaurelius.faunus.formats.edgelist.*");
        imports.add("com.thinkaurelius.faunus.formats.edgelist.rdf.*");
        imports.add("com.thinkaurelius.faunus.formats.graphson.*");
        imports.add("com.thinkaurelius.faunus.formats.noop.*");
        imports.add("com.thinkaurelius.faunus.formats.rexster.*");
        imports.add("com.thinkaurelius.faunus.formats.script.*");
        imports.add("com.thinkaurelius.faunus.formats.sequence.faunus01.*");
        imports.add("com.thinkaurelius.faunus.formats.titan.*");
        imports.add("com.thinkaurelius.faunus.formats.titan.hbase.*");
        imports.add("com.thinkaurelius.faunus.formats.titan.cassandra.*");
        imports.add("com.thinkaurelius.faunus.hdfs.*");
        imports.add("com.thinkaurelius.faunus.tinkerpop.gremlin.*");
        imports.add("com.tinkerpop.gremlin.Tokens.T");
        imports.add("com.tinkerpop.gremlin.groovy.*");
        imports.add("com.thinkaurelius.faunus.Tokens.F");

        // titan
        imports.addAll(com.thinkaurelius.titan.tinkerpop.gremlin.Imports.getImports());

        // tinkerpop (most likely inherited from Titan, but just to be safe)
        imports.addAll(com.tinkerpop.gremlin.Imports.getImports());

        evaluates.add("hdfs = FileSystem.get(new Configuration())");
        evaluates.add("local = FileSystem.getLocal(new Configuration())");
    }

    public static List<String> getImports() {
        return Imports.imports;
    }

    public static List<String> getEvaluates() {
        return Imports.evaluates;
    }
}