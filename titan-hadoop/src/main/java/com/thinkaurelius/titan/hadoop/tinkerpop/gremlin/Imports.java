package com.thinkaurelius.titan.hadoop.tinkerpop.gremlin;

import com.tinkerpop.pipes.transform.TransformPipe;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import javax.script.Bindings;
import javax.script.SimpleBindings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Imports {

    private static final List<String> imports = new ArrayList<String>();
    private static final List<String> evaluates = new ArrayList<String>();

    public static final String HDFS = "hdfs";
    public static final String LOCAL = "local";

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
        imports.add("com.thinkaurelius.titan.hadoop.*");
        imports.add("com.thinkaurelius.titan.hadoop.formats.*");
        imports.add("com.thinkaurelius.titan.hadoop.formats.edgelist.*");
        imports.add("com.thinkaurelius.titan.hadoop.formats.edgelist.rdf.*");
        imports.add("com.thinkaurelius.titan.hadoop.formats.graphson.*");
        imports.add("com.thinkaurelius.titan.hadoop.formats.noop.*");
        imports.add("com.thinkaurelius.titan.hadoop.formats.rexster.*");
        imports.add("com.thinkaurelius.titan.hadoop.formats.script.*");
        imports.add("com.thinkaurelius.titan.hadoop.formats.sequence.faunus01.*");
        imports.add("com.thinkaurelius.titan.hadoop.formats.titan.*");
        imports.add("com.thinkaurelius.titan.hadoop.formats.titan.hbase.*");
        imports.add("com.thinkaurelius.titan.hadoop.formats.titan.cassandra.*");
        imports.add("com.thinkaurelius.titan.hadoop.hdfs.*");
        imports.add("com.thinkaurelius.titan.hadoop.tinkerpop.gremlin.*");
        imports.add("com.tinkerpop.gremlin.Tokens.T");
        imports.add("com.tinkerpop.gremlin.groovy.*");
        imports.add("static " + TransformPipe.Order.class.getName() + ".*");

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

    public static Bindings getEvaluateBindings() throws IOException {
        Bindings bindings = new SimpleBindings();
        bindings.put(Imports.HDFS, FileSystem.get(new Configuration()));
        bindings.put(Imports.LOCAL, FileSystem.getLocal(new Configuration()));
        return bindings;
    }
}