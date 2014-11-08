package com.thinkaurelius.titan.graphdb.tinkerpop.plugin;

import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.graphdb.configuration.TitanConstants;
import com.thinkaurelius.titan.graphdb.tinkerpop.computer.bulkloader.BulkLoaderVertexProgram;
import com.tinkerpop.gremlin.groovy.plugin.Artifact;
import com.tinkerpop.gremlin.groovy.plugin.GremlinPlugin;
import com.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanGremlinPlugin implements GremlinPlugin {

    private static final String IMPORT = "import ";
    private static final String DOT_STAR = ".*";

    private static final Set<String> IMPORTS = new HashSet<String>() {{
        add(IMPORT + TitanGraph.class.getPackage().getName() + DOT_STAR);
        add(IMPORT + BulkLoaderVertexProgram.class.getPackage().getName() + DOT_STAR);
    }};

    @Override
    public String getName() {
        return "aurelius.titan"; // TODO: call this thinkaurelius.titan?
    }

    @Override
    public void pluginTo(final PluginAcceptor pluginAcceptor) {
        pluginAcceptor.addImports(IMPORTS);
    }

    @Override
    public boolean requireRestart() {
        return true;
    }

    @Override
    public Optional<Set<Artifact>> additionalDependencies() {
        return Optional.of(ImmutableSet.of(
                new Artifact("com.thinkaurelius.titan", "titan-cassandra", TitanConstants.VERSION),
//                new Artifact("com.thinkaurelius.titan", "titan-hbase", TitanConstants.VERSION),
//                new Artifact("org.apache.hbase", "hbase-client", "0.98.3-hadoop1"),
//                new Artifact("org.apache.hbase", "hbase-common", "0.98.3-hadoop1"),
//                new Artifact("org.apache.hbase", "hbase-server", "0.98.3-hadoop1"), /* For TableInputFormat */
                new Artifact("com.thinkaurelius.titan", "titan-hadoop", TitanConstants.VERSION)));
    }
}
