package com.thinkaurelius.titan.graphdb.tinkerpop.plugin;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.graphdb.tinkerpop.computer.bulkloader.BulkLoaderVertexProgram;
import com.tinkerpop.gremlin.groovy.plugin.Artifact;
import com.tinkerpop.gremlin.groovy.plugin.GremlinPlugin;
import com.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;

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
        return "aurelius.titan";
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
        return Optional.empty();
    }
}
