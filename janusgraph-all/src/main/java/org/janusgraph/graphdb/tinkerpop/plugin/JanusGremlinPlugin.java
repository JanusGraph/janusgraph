package org.janusgraph.graphdb.tinkerpop.plugin;

import org.janusgraph.core.Cardinality;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.attribute.Geo;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.example.GraphOfTheGodsFactory;
import org.janusgraph.graphdb.tinkerpop.JanusIoRegistry;
import org.apache.tinkerpop.gremlin.groovy.plugin.GremlinPlugin;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;

import java.time.temporal.ChronoUnit;
import java.util.HashSet;;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class JanusGremlinPlugin implements GremlinPlugin {

    private static final String IMPORT = "import ";
    private static final String IMPORT_STATIC = IMPORT + "static ";
    private static final String DOT_STAR = ".*";

    private static final Set<String> IMPORTS = new HashSet<String>() {{
        add(IMPORT + "org.janusgraph.core" + DOT_STAR);
        add(IMPORT + "org.janusgraph.core.attribute" + DOT_STAR);
        add(IMPORT + "org.janusgraph.core.schema" + DOT_STAR);
        add(IMPORT + GraphOfTheGodsFactory.class.getName());
        add(IMPORT + "org.janusgraph.hadoop.MapReduceIndexManagement");
        add(IMPORT + "java.time" + DOT_STAR);
        add(IMPORT + JanusIoRegistry.class.getName());

        // Static imports on enum values used in query constraint expressions
        add(IMPORT_STATIC + Geo.class.getName() + DOT_STAR);
        add(IMPORT_STATIC + Text.class.getName() + DOT_STAR);
        add(IMPORT_STATIC + Multiplicity.class.getName() + DOT_STAR);
        add(IMPORT_STATIC + Cardinality.class.getName() + DOT_STAR);
        add(IMPORT_STATIC + ChronoUnit.class.getName() + DOT_STAR);

    }};

    @Override
    public String getName() {
        return "aurelius.janus";
    }

    @Override
    public void pluginTo(final PluginAcceptor pluginAcceptor) {
        pluginAcceptor.addImports(IMPORTS);
    }

    @Override
    public boolean requireRestart() {
        return true;
    }

}
