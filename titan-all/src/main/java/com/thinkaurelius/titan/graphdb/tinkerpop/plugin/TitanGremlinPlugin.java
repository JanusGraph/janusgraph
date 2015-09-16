package com.thinkaurelius.titan.graphdb.tinkerpop.plugin;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.attribute.Geo;
import com.thinkaurelius.titan.core.attribute.Text;
import com.thinkaurelius.titan.example.GraphOfTheGodsFactory;
import org.apache.tinkerpop.gremlin.groovy.plugin.GremlinPlugin;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;

import java.time.temporal.ChronoUnit;
import java.util.HashSet;;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanGremlinPlugin implements GremlinPlugin {

    private static final String IMPORT = "import ";
    private static final String IMPORT_STATIC = IMPORT + "static ";
    private static final String DOT_STAR = ".*";

    private static final Set<String> IMPORTS = new HashSet<String>() {{
        add(IMPORT + "com.thinkaurelius.titan.core" + DOT_STAR);
        add(IMPORT + "com.thinkaurelius.titan.core.attribute" + DOT_STAR);
        add(IMPORT + "com.thinkaurelius.titan.core.schema" + DOT_STAR);
        add(IMPORT + GraphOfTheGodsFactory.class.getName());
        add(IMPORT + "com.thinkaurelius.titan.hadoop.MapReduceIndexManagement");
        add(IMPORT + "java.time" + DOT_STAR);

        // Static imports on enum values used in query constraint expressions
        add(IMPORT_STATIC + Geo.class.getName() + DOT_STAR);
        add(IMPORT_STATIC + Text.class.getName() + DOT_STAR);
        add(IMPORT_STATIC + Multiplicity.class.getName() + DOT_STAR);
        add(IMPORT_STATIC + Cardinality.class.getName() + DOT_STAR);
        add(IMPORT_STATIC + ChronoUnit.class.getName() + DOT_STAR);

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

}
