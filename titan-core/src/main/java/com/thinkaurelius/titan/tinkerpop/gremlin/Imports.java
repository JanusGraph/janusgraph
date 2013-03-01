package com.thinkaurelius.titan.tinkerpop.gremlin;

import java.util.ArrayList;
import java.util.List;

/**
 * Titan specific Gremlin imports
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Imports {

    private static final List<String> imports = new ArrayList<String>();

    static {
        // titan
        imports.add("com.thinkaurelius.titan.core.*");
        imports.add("com.thinkaurelius.titan.core.attribute.*");
        imports.add("org.apache.commons.configuration.*");
    }

    public static List<String> getImports() {
        return Imports.imports;
    }
}
