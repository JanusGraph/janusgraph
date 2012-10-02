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
    }

    public static List<String> getImports() {
        return Imports.imports;
    }
}
