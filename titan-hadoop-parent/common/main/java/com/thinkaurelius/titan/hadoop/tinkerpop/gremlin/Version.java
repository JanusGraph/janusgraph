package com.thinkaurelius.titan.hadoop.tinkerpop.gremlin;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Version {

    public static void main(final String[] arguments) throws IOException {
        System.out.println("titan.hadoop " + com.thinkaurelius.titan.hadoop.Tokens.VERSION);
        System.out.println("gremlin " + com.tinkerpop.gremlin.Tokens.VERSION);
    }
}
