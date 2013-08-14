package com.thinkaurelius.faunus.tinkerpop.gremlin;

import javax.script.Bindings;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ScriptExecutor {

    public static void main(final String[] arguments) throws IOException {
        if (arguments.length == 0) {
            System.out.println("Usage: <path_to_faunus_gremlin_script> <argument a1> <argument a2> ...");
        } else {
            evaluate(new FileReader(arguments[0]), Arrays.asList(arguments).subList(1, arguments.length));
        }
    }

    protected static void evaluate(final Reader reader, final List<String> arguments) throws IOException {
        final FaunusGremlinScriptEngine engine = new FaunusGremlinScriptEngine();

        final Bindings bindings = engine.createBindings();
        if (arguments.size() > 0) {
            for (int i = 0; i < arguments.size(); i++) {
                bindings.put("a" + (i + 1), arguments.get(i));
            }
        }

        try {
            engine.eval(reader, bindings);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }

    }

}
