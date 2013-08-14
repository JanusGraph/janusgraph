package com.thinkaurelius.faunus.tinkerpop.gremlin;

import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import groovy.lang.GroovyClassLoader;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.control.customizers.ImportCustomizer;

import javax.script.ScriptContext;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusGremlinScriptEngine extends GremlinGroovyScriptEngine {

    private static final String DOT_STAR = ".*";
    private static final String EMPTY_STRING = "";

    public FaunusGremlinScriptEngine() {
        super();
        FaunusGremlin.load();
        final CompilerConfiguration conf = new CompilerConfiguration();
        conf.addCompilationCustomizers(FaunusGremlinScriptEngine.getImportCustomizer());
        this.loader = new GroovyClassLoader(getParentLoader(), conf);
        try {
            this.setBindings(Imports.getEvaluateBindings(), ScriptContext.GLOBAL_SCOPE);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static ImportCustomizer getImportCustomizer() {
        final ImportCustomizer ic = GremlinGroovyScriptEngine.getImportCustomizer();
        for (final String imp : Imports.getImports()) {
            ic.addStarImports(imp.replace(DOT_STAR, EMPTY_STRING));
        }
        return ic;
    }
}
