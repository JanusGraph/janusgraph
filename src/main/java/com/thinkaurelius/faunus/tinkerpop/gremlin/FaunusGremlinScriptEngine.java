package com.thinkaurelius.faunus.tinkerpop.gremlin;

import com.tinkerpop.gremlin.groovy.Gremlin;
import org.codehaus.groovy.jsr223.GroovyScriptEngineImpl;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptException;
import java.io.BufferedReader;
import java.io.Reader;
import java.io.StringReader;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusGremlinScriptEngine extends GroovyScriptEngineImpl {

    private String imports;

    public FaunusGremlinScriptEngine() {
        super();
        Gremlin.load();
        FaunusGremlin.load();
        StringBuilder sb = new StringBuilder();
        for (final String imp : Imports.getImports()) {
            sb.append("import ").append(imp).append("\n");
        }

        sb.append("import com.tinkerpop.gremlin.Tokens.T\n");
        sb.append("import com.tinkerpop.gremlin.groovy.*\n");
        sb.append("hdfs = FileSystem.get(new Configuration())\n");
        sb.append("local = FileSystem.getLocal(new Configuration())\n");

        this.imports = sb.toString();
    }

    public Object eval(final String script) throws ScriptException {
        return super.eval(this.imports + script);
    }

    public Object eval(final String script, final ScriptContext context) throws ScriptException {
        return super.eval(this.imports + script, context);
    }

    public Object eval(final String script, final Bindings bindings) throws ScriptException {
        return super.eval(this.imports + script, bindings);
    }


    public Object eval(final Reader reader) throws ScriptException {
        try {
            return super.eval(generateStringReader(reader));
        } catch (Exception e) {
            throw new ScriptException(e.getMessage());
        }
    }

    public Object eval(final Reader reader, final ScriptContext context) throws ScriptException {
        try {
            return super.eval(generateStringReader(reader), context);
        } catch (Exception e) {
            throw new ScriptException(e.getMessage());
        }
    }

    public Object eval(final Reader reader, final Bindings bindings) throws ScriptException {
        try {
            return super.eval(generateStringReader(reader), bindings);
        } catch (Exception e) {
            throw new ScriptException(e.getMessage());
        }
    }

    private StringReader generateStringReader(final Reader reader) throws Exception {
        final StringBuilder sb = new StringBuilder(this.imports);
        final BufferedReader bf = new BufferedReader(reader);
        String line;
        while ((line = bf.readLine()) != null) {
            sb.append(line).append("\n");
        }
        return new StringReader(sb.toString());
    }
}
