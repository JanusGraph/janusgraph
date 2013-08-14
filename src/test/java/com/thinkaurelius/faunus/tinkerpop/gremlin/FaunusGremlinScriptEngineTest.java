package com.thinkaurelius.faunus.tinkerpop.gremlin;

import junit.framework.TestCase;

import javax.script.ScriptEngine;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusGremlinScriptEngineTest extends TestCase {

    public void testEvaluates() throws Exception {
        ScriptEngine engine = new FaunusGremlinScriptEngineFactory().getScriptEngine();
        engine.eval("local.ls()");
    }
}
