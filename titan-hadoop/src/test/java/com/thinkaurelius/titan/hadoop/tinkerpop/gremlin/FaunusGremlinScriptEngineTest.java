package com.thinkaurelius.titan.hadoop.tinkerpop.gremlin;

import junit.framework.TestCase;

import javax.script.ScriptEngine;

import com.thinkaurelius.titan.hadoop.tinkerpop.gremlin.FaunusGremlinScriptEngineFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusGremlinScriptEngineTest extends TestCase {

    public void testEvaluates() throws Exception {
        ScriptEngine engine = new FaunusGremlinScriptEngineFactory().getScriptEngine();
        engine.eval("local.ls()");
        engine.eval("hdfs.ls()");
    }

}
