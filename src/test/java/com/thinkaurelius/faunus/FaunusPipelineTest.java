package com.thinkaurelius.faunus;

import com.thinkaurelius.faunus.formats.graphson.GraphSONInputFormat;
import com.thinkaurelius.faunus.formats.graphson.GraphSONOutputFormat;
import com.thinkaurelius.faunus.mapreduce.MapSequence;
import com.thinkaurelius.faunus.mapreduce.derivations.Identity;
import com.thinkaurelius.faunus.mapreduce.derivations.LabelFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.Arrays;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusPipelineTest extends BaseTest {

    protected static Configuration conf = new Configuration();

    static {
        conf.set(FaunusConfiguration.GRAPH_INPUT_FORMAT_CLASS, GraphSONInputFormat.class.getName());
        conf.set(FaunusConfiguration.GRAPH_OUTPUT_FORMAT_CLASS, GraphSONOutputFormat.class.getName());
        conf.set(FaunusConfiguration.STATISTIC_OUTPUT_FORMAT_CLASS, TextOutputFormat.class.getName());
        conf.set(FaunusConfiguration.OUTPUT_LOCATION, "output.txt");
    }

    public void testMapOnlyComposition() throws Exception {
        FaunusPipeline g = new FaunusPipeline("g", conf);
        g = g.V._().labelFilter(Tokens.Action.KEEP, "father");
        assertEquals(g.getJobSequence().size(), 1);
        List<String> classes = Arrays.asList(g.getJobSequence().get(0).getConfiguration().getStrings(MapSequence.MAP_CLASSES));
        assertEquals(classes.size(), 2);
        assertEquals(classes.get(0), Identity.Map.class.getName());
        assertEquals(classes.get(1), LabelFilter.Map.class.getName());
        assertEquals(g.getJobSequence().get(0).getConfiguration().getStrings(LabelFilter.LABELS + "-1")[0], "father");
        assertEquals(g.getJobSequence().get(0).getConfiguration().get(LabelFilter.ACTION + "-1"), Tokens.Action.KEEP.name());
        assertTrue(g.derivationJob);
    }

    public void testMapOnlyComposition2() throws Exception {
        FaunusPipeline g = new FaunusPipeline("g", conf);
        g = g.V._().labelFilter(Tokens.Action.KEEP, "brother")._();
        assertEquals(g.getJobSequence().size(), 1);
        List<String> classes = Arrays.asList(g.getJobSequence().get(0).getConfiguration().getStrings(MapSequence.MAP_CLASSES));
        assertEquals(classes.size(), 3);
        assertEquals(classes.get(0), Identity.Map.class.getName());
        assertEquals(classes.get(1), LabelFilter.Map.class.getName());
        assertEquals(classes.get(2), Identity.Map.class.getName());
        assertEquals(g.getJobSequence().get(0).getConfiguration().getStrings(LabelFilter.LABELS + "-1")[0], "brother");
        assertEquals(g.getJobSequence().get(0).getConfiguration().get(LabelFilter.ACTION + "-1"), Tokens.Action.KEEP.name());
        assertTrue(g.derivationJob);
        try {
            String x = g.getJobSequence().get(0).getConfiguration().getStrings(LabelFilter.LABELS + "-1")[1];
            assertFalse(true);
        } catch (ArrayIndexOutOfBoundsException e) {
            assertTrue(true);
        }
    }

    /* public void testClosure() throws Exception {
       ScriptEngine engine = new GroovyScriptEngineImpl();
       Closure closure = (Closure) engine.eval("{-> 1+2}");
       closure = closure.dehydrate();

       ByteArrayOutputStream bos = new ByteArrayOutputStream();
       ObjectOutput out = new ObjectOutputStream(bos);
       out.writeObject(closure);
       out.flush();
       out.close();

       BASE64Encoder encoder = new BASE64Encoder();
       String closureString = encoder.encode(bos.toByteArray());
       System.out.println(closureString);

       ByteArrayInputStream bis = new ByteArrayInputStream(new BASE64Decoder().decodeBuffer(closureString));
       ObjectInputStream in = new ObjectInputStream(bis);

       Closure closure1 = (Closure) in.readObject();
       in.close();

       System.out.println(closure1.call());
   } */

}
