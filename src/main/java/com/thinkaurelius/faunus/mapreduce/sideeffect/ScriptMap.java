package com.thinkaurelius.faunus.mapreduce.sideeffect;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.mapreduce.util.EmptyConfiguration;
import com.thinkaurelius.faunus.mapreduce.util.SafeMapperOutputs;
import com.thinkaurelius.faunus.tinkerpop.gremlin.FaunusGremlinScriptEngine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.script.ScriptEngine;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ScriptMap {

    public static final String CLASS = Tokens.makeNamespace(ScriptMap.class) + ".class";
    public static final String SCRIPT_PATH = Tokens.makeNamespace(ScriptMap.class) + ".scriptPath";
    public static final String SCRIPT_ARGS = Tokens.makeNamespace(ScriptMap.class) + ".scriptArgs";

    private static final String ARGS = "args";
    private static final String V = "v";
    private static final String SETUP_ARGS = "setup(args)";
    private static final String MAP_V_ARGS = "map(v,args)";
    private static final String CLEANUP_ARGS = "cleanup(args)";

    public static Configuration createConfiguration(final String scriptUri, final String... args) {
        Configuration configuration = new EmptyConfiguration();
        configuration.set(SCRIPT_PATH, scriptUri);
        configuration.setStrings(SCRIPT_ARGS, args);
        return configuration;
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private final ScriptEngine engine = new FaunusGremlinScriptEngine();
        private SafeMapperOutputs outputs;
        private Text textWritable = new Text();

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            final FileSystem fs = FileSystem.get(context.getConfiguration());
            try {
                this.engine.eval(new InputStreamReader(fs.open(new Path(context.getConfiguration().get(SCRIPT_PATH)))));
                this.engine.put(ARGS, context.getConfiguration().getStrings(SCRIPT_ARGS));
                this.engine.eval(SETUP_ARGS);
            } catch (Exception e) {
                throw new InterruptedException(e.getMessage());
            }
            this.outputs = new SafeMapperOutputs(context);

        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            if (value.hasPaths()) {
                final Object result;
                try {
                    this.engine.put(V, value);
                    result = engine.eval(MAP_V_ARGS);
                } catch (Exception e) {
                    throw new InterruptedException(e.getMessage());
                }
                this.textWritable.set((null == result) ? Tokens.NULL : result.toString());
                this.outputs.write(Tokens.SIDEEFFECT, NullWritable.get(), this.textWritable);
            }
            this.outputs.write(Tokens.GRAPH, NullWritable.get(), value);
        }

        @Override
        public void cleanup(final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            try {
                this.engine.eval(CLEANUP_ARGS);
            } catch (Exception e) {
                throw new InterruptedException(e.getMessage());
            }
            this.outputs.close();
        }

    }
}
