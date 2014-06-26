package com.thinkaurelius.titan.hadoop.mapreduce.util;

import com.thinkaurelius.titan.hadoop.Tokens;
import com.thinkaurelius.titan.hadoop.compat.HadoopCompiler;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SafeReducerOutputs {

    private final MultipleOutputs outputs;
    private final Reducer.Context context;
    private final boolean testing;

    public SafeReducerOutputs(final Reducer.Context context) {
        this.context = context;
        this.outputs = new MultipleOutputs(this.context);
        this.testing = this.context.getConfiguration().getBoolean(HadoopCompiler.TESTING, false);
    }

    public void write(final String type, final Writable key, final Writable value) throws IOException, InterruptedException {
        if (this.testing) {
            if (type.equals(Tokens.SIDEEFFECT))
                this.context.write(key, value);
        } else
            this.outputs.write(type, key, value);
    }

    public void close() throws IOException, InterruptedException {
        this.outputs.close();
    }
}
