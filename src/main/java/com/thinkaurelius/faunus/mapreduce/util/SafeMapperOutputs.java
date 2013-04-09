package com.thinkaurelius.faunus.mapreduce.util;

import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SafeMapperOutputs {

    private final MultipleOutputs outputs;
    private final Mapper.Context context;
    private final boolean testing;

    public SafeMapperOutputs(final Mapper.Context context) {
        this.context = context;
        this.outputs = new MultipleOutputs(this.context);
        this.testing = this.context.getConfiguration().getBoolean(FaunusCompiler.TESTING, false);
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
