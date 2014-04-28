package com.thinkaurelius.faunus.formats.edgelist;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EdgeListRecordWriter extends RecordWriter<NullWritable, FaunusVertex> {
    protected DataOutputStream out;
    private static final String UTF8 = "UTF-8";
    private static final byte[] NEWLINE;
    private static final byte[] TAB;

    static {
        try {
            NEWLINE = Tokens.NEWLINE.getBytes(UTF8);
            TAB = Tokens.TAB.getBytes(UTF8);
        } catch (final UnsupportedEncodingException uee) {
            throw new IllegalArgumentException("Can not find " + UTF8 + " encoding");
        }
    }

    public EdgeListRecordWriter(final DataOutputStream out) {
        this.out = out;
    }

    @Override
    public void write(final NullWritable key, final FaunusVertex vertex) throws IOException {
        if (null != vertex) {
            final byte[] id = vertex.getId().toString().getBytes(UTF8);
            for (final Edge edge : vertex.getEdges(Direction.OUT)) {
                this.out.write(id);
                this.out.write(TAB);
                this.out.write(edge.getVertex(Direction.IN).getId().toString().getBytes(UTF8));
                this.out.write(TAB);
                this.out.write(edge.getLabel().getBytes(UTF8));
                this.out.write(NEWLINE);
            }
        }
    }

    @Override
    public synchronized void close(TaskAttemptContext context) throws IOException {
        out.close();
    }
}
