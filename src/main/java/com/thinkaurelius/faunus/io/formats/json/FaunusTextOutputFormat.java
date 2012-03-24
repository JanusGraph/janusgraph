package com.thinkaurelius.faunus.io.formats.json;

import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import com.tinkerpop.blueprints.pgm.Edge;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusTextOutputFormat extends FileOutputFormat<NullWritable, FaunusVertex> {
    protected static class LineRecordWriter extends RecordWriter<NullWritable, FaunusVertex> {
        private static final String utf8 = "UTF-8";
        private static final byte[] NEWLINE;

        static {
            try {
                NEWLINE = "\n".getBytes(utf8);
            } catch (UnsupportedEncodingException uee) {
                throw new IllegalArgumentException("can't find " + utf8 + " encoding");
            }
        }

        protected DataOutputStream out;

        public LineRecordWriter(final DataOutputStream out) {
            this.out = out;
        }

        public synchronized void write(final NullWritable nullKey, final FaunusVertex vertex) throws IOException {
            if (null != vertex) {
                final JSONObject object = new JSONObject();
                object.put(JSONTokens.ID, vertex.getId());
                final Set<String> vertexKeys = vertex.getPropertyKeys();
                if (!vertexKeys.isEmpty()) {
                    final JSONObject vertexProperties = new JSONObject();
                    for (String vertexKey : vertexKeys) {
                        vertexProperties.put(vertexKey, vertex.getProperty(vertexKey));
                    }
                    object.put(JSONTokens.PROPERTIES, vertexProperties);
                }

                final List<Edge> outEdges = (List<Edge>) vertex.getOutEdges();
                if (!outEdges.isEmpty()) {
                    final JSONArray outEdgesArray = new JSONArray();
                    for (final Edge outEdge : outEdges) {
                        final JSONObject edge = new JSONObject();
                        edge.put(JSONTokens.IN_ID, outEdge.getInVertex().getId());
                        edge.put(JSONTokens.LABEL, outEdge.getLabel());
                        final Set<String> edgeKeys = outEdge.getPropertyKeys();
                        if (!edgeKeys.isEmpty()) {
                            final JSONObject edgeProperties = new JSONObject();
                            for (final String edgeKey : edgeKeys) {
                                edgeProperties.put(edgeKey, outEdge.getProperty(edgeKey));
                            }
                            edge.put(JSONTokens.PROPERTIES, edgeProperties);
                        }
                        outEdgesArray.add(edge);
                    }
                    object.put(JSONTokens.OUT_E, outEdgesArray);
                }
                this.out.write(object.toString().getBytes(utf8));
                this.out.write(NEWLINE);
            }

        }

        public synchronized void close(TaskAttemptContext context) throws IOException {
            out.close();
        }
    }

    public RecordWriter<NullWritable, FaunusVertex> getRecordWriter(final TaskAttemptContext job) throws IOException, InterruptedException {
        final Configuration conf = job.getConfiguration();
        boolean isCompressed = getCompressOutput(job);
        CompressionCodec codec = null;
        String extension = "";
        if (isCompressed) {
            Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
            codec = ReflectionUtils.newInstance(codecClass, conf);
            extension = codec.getDefaultExtension();
        }
        final Path file = getDefaultWorkFile(job, extension);
        final FileSystem fs = file.getFileSystem(conf);
        if (!isCompressed) {
            FSDataOutputStream fileOut = fs.create(file, false);
            return new LineRecordWriter(fileOut);
        } else {
            FSDataOutputStream fileOut = fs.create(file, false);
            return new LineRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)));
        }
    }
}