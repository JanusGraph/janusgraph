package com.thinkaurelius.faunus.io.formats.json;


import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class JSONRecordReader extends RecordReader<NullWritable, FaunusVertex> {

    //private static final Log LOG = LogFactory.getLog(JSONRecordReader.class);
    private long start;
    private long pos;
    private long end;
    private LineReader in;
    private final int maxLineLength = Integer.MAX_VALUE;

    private final NullWritable key = NullWritable.get();
    private FaunusVertex value = null;

    public void initialize(final InputSplit genericSplit, final TaskAttemptContext context) throws IOException {
        final FileSplit split = (FileSplit) genericSplit;
        final Configuration job = context.getConfiguration();
        //this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
        this.start = split.getStart();
        this.end = this.start + split.getLength();
        final Path file = split.getPath();
        final CompressionCodec codec = new CompressionCodecFactory(job).getCodec(file);

        // open the file and seek to the start of the split
        final FileSystem fs = file.getFileSystem(job);
        final FSDataInputStream fileIn = fs.open(split.getPath());
        boolean skipFirstLine = false;
        if (codec != null) {
            this.in = new LineReader(codec.createInputStream(fileIn), job);
            this.end = Long.MAX_VALUE;
        } else {
            if (this.start != 0) {
                skipFirstLine = true;
                --this.start;
                fileIn.seek(this.start);
            }
            this.in = new LineReader(fileIn, job);
        }
        if (skipFirstLine) {  // skip first line and re-establish "start".
            this.start += this.in.readLine(new Text(), 0, (int) Math.min((long) Integer.MAX_VALUE, this.end - this.start));
        }
        this.pos = this.start;
    }

    public boolean nextKeyValue() throws IOException {
        if (this.value == null) {
            this.value = new FaunusVertex(-1l);
        }
        int newSize = 0;
        while (this.pos < this.end) {
            final Text text = new Text();
            newSize = this.in.readLine(text, this.maxLineLength, Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), this.maxLineLength));
            this.value = JSONUtility.fromJSON(text.toString());

            if (newSize == 0) {
                break;
            }
            this.pos += newSize;
            if (newSize < this.maxLineLength) {
                break;
            }

            // line too long. try again
            //LOG.info("Skipped line of size " + newSize + " at pos " + (pos - newSize));
        }
        if (newSize == 0) {
            this.value = null;
            return false;
        } else {
            return true;
        }
    }

    @Override
    public NullWritable getCurrentKey() {
        return this.key;
    }

    @Override
    public FaunusVertex getCurrentValue() {
        return this.value;
    }

    public float getProgress() {
        if (this.start == this.end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    public synchronized void close() throws IOException {
        if (this.in != null) {
            this.in.close();
        }
    }
}