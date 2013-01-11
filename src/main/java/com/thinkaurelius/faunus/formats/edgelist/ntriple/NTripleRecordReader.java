package com.thinkaurelius.faunus.formats.edgelist.ntriple;

import com.thinkaurelius.faunus.FaunusElement;
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
import org.apache.log4j.Logger;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;

import java.io.IOException;
import java.io.StringReader;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class NTripleRecordReader extends RecordReader<NullWritable, FaunusElement> {

    private final Logger logger = Logger.getLogger(NTripleRecordReader.class);
    private long start;
    private long pos;
    private long end;
    private LineReader in;
    private int maxLineLength = Integer.MAX_VALUE;
    private final RDFParser parser = Rio.createParser(RDFFormat.NTRIPLES);
    private RDFBlueprintsHandler handler;
    private final Queue<FaunusElement> queue = new LinkedList<FaunusElement>();


    public void initialize(final InputSplit genericSplit, final TaskAttemptContext context) throws IOException {
        final FileSplit split = (FileSplit) genericSplit;
        final Configuration conf = context.getConfiguration();
        this.maxLineLength = conf.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
        this.handler = new RDFBlueprintsHandler(context.getConfiguration());
        this.parser.setRDFHandler(this.handler);

        this.start = split.getStart();
        this.end = this.start + split.getLength();
        final Path file = split.getPath();
        final CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(file);

        // open the file and seek to the start of the split
        final FileSystem fs = file.getFileSystem(conf);
        final FSDataInputStream fileIn = fs.open(split.getPath());
        boolean skipFirstLine = false;
        if (codec != null) {
            this.in = new LineReader(codec.createInputStream(fileIn), conf);
            this.end = Long.MAX_VALUE;
        } else {
            if (this.start != 0) {
                skipFirstLine = true;
                --this.start;
                fileIn.seek(this.start);
            }
            this.in = new LineReader(fileIn, conf);
        }
        if (skipFirstLine) {  // skip first line and re-establish "start".
            this.start += this.in.readLine(new Text(), 0, (int) Math.min((long) Integer.MAX_VALUE, this.end - this.start));
        }
        this.pos = this.start;
    }

    public boolean nextKeyValue() throws IOException {
        if (!this.queue.isEmpty())
            return true;

        int newSize = 0;
        while (this.pos < this.end) {
            final Text text = new Text();
            newSize = this.in.readLine(text, this.maxLineLength, Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), this.maxLineLength));
            if (newSize == 0) {
                break;
            }
            this.pos += newSize;
            if (newSize < this.maxLineLength && text.getLength() > 0) {
                try {
                    this.parser.parse(new StringReader(text.toString()), "http://thinkaurelius.com/baseUri#");
                    this.queue.add(this.handler.getSubject());
                    if (!this.handler.isOnlySubject()) {
                        this.queue.add(this.handler.getPredicate());
                        this.queue.add(this.handler.getObject());
                    }
                } catch (Exception e) {
                    throw new IOException(e.getMessage(), e);
                }
                break;
            }
            // line too long. try again
            logger.info("Skipped line of size " + newSize + " at pos " + (pos - newSize));
        }

        return (newSize != 0);

    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public FaunusElement getCurrentValue() {
        if (!this.queue.isEmpty())
            return this.queue.remove();
        else
            return null;
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