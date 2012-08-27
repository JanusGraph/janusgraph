package com.thinkaurelius.faunus.formats.rexster;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A InputSplit that spans a set of vertices.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class RexsterInputSplit extends InputSplit implements Writable {

    private long end = 0;
    private long start = 0;

    public RexsterInputSplit() {
    }

    public RexsterInputSplit(long start, long end) {
        this.start = start;
        this.end = end;
    }

    public String[] getLocations() throws IOException {
        return new String[]{};
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public long getLength() throws IOException {
        return end - start;
    }

    public void readFields(DataInput input) throws IOException {
        start = input.readLong();
        end = input.readLong();
    }

    public void write(DataOutput output) throws IOException {
        output.writeLong(start);
        output.writeLong(end);
    }

    @Override
    public String toString() {
        return String.format("Split at [%s to %s]", this.start, this.end == Long.MAX_VALUE ? "END" : this.end - 1);
    }
}
