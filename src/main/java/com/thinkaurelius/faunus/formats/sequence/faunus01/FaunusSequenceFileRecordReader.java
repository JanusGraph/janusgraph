package com.thinkaurelius.faunus.formats.sequence.faunus01;

import com.thinkaurelius.faunus.FaunusVertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Field;

/**
 * Reads a [NullWritable,FaunusVertex] SequenceFile as encoded in Faunus 0.1.z.
 * Writes a [NullWrtiable,FaunusVertex] SequenceFile as encoded in the current Faunus version.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusSequenceFileRecordReader extends RecordReader<NullWritable, FaunusVertex> {

    private final SequenceFileRecordReader<NullWritable, FaunusVertex01> recordReader = new SequenceFileRecordReader<NullWritable, FaunusVertex01>();

    @Override
    public void initialize(final InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {
        this.recordReader.initialize(split, context);
        try {
            final Field inField = SequenceFileRecordReader.class.getDeclaredField("in");
            inField.setAccessible(true);
            final SequenceFile.Reader reader = (SequenceFile.Reader) inField.get(this.recordReader);
            SerializationFactory serializationFactory = new SerializationFactory(context.getConfiguration());
            final Field valClassNameField = SequenceFile.Reader.class.getDeclaredField("valClassName");
            final Field valClassField = SequenceFile.Reader.class.getDeclaredField("valClass");
            final Field valDerserializerField = SequenceFile.Reader.class.getDeclaredField("valDeserializer");
            final Field valInField = SequenceFile.Reader.class.getDeclaredField("valIn");

            valClassNameField.setAccessible(true);
            valClassField.setAccessible(true);
            valDerserializerField.setAccessible(true);
            valInField.setAccessible(true);

            valClassNameField.set(reader, FaunusVertex01.class.getName());
            valClassField.set(reader, FaunusVertex01.class);
            valDerserializerField.set(reader, serializationFactory.getDeserializer(FaunusVertex01.class));
            ((Deserializer) valDerserializerField.get(reader)).open((DataInputStream) valInField.get(reader));
        } catch (Exception e) {
            throw new InterruptedException(e.getMessage());
        }

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return this.recordReader.nextKeyValue();
    }

    @Override
    public NullWritable getCurrentKey() {
        return this.recordReader.getCurrentKey();
    }

    @Override
    public FaunusVertex getCurrentValue() {
        return VertexConverter.buildFaunusVertex(this.recordReader.getCurrentValue());
    }

    @Override
    public float getProgress() throws IOException {
        return this.recordReader.getProgress();
    }

    @Override
    public synchronized void close() throws IOException {
        this.recordReader.close();
    }

}
