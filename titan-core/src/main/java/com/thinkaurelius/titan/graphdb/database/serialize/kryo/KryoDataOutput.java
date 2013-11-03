package com.thinkaurelius.titan.graphdb.database.serialize.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticByteBuffer;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;

import java.nio.ByteBuffer;

public class KryoDataOutput implements DataOutput {


    private final Output output;
    private final KryoSerializer serializer;
    private final Kryo kryo;

    KryoDataOutput(Output output) {
        this(output, null);
    }

    KryoDataOutput(Output output, KryoSerializer serializer) {
        Preconditions.checkNotNull(output);
        this.output=output;
        this.serializer = serializer;
        if (serializer !=null) kryo = serializer.getKryo();
        else kryo = null;
    }

    public DataOutput putLong(long val) {
        output.writeLong(val);
        return this;
    }

    public DataOutput putInt(int val) {
        output.writeInt(val);
        return this;
    }

    public DataOutput putShort(short val) {
        output.writeShort(val);
        return this;
    }

    public DataOutput putByte(byte val) {
        output.writeByte(val);
        return this;
    }

    @Override
    public DataOutput putChar(char val) {
        output.writeChar(val);
        return this;
    }

    @Override
    public DataOutput putFloat(float val) {
        output.writeFloat(val);
        return this;
    }

    @Override
    public DataOutput putDouble(double val) {
        output.writeDouble(val);
        return this;
    }

    public DataOutput writeObject(Object object, Class<?> type) {
        Preconditions.checkArgument(serializer != null, "This DataOutput has not been initialized for object writing!");
        Preconditions.checkArgument(serializer.isValidObject(kryo,object), "Cannot de-/serialize object: %s", object);
        kryo.writeObjectOrNull(output, object, type);
        return this;
    }

    public DataOutput writeObjectNotNull(Object object) {
        Preconditions.checkNotNull(object);
        Preconditions.checkArgument(serializer != null, "This DataOutput has not been initialized for object writing!");
        Preconditions.checkArgument(serializer.isValidObject(kryo,object), "Cannot de-/serialize object: %s", object);
        kryo.writeObject(output, object);
        return this;
    }

    public DataOutput writeClassAndObject(Object object) {
        Preconditions.checkArgument(serializer != null, "This DataOutput has not been initialized for object writing!");
        Preconditions.checkArgument(serializer.isValidObject(kryo,object), "Cannot de-/serialize object: %s", object);
        kryo.writeClassAndObject(output, object);
        return this;
    }

    @Override
    public int getPosition() {
        return output.position();
    }

    @Override
    public StaticBuffer getStaticBuffer() {
        return getStaticBufferFlipBytes(0,0);
    }

    @Override
    public StaticBuffer getStaticBufferFlipBytes(int from, int to) {
        byte[] value = output.getBuffer();
        int limit = output.position();
        Preconditions.checkArgument(from>=0 && from<=to);
        Preconditions.checkArgument(to<=limit);
        for (int i=from;i<to;i++) value[i] = (byte)~value[i];
        return new StaticArrayBuffer(value,0,limit);
    }

}
