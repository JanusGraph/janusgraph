package com.thinkaurelius.titan.core.time;

import java.util.concurrent.TimeUnit;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class SimpleDurationKryoSerializer extends Serializer<SimpleDuration> {

    @Override
    public void write(Kryo kryo, Output output, SimpleDuration object) {
        TimeUnit u = object.getNativeUnit();
        output.writeLong(object.getLength(u));
        output.writeByte((byte)u.ordinal());
    }

    @Override
    public SimpleDuration read(Kryo kryo, Input input, Class<SimpleDuration> type) {
        return new SimpleDuration(input.readLong(), TimeUnit.values()[input.readByte()]);
    }

}
