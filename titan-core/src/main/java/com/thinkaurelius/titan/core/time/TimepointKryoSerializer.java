package com.thinkaurelius.titan.core.time;

import java.util.concurrent.TimeUnit;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class TimepointKryoSerializer extends Serializer<Timepoint> {

    @Override
    public void write(Kryo kryo, Output output, Timepoint object) {
        TimeUnit u = object.getNativeUnit();
        output.writeLong(object.getTime(u));
        output.writeByte((byte)u.ordinal());
    }

    @Override
    public Timepoint read(Kryo kryo, Input input, Class<Timepoint> type) {
        return new Timepoint(input.readLong(), TimeUnit.values()[input.readByte()]);
    }
}
