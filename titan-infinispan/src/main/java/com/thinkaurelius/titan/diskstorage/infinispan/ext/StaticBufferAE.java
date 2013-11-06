package com.thinkaurelius.titan.diskstorage.infinispan.ext;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticByteBuffer;

public class StaticBufferAE implements AdvancedExternalizer<StaticBuffer> {

    private static final long serialVersionUID = 55907937232302620L;

    @Override
    public void writeObject(ObjectOutput output, StaticBuffer sb)
            throws IOException {
        ByteBuffer b = sb.asByteBuffer();
        byte[] a = b.array();
        int offset = b.arrayOffset();
        int len = b.remaining();
        
        output.writeInt(len);
        output.write(a, offset, len);
    }

    @Override
    public StaticBuffer readObject(ObjectInput input) throws IOException,
            ClassNotFoundException {
        int len = input.readInt();
        byte[] raw = new byte[len];
        int actualLen = input.read(raw, 0, len);
        Preconditions.checkArgument(actualLen == len);
        return new StaticArrayBuffer(raw);
    }

    @Override
    public Set<Class<? extends StaticBuffer>> getTypeClasses() {
        return ImmutableSet.<Class<? extends StaticBuffer>>of(
                StaticBuffer.class, StaticArrayBuffer.class, StaticByteBuffer.class);
    }

    @Override
    public Integer getId() {
        return 84738478; // TODO make this configurable
    }

}
