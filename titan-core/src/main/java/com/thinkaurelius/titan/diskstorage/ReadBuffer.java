package com.thinkaurelius.titan.diskstorage;

import java.nio.ByteBuffer;

/**
 * A Buffer that allows sequential reads and static reads.
 * Should not be used by multiple threads.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface ReadBuffer extends ScanBuffer, StaticBuffer {

    public int getPosition();

    public void movePosition(int delta);

    public ByteBuffer asRelativeByteBuffer();

    public<T> T asRelative(Factory<T> factory);

    public ReadBuffer invert();

}
