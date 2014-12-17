package com.thinkaurelius.titan.graphdb.database.serialize.kryo;

import com.esotericsoftware.kryo.Kryo;

import java.io.Closeable;

public interface KryoInstanceCache extends Closeable {

    /**
     * Provide a Kryo instance unique to the calling thread.  This method must
     * return distinct instances when called from distinct threads.  This method
     * may return distinct instances when called at various times from a single
     * thread, or it may cache an instance and return it to a single thread on
     * every call.
     *
     * @return a Kryo instance
     */
    public Kryo get();
}
