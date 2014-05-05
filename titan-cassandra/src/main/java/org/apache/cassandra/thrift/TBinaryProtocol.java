package org.apache.cassandra.thrift;

import org.apache.thrift.transport.TTransport;

/**
 * This is necessary until Astyanax is updated to "officially" support Cassandra 2.0.x.
 *
 * The story is as follows: Cassandra 2.0.x moved to the new version of Thrift (0.9.x)
 * where problem with TBinaryProtocol was fixed, so TBinaryProtocol class was removed as no longer necessary.
 * Astyanax in it's current state still wants to use TBinaryProtocol bundled with Cassandra,
 * so this class is essentially tricking it (Astyanax) into believing that class is still there.
 *
 * No other changes necessary to make Astyanax work with Cassandra 2.0.x because Thrift API is completely in-tact.
 */

@SuppressWarnings("unused")
public class TBinaryProtocol extends org.apache.thrift.protocol.TBinaryProtocol {
    public TBinaryProtocol(TTransport trans) {
        super(trans);
    }
}
