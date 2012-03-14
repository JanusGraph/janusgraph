package com.thinkaurelius.titan.diskstorage.cassandra;

import com.thinkaurelius.titan.configuration.CassandraStorageConfiguration;
import com.thinkaurelius.titan.diskstorage.cassandra.thriftpool.CTConnection;
import com.thinkaurelius.titan.diskstorage.cassandra.thriftpool.CTConnectionPool;
import com.thinkaurelius.titan.diskstorage.cassandra.thriftpool.UncheckedGenericKeyedObjectPool;
import com.thinkaurelius.titan.exceptions.GraphStorageException;
import com.thinkaurelius.titan.net.NodeID2InetMapper;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

/**
 * Determines which Cassandra nodes locally replicate data for the
 * binary key represented in the argument to {@link #getInetAddress(long)}.
 *
 * Titan requires that Cassandra use the ByteOrderedPartitioner (BOP).
 * The BOP compares variable-length keys to fixed length tokens in order
 * to determine which Cassandra node(s) should replicate rows identified
 * by which key.  An understanding of how the BOP compares keys and tokens
 * is necessary for this class's implementation.
 *
 * The following discussion applies to the BOP in Cassandra 0.7.0.
 * The BOP represents tokens with the dht.BytesToken class.  This class
 * implements compareTo() by calling FBUtilities.compareUnsigned() on the
 * byte payload of itself and the compared BytesToken.  The byte payloads
 * of the two BytesTokens need not be of the same length.
 * FButilities.compareUnsigned() in turn uses the following algorithm.
 * It bytewise compares the two tokens, using casting to int or long to
 * effectively simulate unsigned comparison, until at least one token is
 * exhausted; if a comparison finds a byte smaller than its counterpart,
 * then the token associated with the small byte comes before the other.
 * If one token runs out of bytes before the other, then run-out token
 * comes before the other.  If both tokens are of the same length and carry
 * identical byte payloads, then they are considered equal.
 *
 * Titan keys stored edges by an eight byte "signature".  The BOP
 * appears to generate 16 byte tokens in practice.  This is supported by the
 * implementation of getRandomToken() in Cassandra's
 * dht.AbstractByteOrderedPartioner, which is inherited by the BOP: it
 * simply uses java.util.Random to generate 16 bytes and returns a
 * BytesToken with that payload.
 *
 * For transmission via Thrift, tokens are encoded as Strings containing
 * hexadecimal numbers.  Similarly, the host associated to a token seems to
 * be transmitted as a String containing the IP address.
 *
 *
 * @author dalaro
 *
 */
public class CassandraThriftNodeIDMapper implements NodeID2InetMapper {

	private final String keyspace;
//	private final String thriftHostname;
//	private final int thriftPort;
	private final String selfHostname;

	private final UncheckedGenericKeyedObjectPool
		<String, CTConnection> pool;

	private static final Logger log =
		LoggerFactory.getLogger(CassandraThriftNodeIDMapper.class);

	public CassandraThriftNodeIDMapper(String keyspace, String thriftHostname, int thriftPort) {
		this(keyspace,
		     thriftHostname,
		     thriftPort,
		     CassandraStorageConfiguration.getDefaultRuntimeSelfHostname(),
		     CassandraStorageConfiguration.DEFAULT_THRIFT_TIMEOUT_MS);
	}

	public CassandraThriftNodeIDMapper(String keyspace, String thriftHostname, int thriftPort, String selfHostname, int timeoutMS) {
		this.keyspace = keyspace;
//		this.thriftHostname = thriftHostname;
//		this.thriftPort = thriftPort;
		this.pool = CTConnectionPool.getPool(thriftHostname, thriftPort, timeoutMS);
		this.selfHostname = selfHostname;
	}

	public CassandraThriftNodeIDMapper(String keyspace) {
		this(keyspace,
			 CassandraStorageConfiguration.getDefaultRuntimeHostname(),
			 CassandraStorageConfiguration.DEFAULT_PORT);
	}

	public InetAddress[] getInetAddress(ByteBuffer bb) {
		return convertHostnamesToAddresses(getHostnames(bb, null));
	}

	@Override
	public InetAddress[] getInetAddress(long nodeId) {
		ByteBuffer bb = ByteBuffer.allocate(8);
		bb.putLong(nodeId);
		bb.flip();
		return convertHostnamesToAddresses(getHostnames(bb, nodeId));
	}

	@Override
	public boolean isNodeLocal(long nodeId) {
		if (null == selfHostname)
			throw new IllegalStateException("Cannot call isNodeLocal() " +
					"when CassandraThriftNodeIDMapper's selfHost is null");

		ByteBuffer bb = ByteBuffer.allocate(8);
		bb.putLong(nodeId);
		bb.flip();
		List<String> nodeHosts = getHostnames(bb, nodeId);
		if (null == nodeHosts) {
			return false;
		}
		for (String a : nodeHosts) {
			if (a.equals(selfHostname))
				return true;
		}
		return false;
	}

	private InetAddress[] convertHostnamesToAddresses(List<String> hns) {
		InetAddress result[] = new InetAddress[hns.size()];
		for (int i = 0; i < result.length; i++)
			result[i] = getByName(hns.get(i));
		return result;
	}


    private List<String> getHostnames(ByteBuffer nodeIdBuf, Long nodeId) {
        throw new UnsupportedOperationException("This method is currently not supported!");
    }

	/* Commented out because the FBUtilities.hexToBytes function no longer exists

	private List<String> getHostnames(ByteBuffer nodeIdBuf, Long nodeId) {
		CTConnection conn = null;

		List<String> result = new LinkedList<String>();

		// TODO cache the TokenRanges in some form, maybe on a tunable 30s default ttl
		try {
			conn = pool.genericBorrowObject(keyspace);
			Cassandra.Client client = conn.getClient();

			assert conn.getTransport().isOpen();

			log.debug("Fetching token ring description from Cassandra");
			for (TokenRange tr : client.describe_ring(keyspace)) {
				// TokenRange startToken is exclusive; endToken is inclusive
				// TokenRange provides tokens as hex strings
				String start = tr.getStart_token();
				String end = tr.getEnd_token();
				log.debug("(" + start + ", " + end + "]");
				byte[] startBytes = FBUtilities.hexToBytes(start);
				byte[] endBytes = FBUtilities.hexToBytes(end);

				// The first and lowest TokenRange gets special treatment because its start token
				// comes lexically _after_ its end token; it is considered to wrap around
				// the maximum token. http://wiki.apache.org/cassandra/Operations#Ring_management
				// It can be conceptualized as encoding the union two normal TokenRanges: (end, +inf) and
				// (-inf, start].

				boolean nodeIdComesAfterStartToken =
					-1 == FBUtilities.compareUnsigned(
							 startBytes, nodeIdBuf.array(),
							 0, nodeIdBuf.arrayOffset(),
							 startBytes.length, nodeIdBuf.remaining());

				boolean nodeIdComesBeforeOrEqualsEndToken =
					-1 != FBUtilities.compareUnsigned(
							endBytes, nodeIdBuf.array(),
							0, nodeIdBuf.arrayOffset(),
							endBytes.length, nodeIdBuf.remaining());

				boolean wrappingRange =
					-1 == FBUtilities.compareUnsigned(
							endBytes, startBytes,
							0, 0,
							endBytes.length, startBytes.length);

				if ((wrappingRange && (nodeIdComesAfterStartToken || nodeIdComesBeforeOrEqualsEndToken)) ||
						nodeIdComesAfterStartToken && nodeIdComesBeforeOrEqualsEndToken) {
					for (String s : tr.getEndpoints()) {
						ByteBuffer bb = ByteBuffer.allocate(8);
						bb.putLong(nodeId).flip();
						String nodeIdHex = FBUtilities.bytesToHex(bb.array());
						log.debug("Cassandra node " + s + " holds 0x"
							+ nodeIdHex + " (long " + nodeId + ")");
						result.add(s);
					}
				}
			}
			return result;
		} catch (TException e) {
			log.error("Failed to fetch Cassandra token ring", e);
			throw new GraphStorageException(e);
		} catch (InvalidRequestException e) {
			log.error("Failed to fetch Cassandra token ring", e);
			throw new GraphStorageException(e);
		} finally {
			pool.genericReturnObject(keyspace, conn);
		}
	}
*/
	private static InetAddress getByName(String hostname) {
		try {
			return InetAddress.getByName(hostname);
		} catch (UnknownHostException e) {
			log.debug("Host resolution failed during Cassandra nodeid-to-host mapping", e);
			throw new GraphStorageException(e);
		}
	}

}
