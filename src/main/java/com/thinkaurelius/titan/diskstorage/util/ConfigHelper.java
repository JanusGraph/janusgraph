package com.thinkaurelius.titan.diskstorage.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.exceptions.GraphStorageException;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * Methods which interpret diskstorage-specific configuration options.
 *
 */
public class ConfigHelper {
	
	private static final Logger log = LoggerFactory.getLogger(ConfigHelper.class);

	public static byte[] getRid(Configuration config, Object caller) {
		byte tentativeRid[] = null;
		
		if (config.containsKey(GraphDatabaseConfiguration.INSTANCE_RID_RAW_KEY)) {
			String ridText =
					config.getString(GraphDatabaseConfiguration.INSTANCE_RID_RAW_KEY);
			try {
				tentativeRid = Hex.decodeHex(ridText.toCharArray());
			} catch (DecoderException e) {
				throw new RuntimeException(e);
			}
			
			log.debug("Set rid from hex string: 0x{}" + ridText);
		} else {
			final short lastTwoBytes;
			
			if (config.containsKey(GraphDatabaseConfiguration.INSTANCE_RID_SHORT_KEY)) {
				lastTwoBytes = config.getShort(
						GraphDatabaseConfiguration.INSTANCE_RID_SHORT_KEY);
			} else {
				lastTwoBytes = (short)System.identityHashCode(caller);
			}
			
			byte[] addrBytes;
			try {
				addrBytes = InetAddress.getLocalHost().getAddress();
			} catch (UnknownHostException e) {
				throw new GraphStorageException(e);
			}
			
			tentativeRid = new byte[addrBytes.length + 2];
			System.arraycopy(addrBytes, 0, tentativeRid, 0, addrBytes.length);
			tentativeRid[tentativeRid.length - 2] = (byte)((lastTwoBytes & 0x0000FF00) >> 8);
			tentativeRid[tentativeRid.length - 1] = (byte)(lastTwoBytes & 0x000000FF);
			
			if (log.isDebugEnabled()) {
				log.debug("Set rid: 0x{}" + new String(Hex.encodeHex(tentativeRid)));
			}
		}
		
		return tentativeRid;
	}
}
