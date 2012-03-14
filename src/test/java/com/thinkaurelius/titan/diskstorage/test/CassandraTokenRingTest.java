package com.thinkaurelius.titan.diskstorage.test;

import com.thinkaurelius.titan.configuration.CassandraStorageConfiguration;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraThriftNodeIDMapper;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class CassandraTokenRingTest {

	private static final String keyspace = "titantest00";
	private static final int port = CassandraStorageConfiguration.DEFAULT_PORT;
	private static final String testAddr = "127.0.0.1";

	private static CassandraLocalhostHelper ch1 = new CassandraLocalhostHelper("127.0.0.1");
	private static CassandraLocalhostHelper ch2 = new CassandraLocalhostHelper("127.0.0.2");
	
	private static final Logger log =
		LoggerFactory.getLogger(CassandraTokenRingTest.class);
	
	@BeforeClass
	public static void beforeClass() throws InterruptedException {
		ch1.startCassandra();
		ch2.startCassandra();
		ch1.waitForClusterSize(2);
	}
	
	@AfterClass
	public static void afterClass() throws InterruptedException {
		ch1.stopCassandra();
		ch2.stopCassandra();
	}
	
	@Test
	public void twoNodeTokenRingCheck() throws Exception {
		// Create test mapper
		CassandraThriftNodeIDMapper mapper =
			new CassandraThriftNodeIDMapper(keyspace, testAddr, port);
		
		// Check expected mappings
		InetAddress one = InetAddress.getByName("127.0.0.1");
		InetAddress two = InetAddress.getByName("127.0.0.2");
		
		check(two, mapper.getInetAddress(unhex("0000000000000000")));
		check(two, mapper.getInetAddress(unhex("00000400000024ff")));
		check(two, mapper.getInetAddress(unhex("0000040000002500"))); // two's token value
		check(one, mapper.getInetAddress(unhex("0000040000002501")));
		
		check(one, mapper.getInetAddress(unhex("f355fbac0c7fa96e")));
		check(one, mapper.getInetAddress(unhex("f355fbac0c7fa96f"))); // one's token value
		check(two, mapper.getInetAddress(unhex("f355fbac0c7fa970")));
		check(two, mapper.getInetAddress(unhex("ffffffffffffffff")));
	}
	
	private static void check(InetAddress expected, InetAddress actual[]) {
		assertEquals(1, actual.length);
		assertEquals(expected, actual[0]);
	}
	
	private static long unhex(String s) throws IOException {
//		byte[] raw = FBUtilities.hexToBytes(s);
//		ByteBuffer bb = ByteBuffer.allocate(8);
//		bb.put(raw).flip();
//		return bb.getLong();
        return 0;
	}
	
	
//	private static NavigableMap<String, String> loadTokens() throws FileNotFoundException {
//		NavigableMap<String, String> result = new TreeMap<String, String>();
//		String confPath = "target" + File.separator + 
//			"cassandra-tmp" + File.separator + "conf";
//		File f = new File(confPath);
//		for (File cassInstance : f.listFiles()) {
//			String instance = cassInstance.getName();
//			File cassYaml = new File(cassInstance, "cassandra.yaml");
//			if (cassYaml.isFile() && cassYaml.canRead()) {
//				Yaml y = new Yaml();
//				FileInputStream yamlStream = null;
//				try {
//					yamlStream = new FileInputStream(cassYaml);
//					@SuppressWarnings("unchecked")
//					Map<String, ?> m = (Map<String, ?>) y.load(yamlStream);
//					String tok = (String)m.get("initial_token");
//					result.put(instance, tok);
//					log.debug("Loaded Cassandra token {}->{}", instance, tok);
//				} finally {
//					if (null != yamlStream)
//						try {
//							yamlStream.close();
//						} catch (IOException e) {
//							log.error("Failed to close " + cassYaml, e);
//						}
//				}
//			}
//		}
//		return result;
//	}
}
