package com.thinkaurelius.titan.diskstorage.test;

import com.thinkaurelius.titan.diskstorage.cassandra.CassandraThriftStorageManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;

import static org.junit.Assert.assertEquals;

public class CassandraTokenRingTest {

	private static final String keyspace = "titantest00";
	private static final int port = CassandraThriftStorageManager.DEFAULT_PORT;
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
