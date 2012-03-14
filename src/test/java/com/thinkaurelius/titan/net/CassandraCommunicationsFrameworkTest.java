package com.thinkaurelius.titan.net;

import com.thinkaurelius.titan.configuration.CassandraStorageConfiguration;
import com.thinkaurelius.titan.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.core.GraphTransaction;
import com.thinkaurelius.titan.core.Node;
import com.thinkaurelius.titan.core.query.QueryType;
import com.thinkaurelius.titan.core.query.ResultCollector;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraThriftNodeIDMapper;
import com.thinkaurelius.titan.diskstorage.test.CassandraLocalhostHelper;
import com.thinkaurelius.titan.graphdb.database.GraphDB;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.sendquery.QuerySender;
import com.thinkaurelius.titan.net.server.ForwardTestQT;
import com.thinkaurelius.titan.net.server.ReverseTraversalTestQT;
import com.thinkaurelius.titan.net.server.ReverseTraversalTestQT.TraversalState;
import com.thinkaurelius.titan.net.server.ReverseTraversalTestQT.TraversedEdge;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class CassandraCommunicationsFrameworkTest {

	private static final String STARTING_NODE_ID = "GH.N.TANGO.2631";
	private static final long STARTING_NODE_ID_INTERNAL = 4398046523756L;
	private static final String EXPECTED_NODE_ID = "GH.N.TANGO.1411";
	
	private class TestNode {
		final String addr;
		final String configDirPath;
		final File configDir;
		Kernel kernel;
		GraphDB gdb;
		Serializer serializer;
		NodeID2InetMapper n2i;
		GraphDatabaseConfiguration config;
		
		boolean delete = true;
		
		public TestNode(String addr) {
			this.addr = addr;
			configDirPath = "target" + File.separator + 
				"graphdb-tmp" + File.separator + addr;
			configDir = new File(configDirPath);
		}

		TestNode configure() throws IOException {
			InetSocketAddress listen = 
				new InetSocketAddress(addr, Kernel.getDefaultListenPort());
			
			if (delete) {
				if (configDir.exists()) {
					log.debug("Deleting {}", configDir);
					FileUtils.deleteQuietly(configDir);
				}
				configDir.mkdirs();
			}
			config = new GraphDatabaseConfiguration(configDirPath);
			CassandraStorageConfiguration sc = new CassandraStorageConfiguration();
			sc.setHostname(addr);
			sc.setSelfHostname(addr);
			config.registerClass(HashSet.class);
			config.registerClass(TraversalState.class);
			config.registerClass(TraversedEdge.class);
			config.setStorage(sc);
			config.setReferenceNodeEnabled(true);
			gdb = (GraphDB)config.openDatabase();
			serializer = config.getSerializer();
			n2i = new CassandraThriftNodeIDMapper(
					CassandraStorageConfiguration.DEFAULT_KEYSPACE,
					addr,
					CassandraStorageConfiguration.DEFAULT_PORT);
			kernel = new Kernel(listen, gdb, serializer, n2i);
			kernel.registerQueryType(new ForwardTestQT());
			kernel.registerQueryType(new ReverseTraversalTestQT());
			return this;
		}
		
		TestNode start() {
			kernel.start();
			return this;
		}
		
		TestNode stop() throws InterruptedException {
			kernel.shutdown(1000L);
			config.close();
			return this;
		}
		
		TestNode copyDat(TestNode destination) throws IOException {
			assert configDir.exists();
			assert configDir.isDirectory();
			assert configDir.canRead();
			
			assert destination.configDir.exists();
			assert destination.configDir.isDirectory();
			assert destination.configDir.canWrite();
			
			FileFilter onlyDotDat = new WildcardFileFilter("*.dat");
			
			for (File f : configDir.listFiles(onlyDotDat)) {
				File destFile = new File(destination.configDir, f.getName());
				if (destFile.exists()) {
					log.debug("Deleting {}", destFile);
					destFile.delete();
				}
				FileUtils.copyFile(f, destFile);
				log.debug("Copied {} to {}", f, destFile);
			}
			
			return this;
		}
		
		TestNode setDelete(boolean delete) {
			this.delete = delete;
			return this;
		}
		
	}
	
	private TestNode one;
	private TestNode two;
	
	/*
	 * I set this to true and run once.  That loads Greyhat
	 * into Cassandra and populates target/graphdb-127.0.0.2
	 * with corresponding .dat files.  Then I copy the .dat
	 * files into target/graphdb-127.0.0.1.  Then I set this
	 * to false to use the existing Cassandra Greyhat load
	 * and graphdb configs.  This is ridiculous and labor
	 * intensive.  Since I don't know if it's really necessary
	 * yet, I haven't automated it.
	 */
//	private static final boolean deleteAndReloadData = false;

	private static CassandraLocalhostHelper ch1 = new CassandraLocalhostHelper("127.0.0.1");
	private static CassandraLocalhostHelper ch2 = new CassandraLocalhostHelper("127.0.0.2");

	private static final Logger log =
		LoggerFactory.getLogger(CassandraCommunicationsFrameworkTest.class);
	
	@BeforeClass
	public static void beforeClass() throws InterruptedException {
	}
	
	@AfterClass
	public static void afterClass() throws InterruptedException {
	}
	
	@Before
	public void setUp() throws Exception {
		ch1.setDelete(true);
		ch2.setDelete(true);
		ch1.startCassandra();
		ch2.startCassandra();
		ch1.waitForClusterSize(2);
		
		one = new TestNode("127.0.0.1").configure().start();
		two = new TestNode("127.0.0.2").configure().start();
		

		// Copy .dat files from graphdb2 to graphdb1
		log.debug("Shutting down graph databases");
		one.stop();
		two.stop();
		log.debug("Copying .dat files from gdb 2 to gdb 1");
		two.copyDat(one);
		log.debug("Restarting graph databases");
		one.setDelete(false).configure().start();
		two.setDelete(false).configure().start();
		log.debug("Graph databases restarted");
		Thread.sleep(100L);
	}
	
	@After
	public void tearDown() throws Exception {
		one.stop();
		two.stop();
		ch1.stopCassandra();
		ch2.stopCassandra();
	}
	
	@Test
	public void testSimpleGreyhatForwardedQuery() throws InterruptedException {
		Kernel k = two.kernel;
		QuerySender qs = k.createQuerySender();
		ResultCollector<String> rc = CommunicationsFrameworkTest.printingResultCollector();
		Class<? extends QueryType<String, String>> queryClass =
			ForwardTestQT.class;
		
		// Use hard-coded anchor node id
		long anchorId = STARTING_NODE_ID_INTERNAL; // gdb ID corresponding to STARTING_NODE_ID
		
		// Issue query and wait
		qs.sendQuery(anchorId, "Forwarded=False", queryClass, rc);
		Thread.sleep(3000L);
		
		verify(rc).added("ID=" + EXPECTED_NODE_ID);
		verify(rc, times(1)).added(any());
	}
	
	@Test
	public void testSimpleGreyhatForwardedQueryAfterKeyLookup() throws InterruptedException {
		Kernel k = two.kernel;
		QuerySender qs = k.createQuerySender();
		ResultCollector<String> rc = CommunicationsFrameworkTest.printingResultCollector();
		Class<? extends QueryType<String, String>> queryClass =
			ForwardTestQT.class;
		
		// Lookup anchor node id by the keyed "ID" property
		GraphTransaction lookupAnchorTx = two.gdb.startTransaction();
		Node anchor = lookupAnchorTx.getNodeByKey("ID", STARTING_NODE_ID);
		long anchorId = anchor.getID();
		log.debug("Anchor ID: {}", anchorId);
		lookupAnchorTx.commit();
		
		// Issue query and wait
		qs.sendQuery(anchorId, "Forwarded=False", queryClass, rc);
		Thread.sleep(3000L);
		
		verify(rc).added("ID=" + EXPECTED_NODE_ID);
		verify(rc, times(1)).added(any());
	}
	
	@Test
	public void testSimpleGreyhatReverseTraversal() throws InterruptedException {
		Kernel k = two.kernel;
		QuerySender qs = k.createQuerySender();
		ResultCollector<TraversedEdge> rc = CommunicationsFrameworkTest.printingResultCollector();
		Class<? extends QueryType<TraversalState, TraversedEdge>> queryClass =
			ReverseTraversalTestQT.class;
		
		TraversalState initialState = new TraversalState();
		
		GraphTransaction lookupAnchorTx = two.gdb.startTransaction();
		Node anchor = lookupAnchorTx.getNodeByKey("ID", "GH.N.TANGO.2635");
		long anchorId = anchor.getID();
		log.debug("Anchor ID: {}", anchorId);
		lookupAnchorTx.commit();
		
		qs.sendQuery(anchorId, initialState, queryClass, rc);
		Thread.sleep(3000L);
		
		verify(rc, times(97)).added(any());
	}
	
}

