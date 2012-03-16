package com.thinkaurelius.titan.net;

import cern.colt.list.AbstractLongList;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.attribute.Interval;
import com.thinkaurelius.titan.core.query.QueryResult;
import com.thinkaurelius.titan.core.query.QueryType;
import com.thinkaurelius.titan.core.query.ResultCollector;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.graphdb.database.GraphDB;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.database.serialize.kryo.KryoSerializer;
import com.thinkaurelius.titan.graphdb.edgequery.InternalEdgeQuery;
import com.thinkaurelius.titan.graphdb.edges.InternalEdge;
import com.thinkaurelius.titan.graphdb.edges.factory.EdgeLoader;
import com.thinkaurelius.titan.graphdb.sendquery.QuerySender;
import com.thinkaurelius.titan.graphdb.transaction.GraphTx;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;

/**
 * Use a mock GraphDB implementation and multiple local instances
 * of the communication framework (listening on different ports) to
 * test the communication framework features in isolation from a
 * particular graph database backend.
 * 
 * @author dalaro
 */
public class CommunicationsFrameworkTest {

	private InetSocketAddress k1Listen, k2Listen, k3Listen;
	private Kernel k1, k2, k3;
	private GraphDB gdb;
	private Serializer serializer;
	private NodeID2InetMapper node2inet;
	
	public CommunicationsFrameworkTest() {
		InetAddress localHost;
		final int port = Kernel.getDefaultListenPort();
		k1Listen = new InetSocketAddress("127.0.0.1", port);
		k2Listen = new InetSocketAddress("127.0.0.2", port);
		k3Listen = new InetSocketAddress("127.0.0.3", port);
	}
	
	@Before
	public void setUp() throws Exception {
		Map<Long, InetAddress> nodeMapping = new HashMap<Long, InetAddress>();
		nodeMapping.put(1L, k1Listen.getAddress());
		nodeMapping.put(2L, k2Listen.getAddress());
		nodeMapping.put(3L, k3Listen.getAddress());
		
		this.gdb = mockGraphDBWithForward();
//		this.serializer = mockSerializer();
		this.node2inet = mockMapper(nodeMapping);
		this.k1 = new Kernel(k1Listen, gdb, mockSerializer(), node2inet);
		this.k2 = new Kernel(k2Listen, gdb, mockSerializer(), node2inet);
		this.k3 = new Kernel(k3Listen, gdb, mockSerializer(), node2inet);
		k1.start();
		k2.start();
		k3.start();
	}
	
	@After
	public void tearDown() throws Exception {
		k1.shutdown(5000L);
		k2.shutdown(5000L);
		k3.shutdown(5000L);
	}
	
	@Test
	public void simpleQueryTest() throws Exception {
		QuerySender qs = k1.createQuerySender();
//		ResultCollector<String> rc = new TestResultCollector();
		ResultCollector<String> rc = printingResultCollector();
		Class<? extends QueryType<String, String>> queryClass = TestQueryType.class;
		
		k1.registerQueryType(new TestQueryType());
		k2.registerQueryType(new TestQueryType());
		
		qs.sendQuery(2L, "sendQuery", queryClass, rc);
		
		// Let query run
		Thread.sleep(5000L);
		
		// Check that the ResultCollector received our results in order
		verify(rc, times(1)).added("Mock result 1/2");
		verify(rc, times(1)).added("Mock result 2/2");
//		inOrder.verify(rc).close();
		
		// Check that ResultCollector received no spurious results
		verify(rc, times(2)).added(any());
		
		// Check that ResultCollector was closed and not aborted
//		verify(rc, times(0)).abort();
//		verify(rc, times(1)).close();
	}
	
	@Test
	public void simpleQueryForwardTest() throws Exception {
		QuerySender qs = k1.createQuerySender();
//		ResultCollector<String> rc = new TestResultCollector();
		ResultCollector<String> rc = printingResultCollector();
		Class<? extends QueryType<String, String>> queryClass = ForwardTestQT.class;
		
		k1.registerQueryType(new ForwardTestQT());
		k2.registerQueryType(new ForwardTestQT());
		k3.registerQueryType(new ForwardTestQT());
		
		qs.sendQuery(2L, "Start", queryClass, rc);
		
		// Let query run
		Thread.sleep(2500L);

		verify(rc).added("Test result from start node");
		verify(rc).added("Test result from finish node");
		verify(rc, times(2)).added(anyObject());
	}
	
	private static class TestQueryType implements QueryType<String, String> {

		private static final Logger log = 
			LoggerFactory.getLogger(TestQueryType.class);
		
		@Override
		public Class<String> queryType() {
			return String.class;
		}

		@Override
		public Class<String> resultType() {
			return String.class;
		}

		@Override
		public void answer(GraphTransaction tx, Node anchor, String query,
				QueryResult<String> result) {
			log.debug("Answering query node=" + anchor + 
					" query=" + query + " result=" + result); 
			result.add("Mock result 1/2");
			result.add("Mock result 2/2");
			tx.commit();
		}	
	}
	
	private static class ForwardTestQT implements QueryType<String, String> {

		private static final Logger log = 
			LoggerFactory.getLogger(ForwardTestQT.class);
		
		@Override
		public Class<String> queryType() {
			return String.class;
		}

		@Override
		public Class<String> resultType() {
			return String.class;
		}

		@Override
		public void answer(GraphTransaction tx, Node anchor, String query,
				QueryResult<String> result) {
			log.debug("Answering query node=" + anchor + 
					" query=" + query + " result=" + result);
			if (query.equals("Start")) {
				result.add("Test result from start node");
				tx.forwardQuery(3, "Finish");
			} else if (query.equals("Finish")) {
				result.add("Test result from finish node");
			} else {
				log.error("Received bogus query load \"" + query + "\"");
			}
		}
		
	}
	
	/*
	 * Static object mocking methods
	 */
	
	public static <T> ResultCollector<T> printingResultCollector() {
		final Logger log = LoggerFactory.getLogger(ResultCollector.class);
		
		ResultCollector<T> rc = mock(ResultCollector.class);
		doAnswer(new Answer() {

			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				log.debug("Result received: " + invocation.getArguments()[0].toString());
				return null;
			}
			
		}).when(rc).added(anyObject());
		return rc;
	}
	
	public static GraphDB mockGraphDB() {
		Node mockNode = mock(Node.class);
		
		GraphTx mockTxn = mock(GraphTx.class);
		when(mockTxn.getNode(anyLong())).thenReturn(mockNode);
		when(mockTxn.isOpen()).thenReturn(false);
		
		GraphDB gdb = mock(GraphDB.class);
		when(gdb.startTransaction(any(GraphTransactionConfig.class),
				any(QuerySender.class))).
			thenReturn(mockTxn);
		
		return gdb;
	}
	
	public static GraphDB mockGraphDBWithForward() {
		/* Calling mock() inside thenAnswer() is forbidden
		 * (see Mockito issue 53), so while this approach
		 * compiles and looks acceptable, Mockito throws an
		 * exception at runtime instead of creating a mockTxn.
		 */
//		GraphDB gdb = mock(GraphDB.class);
//		when(gdb.startTransaction(anyBoolean(), any(QuerySender.class))).thenAnswer(new Answer() {
//			@Override
//			public Object answer(final InvocationOnMock outerInvocation) throws Throwable {
//				GraphTx mockTxn = mock(GraphTx.class);
//				when(mockTxn.getNode(anyLong())).thenReturn(mock(Node.class));
//				when(mockTxn.isOpen()).thenReturn(false);
//				doAnswer(new Answer() {
//
//					@Override
//					public Object answer(InvocationOnMock innerInvocation)
//							throws Throwable {
//						long nodeId = (Long)innerInvocation.getArguments()[0];
//						Object queryLoad = (Object)innerInvocation.getArguments()[1];
//						((QuerySender)(outerInvocation.getArguments()[1])).forwardQuery(nodeId, queryLoad);
//						return null;
//					}
//					
//				}).when(mockTxn).forwardQuery(anyLong(), anyObject());
//				return mockTxn;
//			}
//		});
//		return gdb;
		GraphDB gdb = mock(GraphDB.class);
		when(gdb.startTransaction(any(GraphTransactionConfig.class),
				any(QuerySender.class))).thenAnswer(new Answer<GraphTx>() {

			@Override
			public GraphTx answer(InvocationOnMock invocation) throws Throwable {
				return new TestGraphTransaction((QuerySender)invocation.getArguments()[1]);
			}
			
		});
		return gdb;
	}
	
	public static class TestGraphTransaction implements GraphTx {

		private final QuerySender querySender;
		
		public TestGraphTransaction(QuerySender querySender) {
			this.querySender = querySender;
		}
		
		@Override
		public Node createNode() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Relationship createRelationship(RelationshipType relType,
				Node start, Node end) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Relationship createRelationship(String relType, Node start, Node end) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Property createProperty(PropertyType propType, Node node,
				Object attribute) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Property createProperty(String propType, Node node,
				Object attribute) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Node getNode(long id) {
			return mock(Node.class);
		}

		@Override
		public boolean containsNode(long nodeid) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public EdgeQuery makeEdgeQuery(long nodeid) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Node getNodeByKey(PropertyType type, Object key) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Node getNodeByKey(String name, Object key) {
			// TODO Auto-generated method stub
			return null;
		}
		
		@Override
		public Iterable<? extends Node> getAllNodes() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Iterable<? extends Relationship> getAllRelationships() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public <T, U> void sendQuery(long nodeid, T queryLoad,
				Class<? extends QueryType<T, U>> queryType,
				ResultCollector<U> resultCollector) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void forwardQuery(long nodeid, Object queryLoad) {
			querySender.forwardQuery(nodeid, queryLoad);
		}

		@Override
		public boolean containsEdgeType(String name) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public EdgeType getEdgeType(String name) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public PropertyType getPropertyType(String name) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public RelationshipType getRelationshipType(String name) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public EdgeTypeMaker createEdgeType() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void commit() {
			// TODO Auto-generated method stub
			
		}

        @Override
        public void rollingCommit() {
            // TODO Auto-generated method stub

        }

		@Override
		public void abort() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public boolean isOpen() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean isClosed() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean hasModifications() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public TransactionHandle getTxHandle() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public EdgeLoader getEdgeFactory() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public InternalEdgeQuery makeEdgeQuery(InternalNode n) {
			// TODO Auto-generated method stub
			return null;
		}


		@Override
		public void loadEdges(InternalEdgeQuery query) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public AbstractLongList getRawNeighborhood(
				InternalEdgeQuery query) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void deletedEdge(InternalEdge edge) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void addedEdge(InternalEdge edge) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void loadedEdge(InternalEdge edge) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public boolean isDeletedEdge(InternalEdge e) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public InternalNode getExistingNode(long id) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public long[] getNodeIDsByAttributeFromDisk(PropertyType type,
                                                    Interval<?> interval) {
			// TODO Auto-generated method stub
			return new long[0];
		}

		@Override
		public Set<Node> getNodesByAttribute(PropertyType type, Object attribute) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Set<Node> getNodesByAttribute(String type, Object attribute) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Set<Node> getNodesByAttribute(PropertyType type,
				Interval<?> interval) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Set<Node> getNodesByAttribute(String type, Interval<?> interval) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public GraphTransactionConfig getTxConfiguration() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void flush() {
			// TODO Auto-generated method stub
			
		}
		
	}
	
	public static Serializer mockSerializer() {
		Serializer s = new KryoSerializer();
		return s;
//		DataOutput dataOut = mock(DataOutput.class);
//		when(dataOut.writeObjectNotNull(any(String.class))).thenReturn(dataOut);
//		
//		Serializer s = mock(Serializer.class);
//		when(s.getDataOutput(anyInt(), anyBoolean())).thenReturn(dataOut);
//		when(s.readObjectNotNull(any(ByteBuffer.class), any(Class.class))).
//			thenReturn("deserialization placeholder");
//		
//		return s;
	}
	
	public static NodeID2InetMapper 
	mockMapper(final Map<Long, InetAddress> nodeIdMapping) {
		NodeID2InetMapper m = mock(NodeID2InetMapper.class);
		
		for (Map.Entry<Long, InetAddress> e : nodeIdMapping.entrySet()) {
			when(m.getInetAddress(e.getKey())).
				thenReturn(new InetAddress[]{e.getValue()});
		}
		
		return m;
		
//		return new NodeID2InetMapper() {
//			public InetSocketAddress[] getInetAddress(long nodeId) {
//				return new InetSocketAddress[]{nodeIdMapping.get(nodeId)};
//			}
//		};
	}
}
