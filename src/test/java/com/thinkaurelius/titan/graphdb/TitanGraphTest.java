package com.thinkaurelius.titan.graphdb;


import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.testutil.RandomGenerator;
import com.tinkerpop.blueprints.Direction;
import static com.tinkerpop.blueprints.Direction.*;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class TitanGraphTest extends TitanGraphTestCommon {

	private Logger log = LoggerFactory.getLogger(TitanGraphTest.class);
	
	public TitanGraphTest(Configuration config) {
		super(config);
	}

	@Test
	public void testOpenClose() { }
	
    @Test
    public void basicTest() {
        TitanKey weight = makeWeightPropertyType("weight");
        TitanVertex n1 = tx.addVertex();
        n1.addProperty(weight, 10.5);
        clopen();
        long nid = n1.getID();
        assertTrue(tx.containsVertex(nid));
        assertTrue(tx.containsType("weight"));
        weight = tx.getPropertyKey("weight");
        assertEquals(weight.getDataType(),Double.class);
        assertEquals(weight.getName(),"weight");
        n1 = tx.getVertex(nid);
        // When this is commented, our HBase subclass fails this test.
        // When this is uncommented, our HBase subclass passes this test!
        // I suspect this is not supposed to have correctness side effects.
        // Furthermore, when it is commented (HBase fails), only one get()
        // is issued to the HBaseOrderedKeyColumnValueStore.
        //
        // Cassandra fails this test regardless of whether the following
        // lines are commented.  It fails on the same assertion as does HBase.
//        for (TitanProperty prop : n1.properties()) {
//        	Object o = prop.getProperty();
//        }
        n1.query().relations();
        System.out.println();
        assertEquals(10.5,n1.getProperty(weight));

    }
    
	@Test
	public void primitiveCreateAndRetrieve() {
		TitanKey weight = makeWeightPropertyType("weight");
		TitanKey id = makeIDPropertyType("id");
		TitanLabel knows = makeLabeledRelationshipType("knows",id,weight);
		
		TitanVertex n1 = tx.addVertex(), n3 = tx.addVertex();
		TitanEdge e=n3.addEdge(knows, n1);
		e.addProperty(id, 111);
		
		assertEquals(111,e.getProperty(id));
		clopen();
		long nid = n3.getID();
		
		n3 = tx.getVertex(nid);
		
		e=Iterables.getOnlyElement(n3.getTitanEdges(Direction.OUT, tx.getEdgeLabel("knows")));
		assertEquals(111,e.getProperty(id));

	}
	
	@Test
	public void createDelete() {
		TitanKey weight = makeWeightPropertyType("weight");
		TitanKey id = makeIDPropertyType("id");
		TitanLabel knows = makeLabeledRelationshipType("knows",id,weight);
		
		TitanVertex n1 = tx.addVertex(), n3 = tx.addVertex();
		TitanEdge e=n3.addEdge(knows, n1);
		e.addProperty(id, 111);
		n3.addProperty(id, 445);
		assertEquals(111,e.getProperty(id));
		clopen();
		long nid = n3.getID();
		
		n3 = tx.getVertex(nid);
		assertEquals(445,n3.getProperty("id"));
		e=Iterables.getOnlyElement(n3.getTitanEdges(Direction.OUT, tx.getEdgeLabel("knows")));
		assertEquals(111,e.getProperty(id));
		TitanProperty p = Iterables.getOnlyElement(n3.getProperties("id"));
		p.remove();
		n3.addProperty("id", 353);
		clopen();
		
		n3 = tx.getVertex(nid);
		assertEquals(353,n3.getProperty("id"));
	}
	
	@Test
	public void multipleIndexRetrieval() {
		TitanKey id = makeIDPropertyType("id");
		TitanKey name = makeUnkeyedStringPropertyType("name");
		int noNodes = 100; int div = 10; int mod = noNodes/div;
		for (int i=0;i<noNodes;i++) {
			TitanVertex n = tx.addVertex();
			n.addProperty(id, i);
			n.addProperty(name, "Name" + (i % mod));
		}
		clopen();
		for (int j=0;j<mod;j++) {
			Iterable<Vertex> nodes = tx.getVertices("name", "Name" + j);
			assertEquals(div,Iterables.size(nodes));
			for (Vertex n : nodes) {
				int nid = ((Number)n.getProperty("id")).intValue();
				assertEquals(j,nid%mod);
			}
		}
		
	}
	
	@Test
	public void edgeGroupTest() {
		TypeGroup g1 = TypeGroup.of(3, "group1");
		TypeGroup g2 = TypeGroup.of(5, "group2");
		TitanKey name = makeStringIDPropertyType("name",g1);
		TitanKey id = makeIDPropertyType("id",g2);
		TitanLabel connect = makeRelationshipType("connect",g1);
		TitanLabel knows = makeRelationshipType("knows",g2);
		TitanLabel friend = makeRelationshipType("friend",g2);
		
		int noNodes = 100;
		TitanVertex nodes[] = new TitanVertex[noNodes];
		for (int i=0;i<noNodes;i++) {
			nodes[i] = tx.addVertex();
			nodes[i].addProperty(name, "Name" + (i));
			nodes[i].addProperty(id, i);
		}
		
		int noEdges = 4;
		for (int i=0;i<noNodes;i++) {
			TitanVertex n = nodes[i];
			for (int j =1;j<=noEdges;j++) {
				n.addEdge(connect, nodes[wrapAround(i + j, noNodes)]);
				n.addEdge(knows, nodes[wrapAround(i + j, noNodes)]);
				n.addEdge(friend, nodes[wrapAround(i + j, noNodes)]);
			}
		}
		
		clopen();
		
		for (int i=0;i<noNodes;i++) {
			TitanVertex n = tx.getVertex("id", i);
			assertEquals(1,Iterables.size(n.query().group(g1).properties()));
			assertEquals(1,Iterables.size(n.query().group(g2).properties()));
			assertEquals(noEdges,Iterables.size(n.query().group(g1).direction(Direction.OUT).edges()));
			assertEquals(noEdges*2,Iterables.size(n.query().group(g1).edges()));
			assertEquals(noEdges*2,Iterables.size(n.query().group(g2).direction(Direction.OUT).edges()));
			assertEquals(noEdges*4,Iterables.size(n.query().group(g2).edges()));

		}
		
	}
	
	
	@Test
	public void createAndRetrieveSimple() {
		String[] etNames = {"connect","name","weight","knows"};
		TitanLabel connect = makeRelationshipType(etNames[0]);
		TitanKey name = makeStringPropertyType(etNames[1]);
		TitanKey weight = makeWeightPropertyType(etNames[2]);
		TitanKey id = makeIDPropertyType("id");
		TitanLabel knows = makeLabeledRelationshipType(etNames[3],id,weight);
		
		assertEquals(connect,tx.getEdgeLabel(etNames[0]));
		assertEquals(name,tx.getPropertyKey(etNames[1]));
		assertEquals(knows,tx.getEdgeLabel(etNames[3]));
		assertTrue(knows.isEdgeLabel());
		
		TitanVertex n1 = tx.addVertex();
		TitanVertex n2 = tx.addVertex();
		TitanVertex n3 = tx.addVertex();
		n1.addProperty(name, "Node1");
		n2.addProperty(name, "Node2");
		n3.addProperty(weight, 5.0);
		TitanEdge e = n1.addEdge(connect, n2);
		assertEquals(n1,e.getVertex(OUT));
		assertEquals(n2,e.getVertex(IN));
		e = n2.addEdge(knows, n3);
		e.addProperty(weight, 3.0);
		e.addProperty(name, "HasProperties TitanRelation");
		e=n3.addEdge(knows, n1);
		n3.addEdge(connect, n3);
		e.addProperty(id, 111);
		assertEquals(4,Iterables.size(n3.getEdges()));
		
		clopen();
		
		connect = tx.getEdgeLabel(etNames[0]);
        assertTrue(connect.isSimple());
		assertEquals(connect.getName(),etNames[0]);
        assertTrue(connect.isDirected());
		name = tx.getPropertyKey(etNames[1]);
		assertTrue(name.isUnique());
		assertTrue(name.hasIndex());
		log.debug("Loaded edge types");
		n2 = tx.getVertex(name, "Node2");
		assertEquals("Node2",n2.getProperty(name, String.class));
		e = Iterables.getOnlyElement(n2.getTitanEdges(Direction.BOTH, connect));
		n1 = e.getVertex(OUT);
		log.debug("Retrieved node!");
		assertEquals(n1,e.getVertex(OUT));
		assertEquals(n2,e.getVertex(IN));
		
		log.debug("First:");
		assertEquals(e,Iterables.getOnlyElement(n2.getEdges(Direction.IN)));
		log.debug("Second:");
		assertEquals(e,Iterables.getOnlyElement(n1.getEdges(Direction.OUT)));
		
		assertEquals(1,Iterables.size(n2.getTitanEdges(Direction.BOTH,tx.getEdgeLabel("knows"))));
		
		assertEquals(1,Iterables.size(n1.getTitanEdges(Direction.BOTH,tx.getEdgeLabel("knows"))));
		assertEquals(2,Iterables.size(n1.getEdges()));

		log.debug("Third:");
		assertEquals(e,Iterables.getOnlyElement(n2.getEdges(Direction.IN, "connect")));
		log.debug("Four:");
		assertEquals(e,Iterables.getOnlyElement(n1.getTitanEdges(Direction.OUT, connect)));
		
		log.debug("Fith:");
		assertEquals(e,Iterables.getOnlyElement(n2.getEdges(Direction.BOTH,"connect")));
		log.debug("Sixth:");
		assertEquals(e,Iterables.getOnlyElement(n1.getTitanEdges(Direction.BOTH,connect)));
		
		e=Iterables.getOnlyElement(n2.getTitanEdges(Direction.OUT,tx.getEdgeLabel("knows")));
		assertEquals(3.0,e.getProperty(weight));
		assertEquals("HasProperties TitanRelation",e.getProperty(name, String.class));
		n3=e.getVertex(IN);
		
		e=Iterables.getOnlyElement(n3.getTitanEdges(Direction.OUT,tx.getEdgeLabel("knows")));
		assertEquals(111,e.getProperty(id));
		
		assertEquals(4,Iterables.size(n3.getEdges()));
				
		//Delete Edges, create new ones		
		e=Iterables.getOnlyElement(n2.getTitanEdges(Direction.OUT,tx.getEdgeLabel("knows")));
		e.remove();
		assertEquals(0,Iterables.size(n2.getTitanEdges(Direction.BOTH,tx.getEdgeLabel("knows"))));
		assertEquals(1,Iterables.size(n3.getTitanEdges(Direction.BOTH,tx.getEdgeLabel("knows"))));
		
		e = n2.addEdge(knows, n1);
		e.addProperty(weight, 111.5);
		e.addProperty(name, "New TitanRelation");
		assertEquals(1,Iterables.size(n2.getEdges(Direction.BOTH,"knows")));
		assertEquals(2,Iterables.size(n1.getEdges(Direction.BOTH,"knows")));
		
		clopen();
		n2 = tx.getVertex(name, "Node2");
		e=Iterables.getOnlyElement(n2.getTitanEdges(Direction.OUT,tx.getEdgeLabel("knows")));
		assertEquals("New TitanRelation",e.getProperty(tx.getPropertyKey("name"),String.class));
		assertEquals(111.5,e.getProperty("weight",Double.class).doubleValue(),0.01);
		
	}
	
	@Test
	public void neighborhoodTest() {
		createAndRetrieveSimple();
		log.debug("Neighborhood:");
		TitanVertex n1 = tx.getVertex("name", "Node1");
		TitanQuery q = tx.query(n1.getID()).direction(Direction.OUT).types(tx.getEdgeLabel("connect"));
		VertexList res = q.vertexIds();
		assertEquals(1,res.size());
		TitanVertex n2 = tx.getVertex("name", "Node2");
		assertEquals(n2.getID(),res.getID(0));
	}

	
	@Test
	public void createAndRetrieveMedium() {
		//Create Graph
		TitanLabel connect = makeRelationshipType("connect");
		TitanKey name = makeStringPropertyType("name");
		TitanKey weight = makeWeightPropertyType("weight");
		TitanKey id = makeIDPropertyType("id");
		TitanLabel knows = makeLabeledRelationshipType("knows",id,weight);
		
		//Create Nodes
		int noNodes = 500;
		String[] names = new String[noNodes];
		int[] ids = new int[noNodes];
		TitanVertex[] nodes = new TitanVertex[noNodes];
		for (int i=0;i<noNodes;i++) {
			names[i]=RandomGenerator.randomString();
			ids[i] = RandomGenerator.randomInt(1, Integer.MAX_VALUE / 2);
			nodes[i] = tx.addVertex();
			nodes[i].addProperty(name, names[i]);
			nodes[i].addProperty(id, ids[i]);
			if (i%1000==999) log.debug(".");
		}
		log.debug("Nodes created");
		int[] connectOff = {-100, -34, -4, 10, 20};
		int[] knowsOff = {-400, -18, 8, 232, 334};
		for (int i=0;i<noNodes;i++) {
			TitanVertex n = nodes[i];
			for (int c : connectOff) {
				n.addEdge(connect, nodes[wrapAround(i + c, noNodes)]);
			}
			for (int k : knowsOff) {
				TitanVertex n2 = nodes[wrapAround(i+k,noNodes)];
				TitanEdge r = n.addEdge(knows, n2);
				r.addProperty(id, n.getProperty(id,Number.class).intValue() + n2.getProperty(id,Number.class).intValue());
				r.addProperty(weight, k * 1.5);
				r.addProperty(name, i + "-" + k);
			}
			if (i%100==99) log.debug(".");
		}
		
		clopen();
		
		nodes = new TitanVertex[noNodes];
		name = tx.getPropertyKey("name");
		assertEquals("name",name.getName());
		id = tx.getPropertyKey("id");
		assertTrue(id.isFunctional());
		for (int i=0;i<noNodes;i++) {
			TitanVertex n = tx.getVertex(id, ids[i]);
			assertEquals(n,tx.getVertex(name, names[i]));
			assertEquals(names[i],n.getProperty(name, String.class));
			nodes[i]=n;
		}
		knows = tx.getEdgeLabel("knows");
		assertTrue(!knows.isSimple());
		for (int i=0;i<noNodes;i++) {
			TitanVertex n = nodes[i];
			assertEquals(connectOff.length+knowsOff.length,Iterables.size(n.getEdges(Direction.OUT)));
			assertEquals(connectOff.length,Iterables.size(n.getEdges(Direction.OUT,"connect")));
			assertEquals(connectOff.length*2,Iterables.size(n.getTitanEdges(Direction.BOTH,tx.getEdgeLabel("connect"))));
			assertEquals(knowsOff.length*2,Iterables.size(n.getTitanEdges(Direction.BOTH,knows)),i);
			
			assertEquals(connectOff.length+knowsOff.length+2,Iterables.size(n.query().direction(Direction.OUT).relations()));
			for (TitanEdge r : n.getTitanEdges(Direction.OUT,knows)) {
				TitanVertex n2 = r.getOtherVertex(n);
				int idsum = n.getProperty(id,Number.class).intValue()+n2.getProperty(id,Number.class).intValue();
				assertEquals(idsum,r.getProperty(id,Number.class).intValue());
				double k = r.getProperty(weight,Number.class).doubleValue()/1.5;
				int ki = (int)k;
				assertEquals(i+"-"+ki,r.getProperty(name, String.class));
			}
		}
	}
	
}
