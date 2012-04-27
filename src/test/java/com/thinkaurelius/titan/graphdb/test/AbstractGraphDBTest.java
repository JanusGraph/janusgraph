package com.thinkaurelius.titan.graphdb.test;


import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.util.test.RandomGenerator;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class AbstractGraphDBTest extends AbstractGraphDBTestCommon {

	private Logger log = LoggerFactory.getLogger(AbstractGraphDBTest.class);
	
	public AbstractGraphDBTest(GraphDatabaseConfiguration config) {
		super(config);
	}

	@Test
	public void testOpenClose() { }
	
    @Test
    public void basicTest() {
        PropertyType weight = makeWeightPropertyType("weight");
        Node n1 = tx.createNode();
        n1.createProperty(weight,10.5);
        clopen();
        long nid = n1.getID();
        assertTrue(tx.containsNode(nid));
        n1 = tx.getNode(nid);
        // When this is commented, our HBase subclass fails this test.
        // When this is uncommented, our HBase subclass passes this test!
        // I suspect this is not supposed to have correctness side effects.
        // Furthermore, when it is commented (HBase fails), only one get()
        // is issued to the HBaseOrderedKeyColumnValueStore.
        //
        // Cassandra fails this test regardless of whether the following
        // lines are commented.  It fails on the same assertion as does HBase.
//        for (Property prop : n1.getProperties()) {
//        	Object o = prop.getAttribute();
//        }
        n1.edgeQuery().getEdges();
        System.out.println();
        assertEquals(10.5,n1.getNumber(weight));

    }
    
	@Test
	public void primitiveCreateAndRetrieve() {
		PropertyType weight = makeWeightPropertyType("weight");
		PropertyType id = makeIDPropertyType("id");
		RelationshipType knows = makeLabeledRelationshipType("knows",id,weight);
		
		Node n1 = tx.createNode(), n3 = tx.createNode();
		Relationship e=n3.createRelationship(knows, n1);
		e.createProperty(id, 111);
		
		assertEquals(111,e.getNumber(id));
		clopen();
		long nid = n3.getID();
		
		n3 = tx.getNode(nid);
		
		e=Iterators.getOnlyElement(n3.getRelationshipIterator(tx.getRelationshipType("knows"), Direction.Out));
		assertEquals(111,e.getNumber(id));

	}
	
	@Test
	public void createDelete() {
		PropertyType weight = makeWeightPropertyType("weight");
		PropertyType id = makeIDPropertyType("id");
		RelationshipType knows = makeLabeledRelationshipType("knows",id,weight);
		
		Node n1 = tx.createNode(), n3 = tx.createNode();
		Relationship e=n3.createRelationship(knows, n1);
		e.createProperty(id, 111);
		n3.createProperty(id, 445);
		assertEquals(111,e.getNumber(id));
		clopen();
		long nid = n3.getID();
		
		n3 = tx.getNode(nid);
		assertEquals(445,n3.getNumber("id"));
		e=Iterators.getOnlyElement(n3.getRelationshipIterator(tx.getRelationshipType("knows"), Direction.Out));
		assertEquals(111,e.getNumber(id));
		Property p = Iterables.getOnlyElement(n3.getProperties("id"));
		p.delete();
		n3.createProperty("id", 353);
		clopen();
		
		n3 = tx.getNode(nid);
		assertEquals(353,n3.getNumber("id"));
	}
	
	@Test
	public void multipleIndexRetrieval() {
		PropertyType id = makeIDPropertyType("id");
		PropertyType name = makeUnkeyedStringPropertyType("name");
		int noNodes = 100; int div = 10; int mod = noNodes/div;
		for (int i=0;i<noNodes;i++) {
			Node n = tx.createNode();
			n.createProperty(id, i);
			n.createProperty(name, "Name"+(i%mod));
		}
		clopen();
		for (int j=0;j<mod;j++) {
			Set<Node> nodes = tx.getNodesByAttribute("name", "Name"+j);
			assertEquals(div,nodes.size());
			for (Node n : nodes) {
				int nid = n.getNumber("id").intValue();
				assertEquals(j,nid%mod);
			}
		}
		
	}
	
	@Test
	public void edgeGroupTest() {
		EdgeTypeGroup g1 = EdgeTypeGroup.of(3, "group1");
		EdgeTypeGroup g2 = EdgeTypeGroup.of(5, "group2");
		PropertyType name = makeStringIDPropertyType("name",g1);
		PropertyType id = makeIDPropertyType("id",g2);
		RelationshipType connect = makeRelationshipType("connect",g1);
		RelationshipType knows = makeRelationshipType("knows",g2);
		RelationshipType friend = makeRelationshipType("friend",g2);
		
		int noNodes = 100;
		Node nodes[] = new Node[noNodes];
		for (int i=0;i<noNodes;i++) {
			nodes[i] = tx.createNode();
			nodes[i].createProperty(name, "Name"+(i));
			nodes[i].createProperty(id, i);
		}
		
		int noEdges = 4;
		for (int i=0;i<noNodes;i++) {
			Node n = nodes[i];
			for (int j =1;j<=noEdges;j++) {
				n.createRelationship(connect, nodes[wrapAround(i+j,noNodes)]);
				n.createRelationship(knows, nodes[wrapAround(i+j,noNodes)]);
				n.createRelationship(friend, nodes[wrapAround(i+j,noNodes)]);
			}
		}
		
		clopen();
		
		for (int i=0;i<noNodes;i++) {
			Node n = tx.getNodeByKey("id", i);
			assertEquals(1,Iterables.size(n.getProperties(g1)));
			assertEquals(1,Iterables.size(n.getProperties(g2)));
			assertEquals(noEdges,Iterables.size(n.getRelationships(g1, Direction.Out)));
			assertEquals(noEdges*2,Iterables.size(n.getRelationships(g1)));
			assertEquals(noEdges*2,Iterables.size(n.getRelationships(g2, Direction.Out)));
			assertEquals(noEdges*4,Iterables.size(n.getRelationships(g2)));

		}
		
	}
	
	
	@Test
	public void createAndRetrieveSimple() {
		String[] etNames = {"connect","name","weight","knows"};
		RelationshipType connect = makeRelationshipType(etNames[0]);
		PropertyType name = makeStringPropertyType(etNames[1]);
		PropertyType weight = makeWeightPropertyType(etNames[2]);
		PropertyType id = makeIDPropertyType("id");
		RelationshipType knows = makeLabeledRelationshipType(etNames[3],id,weight);
		
		assertEquals(connect,tx.getRelationshipType(etNames[0]));
		assertEquals(name,tx.getPropertyType(etNames[1]));
		assertEquals(knows,tx.getRelationshipType(etNames[3]));
		assertTrue(knows.isRelationshipType());
		
		Node n1 = tx.createNode();
		Node n2 = tx.createNode();
		Node n3 = tx.createNode();
		n1.createProperty(name, "Node1");
		n2.createProperty(name, "Node2");
		n3.createProperty(weight , 5.0);
		Relationship e = n1.createRelationship(connect, n2);
		assertEquals(n1,e.getStart());
		assertEquals(n2,e.getEnd());
		e = n2.createRelationship(knows, n3);
		e.createProperty(weight, 3.0);
		e.createProperty(name,"Labeled Edge");
		e=n3.createRelationship(knows, n1);
		n3.createRelationship(connect,n3);
		e.createProperty(id, 111);
		assertEquals(3,Iterables.size(n3.getRelationships()));
		
		clopen();
		
		connect = tx.getRelationshipType(etNames[0]);
		assertEquals(connect.getCategory(),EdgeCategory.Simple);
		assertEquals(connect.getName(),etNames[0]);
		assertEquals(connect.getDirectionality(),Directionality.Directed);
		name = tx.getPropertyType(etNames[1]);
		assertTrue(name.isKeyed());
		assertTrue(name.hasIndex());
		log.debug("Loaded edge types");
		n2 = tx.getNodeByKey(name, "Node2");
		assertEquals("Node2",n2.getAttribute(name, String.class));
		e = Iterators.getOnlyElement(n2.getRelationshipIterator(connect));
		n1 = e.getStart();
		log.debug("Retrieved node!");
		assertEquals(n1,e.getStart());
		assertEquals(n2,e.getEnd());
		
		log.debug("First:");
		assertEquals(e,Iterators.getOnlyElement(n2.getRelationshipIterator(Direction.In)));
		log.debug("Second:");
		assertEquals(e,Iterators.getOnlyElement(n1.getRelationshipIterator(Direction.Out)));
		
		assertEquals(1,Iterables.size(n2.getRelationships(tx.getRelationshipType("knows"))));
		
		assertEquals(1,Iterables.size(n1.getRelationships(tx.getRelationshipType("knows"))));
		assertEquals(2,Iterables.size(n1.getRelationships()));

		log.debug("Third:");
		assertEquals(e,Iterators.getOnlyElement(n2.getRelationshipIterator("connect", Direction.In)));
		log.debug("Four:");
		assertEquals(e,Iterators.getOnlyElement(n1.getRelationshipIterator(connect, Direction.Out)));
		
		log.debug("Fith:");
		assertEquals(e,Iterators.getOnlyElement(n2.getRelationshipIterator("connect")));
		log.debug("Sixth:");
		assertEquals(e,Iterators.getOnlyElement(n1.getRelationshipIterator(connect)));
		
		e=Iterators.getOnlyElement(n2.getRelationshipIterator(tx.getRelationshipType("knows"), Direction.Out));
		assertEquals(3.0,e.getNumber(weight));
		assertEquals("Labeled Edge",e.getAttribute(name, String.class));
		n3=e.getEnd();
		
		e=Iterators.getOnlyElement(n3.getRelationshipIterator(tx.getRelationshipType("knows"), Direction.Out));
		assertEquals(111,e.getNumber(id));
		
		assertEquals(3,Iterables.size(n3.getRelationships()));
				
		//Delete Edges, create new ones		
		e=Iterators.getOnlyElement(n2.getRelationshipIterator(tx.getRelationshipType("knows"), Direction.Out));
		e.delete();
		assertEquals(0,Iterables.size(n2.getRelationships(tx.getRelationshipType("knows"))));
		assertEquals(1,Iterables.size(n3.getRelationships(tx.getRelationshipType("knows"))));
		
		e = n2.createRelationship(knows, n1);
		e.createProperty(weight, 111.5);
		e.createProperty(name,"New Edge");
		assertEquals(1,Iterables.size(n2.getRelationships("knows")));
		assertEquals(2,Iterables.size(n1.getRelationships("knows")));
		
		clopen();
		n2 = tx.getNodeByKey(name, "Node2");
		e=Iterators.getOnlyElement(n2.getRelationshipIterator(tx.getRelationshipType("knows"), Direction.Out));
		assertEquals("New Edge",e.getString(tx.getPropertyType("name")));
		assertEquals(111.5,e.getNumber("weight").doubleValue(),0.01);
		
	}
	
	@Test
	public void neighborhoodTest() {
		createAndRetrieveSimple();
		log.debug("Neighborhood:");
		Node n1 = tx.getNodeByKey("name", "Node1");
		EdgeQuery q = tx.makeEdgeQuery(n1.getID()).inDirection(Direction.Out).withEdgeType(tx.getRelationshipType("connect"));
		NodeList res = q.getNeighborhood();
		assertEquals(1,res.size());
		Node n2 = tx.getNodeByKey("name", "Node2");
		assertEquals(n2.getID(),res.getID(0));
	}

	
	@Test
	public void createAndRetrieveMedium() {
		//Create Graph
		RelationshipType connect = makeRelationshipType("connect");
		PropertyType name = makeStringPropertyType("name");
		PropertyType weight = makeWeightPropertyType("weight");
		PropertyType id = makeIDPropertyType("id");
		RelationshipType knows = makeLabeledRelationshipType("knows",id,weight);
		
		//Create Nodes
		int noNodes = 500;
		String[] names = new String[noNodes];
		int[] ids = new int[noNodes];
		Node[] nodes = new Node[noNodes];
		for (int i=0;i<noNodes;i++) {
			names[i]=RandomGenerator.randomString();
			ids[i] = RandomGenerator.randomInt(1, Integer.MAX_VALUE/2);
			nodes[i] = tx.createNode();
			nodes[i].createProperty(name, names[i]);
			nodes[i].createProperty(id, ids[i]);
			if (i%1000==999) log.debug(".");
		}
		log.debug("Nodes created");
		int[] connectOff = {-100, -34, -4, 10, 20};
		int[] knowsOff = {-400, -18, 8, 232, 334};
		for (int i=0;i<noNodes;i++) {
			Node n = nodes[i];
			for (int c : connectOff) {
				n.createRelationship(connect, nodes[wrapAround(i+c,noNodes)]);
			}
			for (int k : knowsOff) {
				Node n2 = nodes[wrapAround(i+k,noNodes)];
				Relationship r = n.createRelationship(knows, n2);
				r.createProperty(id, n.getNumber(id).intValue()+n2.getNumber(id).intValue());
				r.createProperty(weight, k*1.5);
				r.createProperty(name, i+"-"+k);
			}
			if (i%100==99) log.debug(".");
		}
		
		clopen();
		
		nodes = new Node[noNodes];
		name = tx.getPropertyType("name");
		assertEquals("name",name.getName());
		id = tx.getPropertyType("id");
		assertTrue(id.isFunctional());
		for (int i=0;i<noNodes;i++) {
			Node n = tx.getNodeByKey(id, ids[i]);
			assertEquals(n,tx.getNodeByKey(name, names[i]));
			assertEquals(names[i],n.getAttribute(name, String.class));
			nodes[i]=n;
		}
		knows = tx.getRelationshipType("knows");
		assertEquals(knows.getCategory(),EdgeCategory.Labeled);
		for (int i=0;i<noNodes;i++) {
			Node n = nodes[i];
			assertEquals(connectOff.length+knowsOff.length,Iterables.size(n.getRelationships(Direction.Out)));
			assertEquals(connectOff.length,Iterables.size(n.getRelationships(tx.getRelationshipType("connect"),Direction.Out)));
			assertEquals(connectOff.length*2,Iterables.size(n.getRelationships(tx.getRelationshipType("connect"))));
			assertEquals(knowsOff.length*2,Iterables.size(n.getRelationships(knows)),i);
			
			assertEquals(connectOff.length+knowsOff.length+2,Iterables.size(n.getEdges(Direction.Out)));
			for (Relationship r : n.getRelationships(knows,Direction.Out)) {
				Node n2 = r.getOtherNode(n);
				int idsum = n.getNumber(id).intValue()+n2.getNumber(id).intValue();
				assertEquals(idsum,r.getNumber(id).intValue());
				double k = r.getNumber(weight).doubleValue()/1.5;
				int ki = (int)k;
				assertEquals(i+"-"+ki,r.getAttribute(name, String.class));
			}
		}
	}
	
}
