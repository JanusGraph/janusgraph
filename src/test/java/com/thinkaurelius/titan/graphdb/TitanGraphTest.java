package com.thinkaurelius.titan.graphdb;


import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.serializer.SpecialInt;
import com.thinkaurelius.titan.graphdb.serializer.SpecialIntSerializer;
import com.thinkaurelius.titan.graphdb.types.InternalTitanType;
import com.thinkaurelius.titan.testutil.MemoryAssess;
import com.thinkaurelius.titan.testutil.RandomGenerator;
import com.tinkerpop.blueprints.Direction;
import static com.tinkerpop.blueprints.Direction.*;
import static org.junit.Assert.*;

import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TitanGraphTest extends TitanGraphTestCommon {

	private Logger log = LoggerFactory.getLogger(TitanGraphTest.class);
	
	public TitanGraphTest(Configuration config) {
		super(config);
	}

	@Test
	public void testOpenClose() { }
	
    @Test
    public void testMultipleDatabases() {
        long memoryBaseline = 0;
        for (int i=0;i<100;i++) {
            graphdb.addVertex(null);
            clopen();
            if (i==1) {
                memoryBaseline = MemoryAssess.getMemoryUse();
                log.debug("Memory before: {}",memoryBaseline/1024);
            }
        }
        close();
        long memoryAfter = MemoryAssess.getMemoryUse();
        log.debug("Memory after: {}",memoryAfter/1024);
        assertTrue(memoryAfter<10*memoryBaseline);
    }
    
    @Test
    public void testBasic() {
        TitanKey weight = makeWeightPropertyKey("weight");
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

        for (TitanProperty prop : n1.getProperties()) {
        	Object o = prop.getAttribute();
        }
        n1.query().relations();
        System.out.println();
        assertEquals(10.5,n1.getProperty(weight));
    }

    @Test
    public void testTypes() {

        config.subset(GraphDatabaseConfiguration.ATTRIBUTE_NAMESPACE)
                .setProperty("attribute10", SpecialInt.class.getCanonicalName());
        config.subset(GraphDatabaseConfiguration.ATTRIBUTE_NAMESPACE)
                .setProperty("serializer10", SpecialIntSerializer.class.getCanonicalName());

        clopen();
        
        TitanLabel friend = tx.makeType().name("friend").directed().functional().
                group(TypeGroup.of(5, "group1")).makeEdgeLabel();
        
        TitanKey id = tx.makeType().name("uid").functional().indexed().unique().
                dataType(String.class).makePropertyKey();

        TitanKey weight = tx.makeType().name("weight").functional().dataType(Double.class).makePropertyKey();
        
        TitanKey number = tx.makeType().name("number").dataType(Number.class).functional().makePropertyKey();

        TitanKey sint = tx.makeType().name("int").dataType(SpecialInt.class).functional().makePropertyKey();

        TitanLabel link = tx.makeType().name("link").simple().unidirected().makeEdgeLabel();

        TitanLabel connect = tx.makeType().name("connect").undirected().primaryKey(id).signature(weight).
                functional(false).makeEdgeLabel();
        
        TitanVertex v1 = tx.addVertex();
        v1.setProperty("uid","v1");
        try {
            v1.addProperty(number,10.5);
            fail();
        } catch (IllegalArgumentException e) {}
        v1.setProperty("int",new SpecialInt(77));

        clopen();
                
        id = tx.getPropertyKey("uid");
        assertEquals(TypeGroup.DEFAULT_GROUP,id.getGroup());
        assertTrue(id.isUnique());
        assertTrue(id.hasIndex());
        assertTrue(id.isFunctional());
        assertEquals(String.class,id.getDataType());
        assertTrue(id.isSimple());
        
        //Basic properties
        
        friend = tx.getEdgeLabel("friend");
        assertEquals(TypeGroup.of(5,"group1"),friend.getGroup());
        assertEquals("friend",friend.getName());
        assertTrue(friend.isDirected());
        assertFalse(friend.isUndirected());
        assertFalse(friend.isUnidirected());
        assertTrue(friend.isEdgeLabel());
        assertFalse(friend.isPropertyKey());
        assertTrue(friend.isFunctional());
        assertTrue(((InternalTitanType)friend).isFunctionalLocking());
        assertFalse(((InternalTitanType) friend).isHidden());
        assertFalse(friend.isSimple());

        connect = tx.getEdgeLabel("connect");
        assertEquals(TypeGroup.getDefaultGroup(),connect.getGroup());
        assertEquals("connect",connect.getName());
        assertFalse(connect.isDirected());
        assertTrue(connect.isUndirected());
        assertFalse(connect.isUnidirected());
        assertTrue(connect.isEdgeLabel());
        assertFalse(connect.isPropertyKey());
        assertTrue(connect.isFunctional());
        assertFalse(((InternalTitanType)connect).isFunctionalLocking());
        assertFalse(((InternalTitanType) connect).isHidden());
        
        link = tx.getEdgeLabel("link");
        assertTrue(link.isUnidirected());
        assertFalse(link.isFunctional());
        assertTrue(link.isSimple());

        weight = tx.getPropertyKey("weight");
        assertEquals(Double.class,weight.getDataType());


        //Failures
        try {
            tx.makeType().name("fid").makePropertyKey();
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            tx.makeType().name("link").simple().unidirected().makeEdgeLabel();
            fail();
        } catch (IllegalArgumentException e) {}
        tx.makeType().name("test").makeEdgeLabel();
        try {
            tx.makeType().makeEdgeLabel();
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            tx.makeType().name("link2").simple().unidirected().
                    primaryKey(id,weight).signature(id).makeEdgeLabel();
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            tx.makeType().name("link2").simple().unidirected().
                    primaryKey(id,weight).makeEdgeLabel();
            fail();
        } catch (IllegalArgumentException e) {}
        tx.makeType().name("link2").unidirected().
                primaryKey(id,weight).makeEdgeLabel();
        
        //Data types and serialization
        TitanVertex v = tx.addVertex();
        v.addProperty(id,"Hello");
        v.addProperty(weight,0.5);
        try {
            v.addProperty(weight,"0.5");
            fail();
        } catch (IllegalArgumentException e) {}



        v1 = tx.getVertex(id,"v1");
        assertEquals(77, ((SpecialInt) v1.getProperty("int")).getValue());

    }

    @Test
    public void testConfiguration() {
        //default type maker
        //check GraphDatabaseConfiguration

    }

    @Test
    public void testTransaction() {
        
    }

	//Add more removal operations, different transaction contexts
	@Test
	public void testCreateDelete() {
		TitanKey weight = makeWeightPropertyKey("weight");
		TitanKey id = makeIntegerUIDPropertyKey("uid");
		TitanLabel knows = makeKeyedEdgeLabel("knows", id, weight);
		
		TitanVertex n1 = tx.addVertex(), n3 = tx.addVertex();
		TitanEdge e=n3.addEdge(knows, n1);
		e.addProperty(id, 111);
		n3.addProperty(id, 445);
		assertEquals(111,e.getProperty(id));
		clopen();
		long nid = n3.getID();
		
		n3 = tx.getVertex(nid);
		assertEquals(445,n3.getProperty("uid"));
		e=Iterables.getOnlyElement(n3.getTitanEdges(Direction.OUT, tx.getEdgeLabel("knows")));
		assertEquals(111,e.getProperty(id));
		TitanProperty p = Iterables.getOnlyElement(n3.getProperties("uid"));
		p.remove();
		n3.addProperty("uid", 353);
		clopen();
		
		n3 = tx.getVertex(nid);
		assertEquals(353,n3.getProperty("uid"));
	}
	
	@Test
	public void testIndexRetrieval() {
		TitanKey id = makeIntegerUIDPropertyKey("uid");
		TitanKey name = makeStringPropertyKey("name");
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
				int nid = ((Number)n.getProperty("uid")).intValue();
				assertEquals(j,nid%mod);
			}
		}
		
	}
	
	@Test
	public void testTypeGroup() {
		TypeGroup g1 = TypeGroup.of(3, "group1");
		TypeGroup g2 = TypeGroup.of(5, "group2");
        try {
            TypeGroup g3 = TypeGroup.of(126,"maxgroup");
            assertEquals(126,g3.getID());
            TypeGroup g4 = TypeGroup.of(127,"failgroup");
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            TypeGroup g4 = TypeGroup.of(0,"failgroup");
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            TypeGroup g4 = TypeGroup.of(-10,"failgroup");
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            TypeGroup g4 = TypeGroup.of(5,null);
            fail();
        } catch (IllegalArgumentException e) {}
        TypeGroup g22 = TypeGroup.of(5,"group2-copy");
        assertEquals(g2,g22);
        TypeGroup defaultgroup = TypeGroup.of(1,"default-copy");
        assertEquals(TypeGroup.DEFAULT_GROUP,defaultgroup);
		TitanKey name = makeStringUIDPropertyKey("name", g1);
		TitanKey id = makeIntegerUIDPropertyKey("uid", g2);
		TitanLabel connect = makeSimpleEdgeLabel("connect", g1);
		TitanLabel knows = makeSimpleEdgeLabel("knows", g2);
		TitanLabel friend = makeSimpleEdgeLabel("friend", g2);
		
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
		
        connect = tx.getEdgeLabel("connect");
        assertEquals(g1,connect.getGroup());
        id = tx.getPropertyKey("uid");
        assertEquals(g2,id.getGroup());
        
		for (int i=0;i<noNodes;i++) {
			TitanVertex n = tx.getVertex("uid", i);
			assertEquals(1,Iterables.size(n.query().group(g1).properties()));
			assertEquals(1,Iterables.size(n.query().group(g2).properties()));
			assertEquals(noEdges,Iterables.size(n.query().group(g1).direction(Direction.OUT).edges()));
			assertEquals(noEdges*2,Iterables.size(n.query().group(g1).edges()));
			assertEquals(noEdges*2,Iterables.size(n.query().group(g2).direction(Direction.OUT).edges()));
			assertEquals(noEdges*4,Iterables.size(n.query().group(g2).edges()));

		}
		
	}

    //Test all element methods: vertex, edge, property, relation, element
	@Test
	public void testCreateAndRetrieveComprehensive() {
		String[] etNames = {"connect","name","weight","knows"};
		TitanLabel connect = makeSimpleEdgeLabel(etNames[0]);
		TitanKey name = makeUniqueStringPropertyKey(etNames[1]);
		TitanKey weight = makeWeightPropertyKey(etNames[2]);
		TitanKey id = makeIntegerUIDPropertyKey("uid");
		TitanLabel knows = makeKeyedEdgeLabel(etNames[3], id, weight);
		
		assertEquals(connect,tx.getEdgeLabel(etNames[0]));
		assertEquals(name,tx.getPropertyKey(etNames[1]));
		assertEquals(knows,tx.getEdgeLabel(etNames[3]));
		assertTrue(knows.isEdgeLabel());
		
		TitanVertex n1 = tx.addVertex();
		TitanVertex n2 = tx.addVertex();
		TitanVertex n3 = tx.addVertex();
        assertNotNull(n1.toString());
		n1.addProperty(name, "Node1");
		n2.addProperty(name, "Node2");
		n3.addProperty(weight, 5.0);
		TitanEdge e = n1.addEdge(connect, n2);
        assertNotNull(e.toString());
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
        assertNotNull(n2.toString());
		assertEquals("Node2",n2.getProperty(name, String.class));
		e = Iterables.getOnlyElement(n2.getTitanEdges(Direction.BOTH, connect));
        assertNotNull(e.toString());
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
    public void testQuery() {
        TitanKey name = tx.makeType().name("name").dataType(String.class).functional().indexed().unique().makePropertyKey();
        TitanKey time = tx.makeType().name("time").dataType(Integer.class).functional().makePropertyKey();
        TitanKey weight = tx.makeType().name("weight").dataType(Double.class).functional().makePropertyKey();
        
        TitanLabel author = tx.makeType().name("author").functional().simple().unidirected().makeEdgeLabel();
        
        TitanLabel connect = tx.makeType().name("connect").primaryKey(time).makeEdgeLabel();
        TitanLabel friend = tx.makeType().name("friend").primaryKey(time,weight).signature(author).makeEdgeLabel();
        TitanLabel knows = tx.makeType().name("knows").primaryKey(author,weight).makeEdgeLabel();
        
        int noVertices = 100;
        TitanVertex[] vs = new TitanVertex[noVertices];
        for (int i=0;i<noVertices;i++) {
            vs[i]=tx.addVertex();
            vs[i].addProperty(name, "v" + i);
        }
        TitanVertex v = vs[0];
        for (int i=1;i<noVertices;i++) {
            TitanEdge e;
            if (i%3==0) {
                e = v.addEdge(connect,vs[i]);
            } else if (i%3==1) {
                e = v.addEdge(friend,vs[i]);
            } else {
                e = v.addEdge(knows,vs[i]);
            }
            e.setProperty("time",i);
            e.setProperty("weight",i%2+0.5);
            e.setProperty("name","e"+i);
            e.addEdge(author,vs[i%5]);
        }

        VertexList vl = null;
        clopen();
        for (int i=0;i<noVertices;i++) vs[i]=tx.getVertex(vs[i].getID());
        v = vs[0];
        assertEquals(noVertices-1,Iterables.size(v.getEdges()));
        
        //Queries
        assertEquals(10,v.query().labels("connect").direction(Direction.OUT).interval("time",3,31).count());
        assertEquals(33,v.query().labels("connect").direction(Direction.OUT).count());
        assertEquals(10,v.query().inMemory().labels("connect").direction(Direction.OUT).interval("time",3,31).vertexIds().size());
        assertEquals(10,Iterables.size(v.query().inMemory().labels("connect").direction(Direction.OUT).interval("time",3,31).vertices()));
        assertEquals(1,v.query().has("time",1).count());
        assertEquals(10,v.query().interval("time",4,14).count());
        assertEquals(20,v.query().labels("friend","connect").direction(Direction.OUT).interval("time",3,33).count());
        assertEquals(30,v.query().labels("friend","connect","knows").direction(Direction.OUT).interval("time",3,33).count());
        assertEquals(5,v.query().labels("friend").direction(Direction.OUT).interval("time",3,33).has("weight",0.5).count());
        assertEquals(1,v.query().labels("friend").direction(Direction.OUT).interval("weight",0.0,10.0).has("time",4).count());
        assertEquals(0,v.query().labels("friend").direction(Direction.OUT).interval("weight",0.0,0.4).has("time",4).count());
        vl = v.query().labels().direction(Direction.OUT).interval("time",3,31).vertexIds();
        vl.sort();
        for (int i=0;i<vl.size();i++) assertEquals(vs[i+3].getID(),vl.getID(i));
        assertEquals(7,v.query().labels("knows").direction(Direction.OUT).has("author",v).count());
        assertEquals(7,v.query().labels("knows").direction(Direction.OUT).has("author",v).interval("weight",0.0,2.0).count());
        assertEquals(3,v.query().labels("knows").direction(Direction.OUT).has("author",v).interval("weight",0.0,1.0).count());
        assertEquals(20,Iterables.size(v.query().inMemory().labels("connect","friend").direction(Direction.OUT).interval("time",3,33).vertices()));
        assertEquals(50,Iterables.size(v.query().inMemory().labels("connect","friend","knows").has("weight",1.5).vertexIds()));


        clopen();
        for (int i=0;i<noVertices;i++) vs[i]=tx.getVertex(vs[i].getID());
        v = vs[0];

        //Same queries as above but without memory loading
        assertEquals(10,v.query().labels("connect").direction(Direction.OUT).interval("time",3,31).count());
        assertEquals(33,v.query().labels("connect").direction(Direction.OUT).count());
        assertEquals(10,v.query().labels("connect").direction(Direction.OUT).interval("time",3,31).vertexIds().size());
        assertEquals(10,Iterables.size(v.query().labels("connect").direction(Direction.OUT).interval("time",3,31).vertices()));
        assertEquals(1,v.query().has("time",1).count());
        assertEquals(10,v.query().interval("time",4,14).count());
        assertEquals(30,v.query().labels("friend","connect","knows").direction(Direction.OUT).interval("time",3,33).count());
        assertEquals(5,v.query().labels("friend").direction(Direction.OUT).interval("time",3,33).has("weight",0.5).count());
        assertEquals(1,v.query().labels("friend").direction(Direction.OUT).interval("weight",0.0,10.0).has("time",4).count());
        assertEquals(0,v.query().labels("friend").direction(Direction.OUT).interval("weight",0.0,0.4).has("time",4).count());
        vl = v.query().labels().direction(Direction.OUT).interval("time",3,31).vertexIds();
        vl.sort();
        for (int i=0;i<vl.size();i++) assertEquals(vs[i+3].getID(),vl.getID(i));
        assertEquals(7,v.query().labels("knows").direction(Direction.OUT).has("author",v).count());
        assertEquals(7,v.query().labels("knows").direction(Direction.OUT).has("author",v).interval("weight",0.0,2.0).count());
        assertEquals(3,v.query().labels("knows").direction(Direction.OUT).has("author",v).interval("weight",0.0,1.0).count());
        assertEquals(20,Iterables.size(v.query().labels("connect","friend").direction(Direction.OUT).interval("time",3,33).vertices()));
        assertEquals(20,Iterables.size(v.query().labels("connect","friend").direction(Direction.OUT).interval("time",3,33).vertexIds()));
        assertEquals(50,Iterables.size(v.query().labels("connect","friend","knows").has("weight",1.5).vertexIds()));


    }

    //Merge above
	public void neighborhoodTest() {
		testCreateAndRetrieveComprehensive();
		log.debug("Neighborhood:");
		TitanVertex n1 = tx.getVertex("name", "Node1");
		TitanQuery q = tx.query(n1.getID()).direction(Direction.OUT).types(tx.getEdgeLabel("connect"));
		VertexList res = q.vertexIds();
		assertEquals(1,res.size());
		TitanVertex n2 = tx.getVertex("name", "Node2");
		assertEquals(n2.getID(),res.getID(0));
	}

    /**
     * Testing using hundreds of objects
     */
	@Test
	public void createAndRetrieveMedium() {
		//Create Graph
		TitanLabel connect = makeSimpleEdgeLabel("connect");
		TitanKey name = makeUniqueStringPropertyKey("name");
		TitanKey weight = makeWeightPropertyKey("weight");
		TitanKey id = makeIntegerUIDPropertyKey("uid");
		TitanLabel knows = makeKeyedEdgeLabel("knows", id, weight);
		
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
			if ((i+1)%100==0) log.debug("Added 100 nodes");
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
		id = tx.getPropertyKey("uid");
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
