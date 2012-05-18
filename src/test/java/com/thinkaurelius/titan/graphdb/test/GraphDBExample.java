package com.thinkaurelius.titan.graphdb.test;


import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.edgetypes.Directionality;
import com.thinkaurelius.titan.graphdb.edgetypes.EdgeCategory;
import com.tinkerpop.blueprints.Direction;

import static org.junit.Assert.assertEquals;

public class GraphDBExample {

	public static void main(String[] args) {
		if (args.length<1 || args[0].trim().isEmpty()) throw new IllegalArgumentException("Need to specify directory for database!");
		GraphDBExample ex = new GraphDBExample(args[0]);
		ex.createDelete();
	}
	
	
	public TitanGraph graphdb;
	public TitanTransaction tx;
	public final String homeDir;
	
	public GraphDBExample(String dir) {
		homeDir = dir;
	}
	
	public void open() {
		graphdb = TitanFactory.open(homeDir);
		tx=graphdb.startThreadTransaction(TransactionConfig.STANDARD);
	}


	public void close() {
		if (tx!=null) tx.commit();
		graphdb.shutdown();
    }


	
	public void clopen() {
		close();
		open();
	}
	

	public void createDelete() {
		open();
		TitanKey weight = makeWeightPropertyType("weight");
		TitanKey id = makeIDPropertyType("id");
		TitanLabel knows = makeLabeledRelationshipType("knows",id,weight);
		
		TitanVertex n1 = tx.addVertex(), n3 = tx.addVertex();
		TitanEdge e=n3.addEdge(knows, n1);
		e.addProperty(id, 111);
		n3.addProperty(id, 445);
		assertEquals(111,e.getProperty(id, Number.class));
		clopen();
		long nid = n3.getID();
		
		n3 = tx.getVertex(nid);
		assertEquals(445,n3.getProperty("id", Number.class));
		e=Iterables.getOnlyElement(n3.getTitanEdges(Direction.OUT, tx.getEdgeLabel("knows")));
		assertEquals(111,e.getProperty(id,Number.class));
		TitanProperty p = Iterables.getOnlyElement(n3.getProperties("id"));
		p.remove();
		n3.addProperty("id", 353);
		clopen();
		
		n3 = tx.getVertex(nid);
		assertEquals(353,n3.getProperty("id"));
		close();
	}
	
	public TitanKey makeWeightPropertyType(String name) {
		return tx.makeType().name(name).
			simple().functional().
			dataType(Double.class).
                makePropertyKey();
	}
	
	public TitanKey makeIDPropertyType(String name) {
		return makeIDPropertyType(name, TypeGroup.DEFAULT_GROUP);
	}
	
	public TitanKey makeIDPropertyType(String name, TypeGroup group) {
		return tx.makeType().name(name).
			simple().functional().
                unique().indexed().
			dataType(Integer.class).group(group).
                makePropertyKey();
	}

	public TitanLabel makeLabeledRelationshipType(String name, TitanKey key, TitanKey compact) {
		TypeMaker etmaker = tx.makeType();
		TitanLabel relType = etmaker.name(name).
                primaryKey(key).signature(compact).directed().
                makeEdgeLabel();
		return relType;
	}
	
}
