package com.thinkaurelius.titan.graphdb.test;


import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.core.*;

import static org.junit.Assert.assertEquals;

public class GraphDBExample {

	public static void main(String[] args) {
		if (args.length<1 || args[0].trim().isEmpty()) throw new IllegalArgumentException("Need to specify directory for database!");
		GraphDBExample ex = new GraphDBExample(args[0]);
		ex.createDelete();
	}
	
	
	public GraphDatabase graphdb;
	public GraphTransaction tx;
	public final String homeDir;
	
	public GraphDBExample(String dir) {
		homeDir = dir;
	}
	
	public void open() {
		GraphDatabaseConfiguration config = new GraphDatabaseConfiguration(homeDir);
		graphdb = config.openDatabase();
		tx=graphdb.startTransaction();
	}


	public void close() {
		if (tx!=null) tx.commit();
		graphdb.close();
    }


	
	public void clopen() {
		close();
		open();
	}
	

	public void createDelete() {
		open();
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
		close();
	}
	
	public PropertyType makeWeightPropertyType(String name) {
		return tx.createEdgeType().withName(name).
			category(EdgeCategory.Simple).functional(true).
			dataType(Double.class).										
			makePropertyType();
	}
	
	public PropertyType makeIDPropertyType(String name) {
		return makeIDPropertyType(name,EdgeTypeGroup.DefaultGroup);
	}
	
	public PropertyType makeIDPropertyType(String name, EdgeTypeGroup group) {
		return tx.createEdgeType().withName(name).
			category(EdgeCategory.Simple).functional(true).
			makeKeyed().setIndex(PropertyIndex.Standard).
			dataType(Integer.class).group(group).					
			makePropertyType();
	}

	public RelationshipType makeLabeledRelationshipType(String name, PropertyType key, PropertyType compact) {
		EdgeTypeMaker etmaker = tx.createEdgeType();
		RelationshipType relType = etmaker.withName(name).
                addKeySignature(key).addCompactSignature(compact).
										withDirectionality(Directionality.Directed).
										category(EdgeCategory.Labeled).
										makeRelationshipType();
		return relType;
	}
	
}
