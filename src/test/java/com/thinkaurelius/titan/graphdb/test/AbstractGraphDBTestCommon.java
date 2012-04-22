package com.thinkaurelius.titan.graphdb.test;


import com.thinkaurelius.titan.DiskgraphTest;
import com.thinkaurelius.titan.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.core.*;
import org.junit.After;
import org.junit.Before;

public abstract class AbstractGraphDBTestCommon {

	public GraphDatabaseConfiguration config;
	public GraphDatabase graphdb;
	public GraphTransaction tx;
	
	public AbstractGraphDBTestCommon(GraphDatabaseConfiguration config) {
		this.config = config;
	}
	
	@Before
	public void setUp() throws Exception {
		DiskgraphTest.deleteHomeDir();
		open();
	}
	
	@After
	public void tearDown() throws Exception {
		close();
	}
	
	public void open() {
		graphdb = config.openDatabase();
		tx = graphdb.startTransaction();
	}
	
	public void close() {
		if (null != tx && tx.isOpen())
			tx.commit();
		
		if (null != graphdb)
			graphdb.close();
    }
	
	public void clopen() {
		close();
		open();
	}
	
	public static int wrapAround(int value, int maxValue) {
		value = value%maxValue;
		if (value<0) value = value + maxValue;
		return value;
	}
	
	public RelationshipType makeRelationshipType(String name) {
		return makeRelationshipType(name,EdgeTypeGroup.DefaultGroup);
	}
	
	public RelationshipType makeUndirectedRelationshipType(String name) {
		return makeRelationshipType(name, EdgeTypeGroup.DefaultGroup, Directionality.Undirected);
	}
	
	public RelationshipType makeRelationshipType(String name, EdgeTypeGroup group) {
		return makeRelationshipType(name, group, Directionality.Directed);
	}
	
	public RelationshipType makeRelationshipType(String name, EdgeTypeGroup group, Directionality dir) {
		EdgeTypeMaker etmaker = tx.createEdgeType();
		RelationshipType relType = etmaker.withName(name).
                withDirectionality(dir).
                category(EdgeCategory.Simple).group(group).
										makeRelationshipType();
		return relType;
	}

	
	
	public RelationshipType makeLabeledRelationshipType(String name) {
		EdgeTypeMaker etmaker = tx.createEdgeType();
		RelationshipType relType = etmaker.withName(name).
                withDirectionality(Directionality.Directed).
										category(EdgeCategory.Labeled).
										makeRelationshipType();
		return relType;
	}
	
	public RelationshipType makeLabeledRelationshipType(String name, PropertyType key, PropertyType compact) {
		EdgeTypeMaker etmaker = tx.createEdgeType();
		RelationshipType relType = etmaker.withName(name).
                keySignature(key).compactSignature(compact).
										withDirectionality(Directionality.Directed).
										category(EdgeCategory.Labeled).
										makeRelationshipType();
		return relType;
	}
	
	public PropertyType makeStringPropertyType(String name) {
		return tx.createEdgeType().withName(name).
			category(EdgeCategory.Simple).
			makeKeyed().withIndex(true).
			dataType(String.class).										
			makePropertyType();
	}
	
	public PropertyType makeStringIDPropertyType(String name) {
		return makeStringIDPropertyType(name,EdgeTypeGroup.DefaultGroup);
	}
	
	public PropertyType makeStringIDPropertyType(String name, EdgeTypeGroup group) {
		return tx.createEdgeType().withName(name).
			category(EdgeCategory.Simple).functional(true).
			makeKeyed().withIndex(true).
			dataType(String.class).group(group).								
			makePropertyType();
	}
	
	public PropertyType makeUnkeyedStringPropertyType(String name) {
		return tx.createEdgeType().withName(name).
			category(EdgeCategory.Simple).withIndex(true).
			dataType(String.class).									
			makePropertyType();
	}
	
	public PropertyType makeBooleanPropertyType(String name) {
		return tx.createEdgeType().withName(name).
			category(EdgeCategory.Simple).
			dataType(Boolean.class).						
			makePropertyType();		
	}
	
	public PropertyType makeIDPropertyType(String name) {
		return makeIDPropertyType(name,EdgeTypeGroup.DefaultGroup);
	}
	
	public PropertyType makeIDPropertyType(String name, EdgeTypeGroup group) {
		return tx.createEdgeType().withName(name).
			category(EdgeCategory.Simple).functional(true).
			makeKeyed().withIndex(true).
			dataType(Integer.class).group(group).					
			makePropertyType();
	}

	public PropertyType makeWeightPropertyType(String name) {
		return tx.createEdgeType().withName(name).
			category(EdgeCategory.Simple).functional(true).
			dataType(Double.class).										
			makePropertyType();
	}
	
	public PropertyType getOrMakeBooleanPropertyType(String name) {
		if (tx.containsEdgeType(name))
			return tx.getPropertyType(name);
		return makeBooleanPropertyType(name);
	}
	
	public PropertyType getOrMakeStringPropertyType(String name) {
		if (tx.containsEdgeType(name))
			return tx.getPropertyType(name);
		return makeStringPropertyType(name);
	}
	
	public PropertyType getOrMakeStringIDPropertyType(String name) {
		if (tx.containsEdgeType(name))
			return tx.getPropertyType(name);
		return makeStringIDPropertyType(name);
	}
	
	public PropertyType getOrMakeUnkeyedStringPropertyType(String name) {
		if (tx.containsEdgeType(name))
			return tx.getPropertyType(name);
		return makeUnkeyedStringPropertyType(name);
	}
	
	public RelationshipType getOrMakeLabeledRelationshipType(String name) {
		if (tx.containsEdgeType(name))
			return tx.getRelationshipType(name);
//		return makeLabeledRelationshipType(name);
		return makeLabeledRelationshipType(name);
//		try {
//			return tx.getRelationshipType(name); // TODO tx.containsEdgeType()?
//		} catch (IllegalArgumentException e) {
//			return makeLabeledRelationshipType(name);
//		}	
	}
}
