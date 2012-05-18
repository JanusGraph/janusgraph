package com.thinkaurelius.titan.graphdb.test;


import com.thinkaurelius.titan.DiskgraphTest;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.edgetypes.Directionality;
import com.thinkaurelius.titan.graphdb.edgetypes.EdgeCategory;
import org.apache.commons.configuration.Configuration;
import org.junit.After;
import org.junit.Before;

public abstract class AbstractGraphDBTestCommon {

	public Configuration config;
	public TitanGraph graphdb;
	public TitanTransaction tx;
	
	public AbstractGraphDBTestCommon(Configuration config) {
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
		graphdb = TitanFactory.open(config);
		tx = graphdb.startThreadTransaction(TransactionConfig.STANDARD);
	}
	
	public void close() {
		if (null != tx && tx.isOpen())
			tx.commit();
		
		if (null != graphdb)
			graphdb.shutdown();
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
	
	public TitanLabel makeRelationshipType(String name) {
		return makeRelationshipType(name, TypeGroup.DEFAULT_GROUP);
	}
	
	public TitanLabel makeUndirectedRelationshipType(String name) {
		return makeRelationshipType(name, TypeGroup.DEFAULT_GROUP, Directionality.Undirected);
	}
	
	public TitanLabel makeRelationshipType(String name, TypeGroup group) {
		return makeRelationshipType(name, group, Directionality.Directed);
	}
	
	public TitanLabel makeRelationshipType(String name, TypeGroup group, Directionality dir) {
		TypeMaker etmaker = tx.makeType();
		etmaker.name(name).simple().group(group);
        switch(dir) {
            case Undirected: etmaker.undirected(); break;
            case Unidirected: etmaker.unidirected(); break;
            case Directed: etmaker.directed(); break;
        }
		return etmaker.makeEdgeLabel();
	}

	
	
	public TitanLabel makeLabeledRelationshipType(String name) {
		TypeMaker etmaker = tx.makeType();
		TitanLabel relType = etmaker.name(name).directed().makeEdgeLabel();
		return relType;
	}
	
	public TitanLabel makeLabeledRelationshipType(String name, TitanKey key, TitanKey compact) {
		TypeMaker etmaker = tx.makeType();
		TitanLabel relType = etmaker.name(name).
                primaryKey(key).signature(compact).directed().makeEdgeLabel();
		return relType;
	}
	
	public TitanKey makeStringPropertyType(String name) {
		return tx.makeType().name(name).simple().
                unique().indexed().dataType(String.class).makePropertyKey();
	}
	
	public TitanKey makeStringIDPropertyType(String name) {
		return makeStringIDPropertyType(name, TypeGroup.DEFAULT_GROUP);
	}
	
	public TitanKey makeStringIDPropertyType(String name, TypeGroup group) {
		return tx.makeType().name(name).
			simple().functional().
                unique().indexed().
			dataType(String.class).group(group).
                makePropertyKey();
	}
	
	public TitanKey makeUnkeyedStringPropertyType(String name) {
		return tx.makeType().name(name).
			simple().indexed().
			dataType(String.class).
                makePropertyKey();
	}
	
	public TitanKey makeBooleanPropertyType(String name) {
		return tx.makeType().name(name).
			simple().
			dataType(Boolean.class).
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

	public TitanKey makeWeightPropertyType(String name) {
		return tx.makeType().name(name).
			simple().functional().
			dataType(Double.class).
                makePropertyKey();
	}
	
	public TitanKey getOrMakeBooleanPropertyType(String name) {
		if (tx.containsType(name))
			return tx.getPropertyKey(name);
		return makeBooleanPropertyType(name);
	}
	
	public TitanKey getOrMakeStringPropertyType(String name) {
		if (tx.containsType(name))
			return tx.getPropertyKey(name);
		return makeStringPropertyType(name);
	}
	
	public TitanKey getOrMakeStringIDPropertyType(String name) {
		if (tx.containsType(name))
			return tx.getPropertyKey(name);
		return makeStringIDPropertyType(name);
	}
	
	public TitanKey getOrMakeUnkeyedStringPropertyType(String name) {
		if (tx.containsType(name))
			return tx.getPropertyKey(name);
		return makeUnkeyedStringPropertyType(name);
	}
	
	public TitanLabel getOrMakeLabeledRelationshipType(String name) {
		if (tx.containsType(name))
			return tx.getEdgeLabel(name);
//		return makeLabeledRelationshipType(name);
		return makeLabeledRelationshipType(name);
//		try {
//			return tx.getTitanLabel(name); // TODO tx.containsType()?
//		} catch (IllegalArgumentException e) {
//			return makeLabeledRelationshipType(name);
//		}	
	}
}
