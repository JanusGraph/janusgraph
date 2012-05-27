package com.thinkaurelius.titan.graphdb.edgetypes.manager;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.exceptions.InvalidElementException;
import com.thinkaurelius.titan.graphdb.database.InternalTitanGraph;
import com.thinkaurelius.titan.graphdb.edgequery.EdgeQueryUtil;
import com.thinkaurelius.titan.graphdb.edgetypes.*;
import com.thinkaurelius.titan.graphdb.edgetypes.system.SystemKey;
import com.thinkaurelius.titan.graphdb.idmanagement.IDInspector;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.thinkaurelius.titan.graphdb.edgetypes.manager.EdgeTypeManagerUtil.convertSignature;


public class SimpleEdgeTypeManager implements EdgeTypeManager {

	private final InternalTitanGraph graphdb;
	private final EdgeTypeFactory factory;
	
	private final ReadWriteLock mapLock;
	private final Lock mapReadLock;
	private final Lock mapWriteLock;
	
	private final Map<Long,EdgeTypeInformation> idIndex;
	private final Map<String,Long> nameIndex;
	
	public SimpleEdgeTypeManager(InternalTitanGraph graphdb) {
		this.graphdb = graphdb;
		idIndex = new HashMap<Long,EdgeTypeInformation>();
		nameIndex = new HashMap<String,Long>();
		
		factory = new StandardEdgeTypeFactory();
		
		mapLock = new ReentrantReadWriteLock();
		mapReadLock = mapLock.readLock();
		mapWriteLock = mapLock.writeLock();
	}
	
	
	
	public void close() {
		idIndex.clear();
		nameIndex.clear();
	}

	@Override
	public boolean containsEdgeType(long id, InternalTitanTransaction tx) {
		mapReadLock.lock();
		boolean contains = idIndex.containsKey(Long.valueOf(id));
		mapReadLock.unlock();
		if (contains) return true;
		else return graphdb.containsVertexID(id, tx);
	}



	@Override
	public boolean containsEdgeType(String name, InternalTitanTransaction tx) {
		mapReadLock.lock();
		boolean contains = nameIndex.containsKey(name);
		mapReadLock.unlock();
		if (contains) return true;
		else return graphdb.indexRetrieval(name, SystemKey.TypeName, tx).length>0;
	}

	@Override
	public void committed(InternalTitanType edgetype) {
		Long id = edgetype.getID();
		mapWriteLock.lock();
		if (nameIndex.containsKey(edgetype.getName()))
			throw new InvalidElementException("TitanRelation Type with name does already exist: " + edgetype.getName() + " | " + edgetype.isEdgeLabel(),edgetype);
		nameIndex.put(edgetype.getName(),id);
		//Determine system edge ids
		long nameEdgeID = EdgeQueryUtil.queryHiddenFunctionalProperty(edgetype, SystemKey.TypeName).getID();
		long defEdgeID = -1;
		if (edgetype.isPropertyKey()) {
			defEdgeID = EdgeQueryUtil.queryHiddenFunctionalProperty(edgetype, SystemKey.PropertyTypeDefinition).getID();
		} else {
			assert edgetype.isEdgeLabel();
			defEdgeID = EdgeQueryUtil.queryHiddenFunctionalProperty(edgetype, SystemKey.RelationshipTypeDefinition).getID();
		}
		idIndex.put(id, new EdgeTypeInformation(edgetype.getDefinition(), defEdgeID, nameEdgeID ));
		mapWriteLock.unlock();
	}

	private void checkUniqueName(String name) {
		mapReadLock.lock();
		boolean exists = nameIndex.containsKey(name);
		mapReadLock.unlock();
		if (exists)
			throw new IllegalArgumentException("TitanRelation Type with name does already exist: " + name);
	}
	
	@Override
	public TitanKey createPropertyType(InternalTitanTransaction tx, String name,
			EdgeCategory category, Directionality directionality,
			EdgeTypeVisibility visibility,
            FunctionalType isfunctional, TitanType[] keysig, TitanType[] compactsig,
			TypeGroup group,
			boolean isKey, boolean hasIndex, Class<?> objectType) {
		checkUniqueName(name);
		StandardPropertyType pt = new StandardPropertyType(name,category,directionality,visibility,
				isfunctional,convertSignature(keysig),convertSignature(compactsig),group,isKey,hasIndex,objectType);
		return factory.createNewPropertyKey(pt, tx);
	}


	@Override
	public TitanLabel createRelationshipType(InternalTitanTransaction tx, String name,
			EdgeCategory category, Directionality directionality,
			EdgeTypeVisibility visibility,
            FunctionalType isfunctional, TitanType[] keysig, TitanType[] compactsig,
			TypeGroup group) {
		checkUniqueName(name);
		StandardRelationshipType rt = new StandardRelationshipType(name,category,directionality,visibility,
				isfunctional,convertSignature(keysig),convertSignature(compactsig),group);
		return factory.createNewEdgeLabel(rt, tx);
	}



	@Override
	public InternalTitanType getEdgeType(long id, InternalTitanTransaction tx) {
		mapReadLock.lock();
		EdgeTypeInformation info = idIndex.get(Long.valueOf(id));
		mapReadLock.unlock();
		if (info==null) {
			if (!tx.containsVertex(id)) throw new IllegalArgumentException("TitanType is unknown: " + id);
			IDInspector idspec = graphdb.getIDInspector();
			assert idspec.isEdgeTypeID(id);
			InternalTitanType et=null;
			if (idspec.isPropertyTypeID(id)) {
				et = factory.createExistingPropertyKey(id, tx);
			} else if (idspec.isRelationshipTypeID(id)) {
				et = factory.createExistingEdgeLabel(id, tx);
			} else throw new AssertionError("Unexpected type id: " + id);
			mapWriteLock.lock();
            if (idIndex.containsKey(Long.valueOf(id))) et = getEdgeType(id,tx);
            else committed(et);
            mapWriteLock.unlock();
			return et;
		} else {
			return factory.createExistingType(id, info, tx);
		}
	}



	@Override
	public InternalTitanType getEdgeType(String name, InternalTitanTransaction tx) {
		mapReadLock.lock();
		Long id = nameIndex.get(name);
		mapReadLock.unlock();
		if (id==null) {
			long[] ids = graphdb.indexRetrieval(name, SystemKey.TypeName, tx);
			if (ids.length==0) return null;
			else {
				assert ids.length==1;
				id = ids[0];
			}
		} 
		return getEdgeType(id,tx);
	}



	@Override
	public TypeMaker getEdgeTypeMaker(InternalTitanTransaction tx) {
		return new StandardTypeMaker(tx,this);
	}

	
}
