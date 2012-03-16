package com.thinkaurelius.titan.graphdb.edgetypes.manager;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.attribute.PointInterval;
import com.thinkaurelius.titan.exceptions.InvalidEntityException;
import com.thinkaurelius.titan.graphdb.database.GraphDB;
import com.thinkaurelius.titan.graphdb.edgequery.EdgeQueryUtil;
import com.thinkaurelius.titan.graphdb.edgetypes.*;
import com.thinkaurelius.titan.graphdb.edgetypes.system.SystemPropertyType;
import com.thinkaurelius.titan.graphdb.idmanagement.IDInspector;
import com.thinkaurelius.titan.graphdb.transaction.GraphTx;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.thinkaurelius.titan.graphdb.edgetypes.manager.EdgeTypeManagerUtil.convertSignature;


public class SimpleEdgeTypeManager implements EdgeTypeManager {

	private final GraphDB graphdb;
	private final EdgeTypeFactory factory;
	
	private final ReadWriteLock mapLock;
	private final Lock mapReadLock;
	private final Lock mapWriteLock;
	
	private final Map<Long,EdgeTypeInformation> idIndex;
	private final Map<String,Long> nameIndex;
	
	public SimpleEdgeTypeManager(GraphDB graphdb) {
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
	public boolean containsEdgeType(long id, GraphTx tx) {
		mapReadLock.lock();
		boolean contains = idIndex.containsKey(Long.valueOf(id));
		mapReadLock.unlock();
		if (contains) return true;
		else return graphdb.containsNodeID(id, tx);
	}



	@Override
	public boolean containsEdgeType(String name, GraphTx tx) {
		mapReadLock.lock();
		boolean contains = nameIndex.containsKey(name);
		mapReadLock.unlock();
		if (contains) return true;
		else return graphdb.indexRetrieval(new PointInterval<String>(name), SystemPropertyType.EdgeTypeName, tx).length>0;
	}

	@Override
	public void committed(InternalEdgeType edgetype) {
		Long id = edgetype.getID();
		mapWriteLock.lock();
		if (nameIndex.containsKey(edgetype.getName()))
			throw new InvalidEntityException("Edge Type with name does already exist: " + edgetype.getName() + " | " + edgetype.isRelationshipType());
		nameIndex.put(edgetype.getName(),id);
		//Determine system edge ids
		long nameEdgeID = EdgeQueryUtil.queryHiddenFunctionalProperty(edgetype, SystemPropertyType.EdgeTypeName).getID();
		long defEdgeID = -1;
		if (edgetype.isPropertyType()) {
			defEdgeID = EdgeQueryUtil.queryHiddenFunctionalProperty(edgetype, SystemPropertyType.PropertyTypeDefinition).getID();
		} else {
			assert edgetype.isRelationshipType();
			defEdgeID = EdgeQueryUtil.queryHiddenFunctionalProperty(edgetype, SystemPropertyType.RelationshipTypeDefinition).getID();			
		}
		idIndex.put(id, new EdgeTypeInformation(edgetype.getDefinition(), defEdgeID, nameEdgeID ));
		mapWriteLock.unlock();
	}

	private void checkUniqueName(String name) {
		mapReadLock.lock();
		boolean exists = nameIndex.containsKey(name);
		mapReadLock.unlock();
		if (exists)
			throw new IllegalArgumentException("Edge Type with name does already exist: " + name);
	}
	
	@Override
	public PropertyType createPropertyType(GraphTx tx, String name,
			EdgeCategory category, Directionality directionality,
			EdgeTypeVisibility visibility,
			boolean isfunctional, EdgeType[] keysig, EdgeType[] compactsig,
			EdgeTypeGroup group, 
			boolean isKey, PropertyIndex index, Class<?> objectType) {
		checkUniqueName(name);
		StandardPropertyType pt = new StandardPropertyType(name,category,directionality,visibility,
				isfunctional,convertSignature(keysig),convertSignature(compactsig),group,isKey,index,objectType);
		return factory.createNewPropertyType(pt, tx);
	}


	@Override
	public RelationshipType createRelationshipType(GraphTx tx, String name,
			EdgeCategory category, Directionality directionality,
			EdgeTypeVisibility visibility,
			boolean isfunctional, EdgeType[] keysig, EdgeType[] compactsig,
			EdgeTypeGroup group) {
		checkUniqueName(name);
		StandardRelationshipType rt = new StandardRelationshipType(name,category,directionality,visibility,
				isfunctional,convertSignature(keysig),convertSignature(compactsig),group);
		return factory.createNewRelationshipType(rt, tx);
	}



	@Override
	public InternalEdgeType getEdgeType(long id, GraphTx tx) {
		mapReadLock.lock();
		EdgeTypeInformation info = idIndex.get(Long.valueOf(id));
		mapReadLock.unlock();
		if (info==null) {
			if (!tx.containsNode(id)) throw new IllegalArgumentException("EdgeType is unknown: " + id);
			IDInspector idspec = graphdb.getIDInspector();
			assert idspec.isEdgeTypeID(id);
			InternalEdgeType et=null;
			if (idspec.isPropertyTypeID(id)) {
				et = factory.createExistingPropertyType(id, tx);
			} else if (idspec.isRelationshipTypeID(id)) {
				et = factory.createExistingRelationshipType(id, tx);
			} else throw new AssertionError("Unexpected type id: " + id);
			mapWriteLock.lock();
            if (idIndex.containsKey(Long.valueOf(id))) et = getEdgeType(id,tx);
            else committed(et);
            mapWriteLock.unlock();
			return et;
		} else {
			return factory.createExistingEdgeType(id, info, tx);
		}
	}



	@Override
	public InternalEdgeType getEdgeType(String name, GraphTx tx) {
		mapReadLock.lock();
		Long id = nameIndex.get(name);
		mapReadLock.unlock();
		if (id==null) {
			long[] ids = graphdb.indexRetrieval(new PointInterval<String>(name), SystemPropertyType.EdgeTypeName, tx);
			if (ids.length==0) return null;
			else {
				assert ids.length==1;
				id = ids[0];
			}
		} 
		return getEdgeType(id,tx);
	}



	@Override
	public EdgeTypeMaker getEdgeTypeMaker(GraphTx tx) {
		return new StandardEdgeTypeMaker(tx,this);
	}

	
}
