package com.thinkaurelius.titan.graphdb.database;

import cern.colt.list.AbstractLongList;
import cern.colt.list.LongArrayList;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.thinkaurelius.titan.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.attribute.Interval;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.writeaggregation.BatchKeyColumnValueStoreMutator;
import com.thinkaurelius.titan.diskstorage.writeaggregation.DirectKeyColumnValueStoreMutator;
import com.thinkaurelius.titan.diskstorage.writeaggregation.KeyColumnValueStoreMutator;
import com.thinkaurelius.titan.exceptions.GraphStorageException;
import com.thinkaurelius.titan.exceptions.ToBeImplementedException;
import com.thinkaurelius.titan.graphdb.database.idassigner.NodeIDAssigner;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.database.statistics.InternalGraphStatistics;
import com.thinkaurelius.titan.graphdb.database.statistics.TransactionStatistics;
import com.thinkaurelius.titan.graphdb.database.util.EdgeTypeSignature;
import com.thinkaurelius.titan.graphdb.database.util.LimitTracker;
import com.thinkaurelius.titan.graphdb.edgequery.InternalEdgeQuery;
import com.thinkaurelius.titan.graphdb.edgequery.StandardEdgeQuery;
import com.thinkaurelius.titan.graphdb.edges.EdgeDirection;
import com.thinkaurelius.titan.graphdb.edges.InternalEdge;
import com.thinkaurelius.titan.graphdb.edges.factory.EdgeLoader;
import com.thinkaurelius.titan.graphdb.edgetypes.EdgeTypeDefinition;
import com.thinkaurelius.titan.graphdb.edgetypes.InternalEdgeType;
import com.thinkaurelius.titan.graphdb.edgetypes.manager.EdgeTypeManager;
import com.thinkaurelius.titan.graphdb.edgetypes.manager.SimpleEdgeTypeManager;
import com.thinkaurelius.titan.graphdb.edgetypes.system.SystemPropertyType;
import com.thinkaurelius.titan.graphdb.idmanagement.IDInspector;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.sendquery.QuerySender;
import com.thinkaurelius.titan.graphdb.transaction.GraphTx;
import com.thinkaurelius.titan.graphdb.transaction.StandardPersistGraphTx;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

public class StandardGraphDB implements GraphDB {

	private static final int defaultOutputCapacity = 128;
	
	private static final Logger log =
		LoggerFactory.getLogger(StandardGraphDB.class);
	
	private final GraphDatabaseConfiguration config;
	private final IDManager idManager;
	private final EdgeTypeManager etManager;
	
	private final StorageManager storage;
	private final OrderedKeyColumnValueStore edgeStore;
	private final OrderedKeyColumnValueStore propertyIndex;
	
	private final LockManager lockManager;
	private final Serializer serializer;
	
	private final NodeIDAssigner idAssigner;
	private final InternalGraphStatistics statistics;
	
	
	public StandardGraphDB(GraphDatabaseConfiguration configuration,
			StorageManager storage, OrderedKeyColumnValueStore edgeStore, OrderedKeyColumnValueStore propertyIndex,
			LockManager lockManager, Serializer serializer, NodeIDAssigner idAssigner,
			InternalGraphStatistics statistics) {
		this.config=configuration;
		this.idManager = idAssigner.getIDManager();
		this.etManager = new SimpleEdgeTypeManager(this);

		this.storage = storage;
		this.edgeStore = edgeStore;
		this.propertyIndex = propertyIndex;
		
		this.lockManager = lockManager;
		this.serializer = serializer;
		this.idAssigner = idAssigner;
		this.statistics=statistics;
	}
	
	@Override
	public IDInspector getIDInspector() {
		return idManager;
	}
	

	@Override
	public void close() throws GraphStorageException {
		etManager.close();
		
		edgeStore.close();
		propertyIndex.close();
		storage.close();
		config.close();
		idAssigner.close();
	}

	@Override
	public GraphDatabaseConfiguration getConfiguration() {
		return config;
	}
	
	@Override
	 public GraphStatistics getStatistics() {
		return statistics;
	}

	@Override
	public GraphTransaction startTransaction() {
		return startTransaction(false);
	}
	
	@Override
	public GraphTransaction startTransaction(boolean readOnly) {
		if (readOnly) return startTransaction(GraphTransactionConfig.ReadOnly);
		else return startTransaction(GraphTransactionConfig.Standard);
	}
	
	@Override
	public GraphTransaction startTransaction(GraphTransactionConfig configuration) {
		return startTransaction(configuration, config.createQuerySender());
	}
	
	@Override
	public GraphTx startTransaction(GraphTransactionConfig configuration, QuerySender sender) {
		return new StandardPersistGraphTx(this,lockManager,etManager,
				configuration,sender,storage.beginTransaction());
	}
	
	
	@Override
	public boolean containsNodeID(long id, GraphTx tx) {
		log.debug("Checking node existence for {}",id);
		return edgeStore.containsKey(ByteBufferUtil.getLongByteBuffer(id), tx.getTxHandle());
	}
	
	@Override
	public long[] indexRetrieval(Interval<?> interval, PropertyType pt, GraphTx tx) {
		Preconditions.checkArgument(pt.getCategory()==EdgeCategory.Simple,
					"Currently, only simple properties are supported for index retrieval!");
		Preconditions.checkArgument(pt.getIndexType().hasIndex(),
					"Cannot retrieve for given property type - it does not have an index.");
		Preconditions.checkArgument(!interval.isRange() || (pt.getIndexType()==PropertyIndex.Range),
					"Cannot execute range queries for given property type - it does not have a range index.");
		long[] nodes = null;
			
		if (interval.isPoint()) {
			assert pt.getIndexType().hasIndex();
			Object key = interval.getStartPoint();
			Preconditions.checkArgument(pt.getDataType().isInstance(key),"Interval start point object is incompatible with property data type ["+pt.getName()+"].");

			if (pt.isKeyed()) {
				ByteBuffer value = propertyIndex.get(getIndexKey(key), getKeyedIndexColumn(pt), tx.getTxHandle());
				if (value!=null) {
					nodes = new long[1];
					nodes[0]=value.getLong();
				}
			} else {
				List<Entry> entries = propertyIndex.getSlice(getIndexKey(key), getIndexColumn(pt,0), 
															getIndexColumn(pt,-1), true, false, tx.getTxHandle());
				nodes = new long[entries.size()];
				int i = 0;
				for (Entry ent : entries) {
					nodes[i++] = ent.getValue().getLong();
				}
			}
		} else {
			assert interval.isRange() && pt.getIndexType()==PropertyIndex.Range;
			Preconditions.checkArgument(pt.getDataType().isInstance(interval.getStartPoint()),"Interval start point  object is incompatible with property data type.");
			Preconditions.checkArgument(pt.getDataType().isInstance(interval.getEndPoint()),"Interval end point  object is incompatible with property data type.");
			Map<ByteBuffer,List<Entry>> keyEntries = null;
			if (pt.isKeyed()) {
				keyEntries = propertyIndex.getKeySlice(getIndexKey(interval.getStartPoint()), 
						getIndexKey(interval.getEndPoint()), interval.startInclusive(), interval.endInclusive(), 
						getKeyedIndexColumn(pt), getKeyedIndexColumn(pt), true, true,
						tx.getTxHandle());
			} else {
				keyEntries = propertyIndex.getKeySlice(getIndexKey(interval.getStartPoint()), 
						getIndexKey(interval.getEndPoint()), interval.startInclusive(), interval.endInclusive(), 
						getIndexColumn(pt,0), getIndexColumn(pt,-1), true, false,
						tx.getTxHandle());	
			}
			int size = 0;
			for (List<Entry> entries : keyEntries.values()) size +=entries.size();
			nodes = new long[size];
			int i = 0;
			for (List<Entry> entries : keyEntries.values()) {
				for (Entry entry : entries) {
					nodes[i]=entry.getValue().getLong();
					i++;
				}
			}
			assert i==size;
		}

		if (nodes==null) return new long[0];
		else return nodes;
	}
	

	@Override
	public AbstractLongList getAllNodeIDs(long startRange, long endRange) {
		throw new ToBeImplementedException();
	}

	@Override
	public AbstractLongList getRawNeighborhood(InternalEdgeQuery query,
			GraphTx tx) {
		List<Entry> entries = queryForEntries(query,tx.getTxHandle());
		
		EdgeType constantET = null;
		if (query.hasEdgeTypeCondition()) constantET = query.getEdgeTypeCondition();
		
		AbstractLongList result = new LongArrayList();
		
		for (Entry entry : entries) {
			EdgeType et = constantET;
			if (et==null) {
				//Determine Edgetype
				long etid = entry.getColumn().getLong();
				etid = idManager.switchBackEdgeTypeID(etid);
				et=(EdgeType)tx.getNode(etid);
			}
			if (et.isPropertyType() || (!et.isModifiable() && !query.queryUnmodifiable())) {
				continue; //Skip since it does not match query
			}
			//Get neighboring node id
			long nghid;
			if (et.isFunctional()) {
				ByteBuffer val = entry.getValue();
				nghid = val.getLong();
			} else {
				ByteBuffer col = entry.getColumn();
				nghid = col.getLong(col.limit()- 2*ByteBufferUtil.longSize);
			}
			result.add(nghid);
		}
		return result;
	}

	@Override
	public void loadEdges(InternalEdgeQuery query, GraphTx tx) {
		List<Entry> entries = queryForEntries(query,tx.getTxHandle());

		EdgeType edgeType = null;
		if (query.hasEdgeTypeCondition()) edgeType = query.getEdgeTypeCondition();
		InternalNode node = query.getNode();
		
		EdgeLoader factory = tx.getEdgeFactory();
		
		for (Entry entry : entries) {
			ByteBuffer column = entry.getColumn();
			long etDir = column.getLong();
			column.mark();
			long etid = idManager.switchBackEdgeTypeID(etDir);
			if (edgeType==null || edgeType.getID()!=etid) {
				edgeType = (EdgeType)tx.getNode(etid);
			}
			InternalEdge edge;
			ByteBuffer readValues;
			if (edgeType.isRelationshipType()) {
				ByteBuffer read;
				if (edgeType.isFunctional()) {
					read = entry.getValue();
					readValues = read;
				} else {
					read = entry.getColumn();
					read.position(read.limit()- 2*ByteBufferUtil.longSize);
					readValues = entry.getValue();
				}
				long otherid = read.getLong();
				long edgeid = read.getLong();
				InternalNode othernode = tx.getExistingNode(otherid);
				InternalNode[] nodes;
				EdgeDirection dir = EdgeDirection.fromID(idManager.getDirectionFront(etDir));
				if (dir==EdgeDirection.In) {
					assert (edgeType.getDirectionality()==Directionality.Directed);
					nodes = new InternalNode[]{othernode, node};
				} else {
					nodes = new InternalNode[]{node, othernode};
				}
				
				tx.getExistingNode(otherid);
				edge = factory.createExistingRelationship(edgeid, (RelationshipType) edgeType, nodes[0],nodes[1]);
			} else {
				assert edgeType.isPropertyType();
				assert EdgeDirection.fromID(idManager.getDirectionFront(etDir))==EdgeDirection.Out;
				PropertyType propType = (PropertyType)edgeType;
				long edgeid;
				ByteBuffer readObj=entry.getValue();
				readValues = readObj;
				if (edgeType.isFunctional()) {
					edgeid = readObj.getLong();
				} else {
					ByteBuffer col = entry.getColumn();
					edgeid = col.getLong(col.limit()- 1*ByteBufferUtil.longSize);
				}
				Object attribute = serializer.readObjectNotNull(readObj, propType.getDataType());
				edge = factory.createExistingProperty(edgeid, propType, node, attribute);
			}
			//Read labels if any

			if (edgeType.getCategory()==EdgeCategory.Labeled) { // || EdgeCategory.LabeledRestricted
				Map<String,EdgeType> etCache = new HashMap<String,EdgeType>();
				EdgeTypeDefinition def = ((InternalEdgeType)edgeType).getDefinition();
				//First: key signature
				ByteBuffer read = entry.getColumn();
				read.reset();
				for (String str : def.getKeySignature()) readLabel(edge,read,getEdgeType(str,etCache,tx),factory,tx);
				//Second: value signature
				readValues = entry.getValue();
				for (String str : def.getCompactSignature()) readLabel(edge,readValues,getEdgeType(str,etCache,tx),factory,tx);
				
				//Third: read rest
				while (readValues.hasRemaining()) {
					EdgeType type = (EdgeType)tx.getExistingNode(readValues.getLong());
					readLabel(edge,readValues,type,factory,tx);
				}
			}
		}
		query.getNode().loadedEdges(query);
	}
	
	private EdgeType getEdgeType(String name, Map<String,EdgeType> etCache, GraphTx tx) {
		EdgeType et = etCache.get(name);
		if (et==null) {
			et = tx.getEdgeType(name);
			etCache.put(name, et);
		}
		return et;
	}
	
	private void readLabel(InternalNode start, ByteBuffer read, EdgeType type, EdgeLoader loader, GraphTx tx) {
		if (type.isPropertyType()) {
			PropertyType proptype = ((PropertyType) type);
			Object att = serializer.readObject(read, proptype.getDataType());
			if (att!=null) {
				loader.createExistingProperty(proptype, start, att);
			}
		} else {
			assert type.isRelationshipType();
			long nodeid = read.getLong();
			if (nodeid!=0) {
				loader.createExistingRelationship((RelationshipType)type, start, tx.getExistingNode(nodeid));
			}
		}
		

	}
	
	private List<Entry> queryForEntries(InternalEdgeQuery query, TransactionHandle txh) {
		ByteBuffer key = ByteBufferUtil.getLongByteBuffer(query.getNodeID());
		List<Entry> entries = null;
		LimitTracker limit = new LimitTracker(query);
		
		if (query.hasEdgeTypeCondition()) {
			EdgeType et = query.getEdgeTypeCondition();
			if (!et.isNew()) { //Result set must be empty if EdgeType is new
				if (et.isPropertyType()) {
					if (query.isAllowedDirection(EdgeDirection.Out))
						entries=appendResults(key,idManager.getQueryBounds(et.getID(), EdgeDirection.Out.getID()),entries,limit,txh);
				} else {
					assert et.isRelationshipType();
					for (EdgeDirection dir : EdgeDirection.values()) {
						if (query.isAllowedDirection(dir)) {
							entries = appendResults(key,idManager.getQueryBounds(et.getID(), dir.getID()),entries,limit,txh);
						}
					}
				}
			}
		} else if (query.hasEdgeTypeGroupCondition()) {
			short groupid = query.getEdgeTypeGroupCondition().getID();
			if (query.queryRelationships()) {
				for (EdgeDirection dir : EdgeDirection.values()) {
					if (query.isAllowedDirection(dir)) {
						entries = appendResults(key,idManager.getQueryBoundsRelationship(dir.getID(),groupid),entries,limit,txh);
					}
				}
			}
			if (query.queryProperties()) {
				if (query.isAllowedDirection(EdgeDirection.Out))
					entries=appendResults(key,idManager.getQueryBoundsProperty(EdgeDirection.Out.getID(),groupid),entries,limit,txh);
			}
		} else {
			if (query.queryProperties() && query.queryRelationships()) {
				if (!query.hasDirectionCondition()) {
					entries = appendResults(key,new long[]{0,Long.MAX_VALUE},entries,limit,txh);
				} else {
					for (EdgeDirection dir : EdgeDirection.values()) {
						if (query.isAllowedDirection(dir)) {
							entries = appendResults(key,idManager.getQueryBounds(dir.getID()),entries,limit,txh);
						}
					}
				}
			} else if (query.queryRelationships()) { //Relationships only
				if (query.isAllowedDirection(EdgeDirection.Undirected)) {
					entries = appendResults(key,idManager.getQueryBoundsRelationship(EdgeDirection.Undirected.getID()),entries,limit,txh);
				}
				if (query.isAllowedDirection(EdgeDirection.In) && query.isAllowedDirection(EdgeDirection.Out)) {
					long[] bounds = idManager.getQueryBoundsRelationship(EdgeDirection.Out.getID());
					bounds[1] = idManager.getQueryBoundsRelationship(EdgeDirection.In.getID())[1];
					entries = appendResults(key,bounds,entries,limit,txh);
				} else if (query.isAllowedDirection(EdgeDirection.In)) {
					entries = appendResults(key,idManager.getQueryBoundsRelationship(EdgeDirection.In.getID()),entries,limit,txh);
				} else if (query.isAllowedDirection(EdgeDirection.Out)) {
					entries = appendResults(key,idManager.getQueryBoundsRelationship(EdgeDirection.Out.getID()),entries,limit,txh);
				}
			} else if (query.queryProperties()) { //Properties only
				if (query.isAllowedDirection(EdgeDirection.Out))
					entries=appendResults(key,idManager.getQueryBoundsProperty(EdgeDirection.Out.getID()),null,limit,txh);
			}
		}
		
		if (!limit.partialResult() && limit.limitExhausted()) return null;
		else if (entries==null) return ImmutableList.of();
		else return entries;
	}
	
	private List<Entry> appendResults(ByteBuffer key, long[] bounds, List<Entry> entries, LimitTracker limit, TransactionHandle txh) {
		if (limit.limitExhausted()) return null;
		List<Entry> results = null;
		if (limit.partialResult()) {
			results = edgeStore.getSlice(key, 
				ByteBufferUtil.getLongByteBuffer(bounds[0]), 
				ByteBufferUtil.getLongByteBuffer(bounds[1]), 
				true, false, limit.getLimit(), txh);
			limit.retrieved(results.size());
		} else {
			results = edgeStore.getLimitedSlice(key, 
					ByteBufferUtil.getLongByteBuffer(bounds[0]), 
					ByteBufferUtil.getLongByteBuffer(bounds[1]), 
					true, false, limit.getLimit(), txh);
			if (results==null) limit.retrieved(limit.getLimit());
			else limit.retrieved(results.size());
		}
		if (results==null) return null;
		else if (entries==null) return results;
		else {
			entries.addAll(results);
			return entries;
		}
	}
	
	private KeyColumnValueStoreMutator getEdgeStoreMutator(TransactionHandle txh) {
		if (config.isEdgeBatchWritingEnabled()) {
			try {
				MultiWriteKeyColumnValueStore mwstore;
				mwstore = (MultiWriteKeyColumnValueStore)edgeStore;
				return new BatchKeyColumnValueStoreMutator(txh, mwstore, config.getEdgeBatchWriteSize());
			} catch (ClassCastException e) {
				config.setEdgeBatchWritingEnabled(false);
				log.error("Batching writing disabled on edge store");
				log.error("Edge store {} does not support batching", edgeStore, e);
			}
		} 
		
		return new DirectKeyColumnValueStoreMutator(txh, edgeStore);
	}
	
	private KeyColumnValueStoreMutator getPropertyIndexMutator(TransactionHandle txh) {
		if (config.isPropertyBatchWritingEnabled()) {
			try {
				MultiWriteKeyColumnValueStore mwstore;
				mwstore = (MultiWriteKeyColumnValueStore)propertyIndex;
				return new BatchKeyColumnValueStoreMutator(txh, mwstore, config.getPropertyBatchWriteSize());
			} catch (ClassCastException e) {
				config.setPropertyBatchWritingEnabled(false);
				log.error("Batching writing disabled on property store");
				log.error("Property index {} does not support batching", propertyIndex, e);
			}
		} 
		
		return new DirectKeyColumnValueStoreMutator(txh, propertyIndex);		
	}

	@Override
	public void flush(Collection<InternalEdge> addedEdges,
			Collection<InternalEdge> deletedEdges, GraphTx tx) throws GraphStorageException {
		assignIDs(addedEdges,tx);
	}
	
	private void assignIDs(Collection<InternalEdge> addedEdges,GraphTx tx) {
		for (InternalEdge edge : addedEdges) {
			if (edge.isDeleted()) continue;
			if (edge.hasID()) continue;
			
			for (int i=0;i<edge.getArity();i++) {
				InternalNode node = edge.getNodeAt(i);
				if (!node.hasID()) {
					assert node.isNew();
                    node.setID(idAssigner.getNewNodeID(node));
				}
			}
			assert !edge.hasID();
			edge.setID(idAssigner.getNewEdgeID(edge));
		}
	}
	
	@Override
	public boolean save(Collection<InternalEdge> addedEdges,
			Collection<InternalEdge> deletedEdges, GraphTx tx) throws GraphStorageException {
		log.debug("Saving transaction. Added {}, removed {}",addedEdges.size(),deletedEdges.size());
		Map<EdgeType,EdgeTypeSignature> signatures = new HashMap<EdgeType,EdgeTypeSignature>();
		TransactionHandle txh = tx.getTxHandle();
		TransactionStatistics stats = new TransactionStatistics();
		
		KeyColumnValueStoreMutator edgeMutator = getEdgeStoreMutator(txh);
		KeyColumnValueStoreMutator propMutator = getPropertyIndexMutator(txh);
		
		//1. Delete edges
		if (deletedEdges!=null && !deletedEdges.isEmpty()) {
			ListMultimap<InternalNode,ByteBuffer> deletions = ArrayListMultimap.create();
			for (InternalEdge del : deletedEdges) {
				if (del.isProperty()) {
					deletions.put(del.getNodeAt(0), getEntry(tx,del,del.getNodeAt(0),signatures,true).getColumn());
					Property prop = (Property)del;
					if (prop.getPropertyType().getIndexType().hasIndex())
						deleteIndexEntry(prop, propMutator);
				} else {
					assert del.isRelationship();
					deletions.put(del.getNodeAt(0), getEntry(tx,del,del.getNodeAt(0),signatures,true).getColumn());
					if (!del.isUnidirected())
						deletions.put(del.getNodeAt(1), getEntry(tx,del,del.getNodeAt(1),signatures,true).getColumn());
				}
				stats.removedEdge(del);
			}
			
			for (InternalNode node : deletions.keySet()) {
				List<ByteBuffer> dels = deletions.get(node);
				edgeMutator.delete(ByteBufferUtil.getLongByteBuffer(node.getID()), dels);
//				edgeStore.delete(ByteBufferUtil.getLongByteBuffer(node.getID()), dels, txh);
				if (node.isDeleted()) stats.removedNode(node);
			}
			deletedEdges.clear();
		}
		deletedEdges=null;
		
		
		//2. Assign Node IDs
		assignIDs(addedEdges,tx);
		
		ListMultimap<InternalEdgeType,InternalEdge> simpleEdgeTypes = null;
		ListMultimap<InternalEdgeType,InternalEdge> otherEdgeTypes = null;
		ListMultimap<InternalNode,InternalEdge> edges = ArrayListMultimap.create();
		
		//3. Sort Added Edges
		for (InternalEdge edge : addedEdges) {
			if (edge.isDeleted()) continue;

			EdgeType et = edge.getEdgeType();
			if (edge.isNew()) stats.addedEdge(edge);
			
			//Give special treatment to edge type definitions
			if (et==SystemPropertyType.EdgeTypeName || et==SystemPropertyType.PropertyTypeDefinition 
					|| et==SystemPropertyType.RelationshipTypeDefinition) {
				assert edge.getNodeAt(0) instanceof InternalEdgeType;
				InternalEdgeType node = (InternalEdgeType)edge.getNodeAt(0);
				assert node.hasID();
				if (et.getCategory()==EdgeCategory.Simple) {
					if (simpleEdgeTypes==null) simpleEdgeTypes=ArrayListMultimap.create();
					if (node.isNew() && !simpleEdgeTypes.containsKey(node)) stats.addedNode(node);
					simpleEdgeTypes.put(node,edge);
				} else {
					if (otherEdgeTypes==null) otherEdgeTypes=ArrayListMultimap.create();
					otherEdgeTypes.put(node,edge);					
				}
			} else { //Standard Edge
				assert (edge.getArity()==1 && edge.isProperty()) || (edge.getArity()==2 && edge.isRelationship());
				for (int i=0;i<edge.getArity();i++) {
					InternalNode node = edge.getNodeAt(i);
					assert node.hasID();
					if (i==0 || !edge.isUnidirected()) {
						if (node.isNew() && !edges.containsKey(node)) stats.addedNode(node);
						edges.put(node, edge);
					}
				}
			}
		}
		addedEdges.clear(); addedEdges=null;
		
		//3. Persist
		if (simpleEdgeTypes!=null) persist(simpleEdgeTypes,signatures,tx,edgeMutator,propMutator);
		if (otherEdgeTypes!=null) persist(otherEdgeTypes,signatures,tx,edgeMutator,propMutator);
		if (!edges.isEmpty()) persist(edges,signatures,true,tx,edgeMutator,propMutator);
		//Add properties to index
		
		//Commit saved EdgeTypes to EdgeTypeManager
		if (simpleEdgeTypes!=null) commitEdgeTypes(simpleEdgeTypes.keySet());
		if (otherEdgeTypes!=null) commitEdgeTypes(otherEdgeTypes.keySet());
	
		//Update statistics
		statistics.update(stats);

		propMutator.flush();
		edgeMutator.flush();
		
		return true;
	}
	
	private void commitEdgeTypes(Iterable<InternalEdgeType> edgeTypes) {
		for (InternalEdgeType et : edgeTypes) etManager.committed(et);
	}
	
	private<N extends InternalNode> void persist(ListMultimap<N,InternalEdge> sortedEdges, Map<EdgeType,EdgeTypeSignature> signatures,
			GraphTx tx, KeyColumnValueStoreMutator edgeMutator, KeyColumnValueStoreMutator propMutator) {
		persist(sortedEdges,signatures,false,tx,edgeMutator,propMutator);
	}
	
	private<N extends InternalNode> void persist(ListMultimap<N,InternalEdge> sortedEdges, Map<EdgeType,EdgeTypeSignature> signatures,
			boolean sortNodes, GraphTx tx, KeyColumnValueStoreMutator edgeMutator, KeyColumnValueStoreMutator propMutator) {
		assert sortedEdges!=null && !sortedEdges.isEmpty();
		List<Property> properties = new ArrayList<Property>();
		Collection<N> nodes = sortedEdges.keySet();
		if (sortNodes) {
			List<N> sortednodes = new ArrayList<N>(nodes);
			Collections.sort(sortednodes, new Comparator<N>(){

				@Override
				public int compare(N o1, N o2) {
					assert o1.getID()!=o2.getID();
					if (o1.getID()<o2.getID()) return -1;
					else return 1;
				}
				
			});
			nodes=sortednodes;
		}
		
		for (N node : nodes) {
			List<InternalEdge> edges = sortedEdges.get(node);
			List<Entry> batch = new ArrayList<Entry>(edges.size());
			for (InternalEdge edge : edges) {
				if (edge.isProperty()) properties.add((Property)edge);
				batch.add(getEntry(tx,edge,node,signatures));
			}
			edgeMutator.insert(ByteBufferUtil.getLongByteBuffer(node.getID()), batch);
		}
		for (Property prop : properties) {
			addIndexEntry(prop, propMutator);
		}		
	}

	private Entry getEntry(GraphTx tx,InternalEdge edge, InternalNode perspective, Map<EdgeType,EdgeTypeSignature> signatures) {
		return getEntry(tx,edge,perspective,signatures,false);
	}
	
	private Entry getEntry(GraphTx tx,InternalEdge edge, InternalNode perspective, Map<EdgeType,EdgeTypeSignature> signatures, boolean columnOnly) {
		EdgeType et = edge.getEdgeType();
		
		EdgeDirection dir;
		if (et.getDirectionality()==Directionality.Undirected) {
			assert edge.isUndirected();
			dir = EdgeDirection.Undirected;
		} else if (edge.getNodeAt(0).equals(perspective)) {
			//Out Edge
			assert edge.isDirected() || edge.isUnidirected();
			dir = EdgeDirection.Out;
		} else {
			//In Edge
			assert edge.isDirected() && edge.getNodeAt(1).equals(perspective);
			dir = EdgeDirection.In;
		}
		long etValue = idManager.switchEdgeTypeID(et.getID(), dir.getID());
		
		ByteBuffer column=null,value=null;
		switch(et.getCategory()) {
		case Simple:
			if (et.isFunctional()) {
				column = ByteBufferUtil.getLongByteBuffer(etValue);
			} else {
				if (edge.isRelationship()) {
					column = ByteBuffer.allocate(3*ByteBufferUtil.longSize);
					column.putLong(etValue);
					column.putLong(((Relationship) edge).getOtherNode(perspective).getID());
					column.putLong(edge.getID());
				} else {
					assert edge.isProperty();
					column = ByteBuffer.allocate(2*ByteBufferUtil.longSize);
					column.putLong(etValue);
					column.putLong(edge.getID());
				}
				column.flip();
			}
			
			if (columnOnly) break;
			
			if (edge.isRelationship()) {
				if (et.isFunctional()) {
					value = ByteBuffer.allocate(2 * ByteBufferUtil.longSize);
					value.putLong(((Relationship) edge).getOtherNode(perspective).getID());
					value.putLong(edge.getID());
				} else {
					value = ByteBuffer.allocate(0);
				}
				value.flip();
			} else {
				assert edge.isProperty();
				DataOutput out = serializer.getDataOutput(defaultOutputCapacity, true);
				if (et.isFunctional()) {
					out.putLong(edge.getID());
				}
				//Write object
				out.writeObjectNotNull(((Property)edge).getAttribute());
				value = out.getByteBuffer();
			}
			break;
		case Labeled:
			EdgeTypeSignature ets = getSignature(tx,et,signatures);
			
			InternalEdge[] keys = new InternalEdge[ets.keyLength()], 
						   values = new InternalEdge[ets.valueLength()];
			List<InternalEdge> rest = new ArrayList<InternalEdge>();
			ets.sort(edge.getEdges(StandardEdgeQuery.queryAll(edge), false),keys,values,rest);
			
			DataOutput out = serializer.getDataOutput(defaultOutputCapacity, true);
			out.putLong(etValue);
			for (int i=0;i<keys.length;i++) writeVirtualEdge(out,keys[i],ets.getKeyEdgeType(i));
			
			if (!et.isFunctional()) {
				assert edge.isRelationship();
				out.putLong(((Relationship) edge).getOtherNode(perspective).getID());
				out.putLong(edge.getID());
			}
			column = out.getByteBuffer();
			
			if (columnOnly) break;
			out = serializer.getDataOutput(defaultOutputCapacity, true);
			if (et.isFunctional()) {
				assert edge.isRelationship();
				out.putLong(((Relationship) edge).getOtherNode(perspective).getID());
				out.putLong(edge.getID());
			}
			for (int i=0;i<values.length;i++) writeVirtualEdge(out,values[i],ets.getValueEdgeType(i));
			for (InternalEdge v: rest) writeVirtualEdge(out,v);
			value = out.getByteBuffer();
			
			break;
		case LabeledRestricted:
			throw new UnsupportedOperationException("Not yet supported!");
		default: throw new AssertionError("Unexpected edge category: " + et.getCategory());
		}
		return new Entry(column,value);
	}
	
//	private void writeRest(DataOutput out, InternalEdge[] edges) {
//		for (int i=0;i<edges.length;i++) {
//			InternalEdge edge = edges[i];
//			assert edge.getEdgeType().getCategory()==EdgeCategory.Simple;
//			if (edge.isProperty()) {
//				out.writeObject(((Property)edge).getAttribute());
//			} else {
//				assert edge.isUnidirected() && edge.isRelationship();
//				out.putLong(edge.getNodeAt(1).getID());
//			}
//		}
//	}
	
	private void writeVirtualEdge(DataOutput out, InternalEdge edge) {
		assert edge!=null;
		writeVirtualEdge(out,edge,edge.getEdgeType(),true);
	}
	
	private void writeVirtualEdge(DataOutput out, InternalEdge edge, EdgeType edgeType) {
		writeVirtualEdge(out,edge,edgeType,false);
	}
	
	private void writeVirtualEdge(DataOutput out, InternalEdge edge, EdgeType edgeType, boolean writeEdgeType) {
		assert edgeType.getCategory()==EdgeCategory.Simple;

		if (edge==null) {
			assert !writeEdgeType;
			if (edgeType.isPropertyType()) {
				out.writeObject(null);
			} else {
				assert edgeType.isRelationshipType();
				out.putLong(0);
			}
		} else {
			if (writeEdgeType) {
				long etValue = edgeType.getID();
				out.putLong(etValue);
			}
			if (edge.isProperty()) {
				out.writeObject(((Property)edge).getAttribute());
			} else {
				assert edge.isUnidirected() && edge.isRelationship();
				out.putLong(edge.getNodeAt(1).getID());
			}
		}
	}
	
	private static EdgeTypeSignature getSignature(GraphTx tx,EdgeType et, Map<EdgeType,EdgeTypeSignature> signatures) {
		EdgeTypeSignature ets = signatures.get(et);
		if (ets==null) {
			ets = new EdgeTypeSignature(et,tx);
			signatures.put(et, ets);
		}
		return ets;
	}

	// Property Index Handling
	
	private void deleteIndexEntry(Property prop, KeyColumnValueStoreMutator propMutator) {
		PropertyType pt = prop.getPropertyType();
		assert pt.getCategory()==EdgeCategory.Simple;
		if (pt.getIndexType().hasIndex()) {
			switch(pt.getIndexType()) {
			case Standard:
				if (pt.isKeyed()) {
//					propertyIndex.delete(getIndexKey(prop.getAttribute()), 
//							ImmutableList.of(getKeyedIndexColumn(prop.getPropertyType())), txh);
					propMutator.delete(getIndexKey(prop.getAttribute()),
							ImmutableList.of(getKeyedIndexColumn(prop.getPropertyType())));
				} else {
//					propertyIndex.delete(getIndexKey(prop.getAttribute()), 
//							ImmutableList.of(getIndexColumn(prop.getPropertyType(),prop.getID())), txh);
					propMutator.delete(getIndexKey(prop.getAttribute()),
							ImmutableList.of(getIndexColumn(prop.getPropertyType(),prop.getID())));
				}
				break;
			default: 
				throw new UnsupportedOperationException("Only standard property index structures are supported for now.");
			}
		}
	}
	
	private void addIndexEntry(Property prop, KeyColumnValueStoreMutator propMutator) {
		PropertyType pt = prop.getPropertyType();
		assert pt.getCategory()==EdgeCategory.Simple;
		if (pt.getIndexType().hasIndex()) {
			PropertyIndex index = pt.getIndexType();
			if (index==PropertyIndex.Standard || index==PropertyIndex.Range) {
				if (pt.isKeyed()) {
//					propertyIndex.insert(getIndexKey(prop.getAttribute()), 
//							ImmutableList.of(new Entry(getKeyedIndexColumn(pt),getIndexValue(prop))), 
//							txh);
					propMutator.insert(getIndexKey(prop.getAttribute()),
							ImmutableList.of(new Entry(getKeyedIndexColumn(pt),getIndexValue(prop))));
				} else {
//					propertyIndex.insert(getIndexKey(prop.getAttribute()), 
//							ImmutableList.of(new Entry(getIndexColumn(pt,prop.getID()),getIndexValue(prop))), 
//							txh);
					propMutator.insert(getIndexKey(prop.getAttribute()),
							ImmutableList.of(new Entry(getIndexColumn(pt,prop.getID()),getIndexValue(prop))));
				}	
			} else throw new UnsupportedOperationException("Only standard and range property index structures are supported for now.");
		}
	}
	
	private ByteBuffer getIndexKey(Object att) {
		DataOutput out = serializer.getDataOutput(defaultOutputCapacity, true);
		out.writeObjectNotNull(att);
		return out.getByteBuffer();
	}
	
	private ByteBuffer getIndexValue(Property prop) {
		assert prop.getEdgeType().getCategory()==EdgeCategory.Simple;
		return ByteBufferUtil.getLongByteBuffer(prop.getStart().getID());
	}
	
	private ByteBuffer getKeyedIndexColumn(PropertyType type) {
		assert type.isKeyed();
		return ByteBufferUtil.getLongByteBuffer(
				idManager.switchEdgeTypeID(type.getID(), EdgeDirection.In.getID()));
	}

	private ByteBuffer getIndexColumn(PropertyType type, long propertyID) {
		assert !type.isKeyed();
		return ByteBufferUtil.getLongByteBuffer(new long[]{
				idManager.switchEdgeTypeID(type.getID(), EdgeDirection.In.getID()),
				propertyID	});
	}
	
}
