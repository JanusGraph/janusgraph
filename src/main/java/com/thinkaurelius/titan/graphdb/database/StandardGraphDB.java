package com.thinkaurelius.titan.graphdb.database;

import cern.colt.list.AbstractLongList;
import cern.colt.list.LongArrayList;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.diskstorage.writeaggregation.*;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.writeaggregation.DirectStoreMutator;
import com.thinkaurelius.titan.diskstorage.writeaggregation.StoreMutator;
import com.thinkaurelius.titan.exceptions.GraphStorageException;
import com.thinkaurelius.titan.graphdb.database.idassigner.NodeIDAssigner;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.database.statistics.TransactionStatistics;
import com.thinkaurelius.titan.graphdb.database.util.EdgeTypeSignature;
import com.thinkaurelius.titan.graphdb.database.util.LimitTracker;
import com.thinkaurelius.titan.graphdb.edgequery.EdgeQueryUtil;
import com.thinkaurelius.titan.graphdb.edgequery.InternalEdgeQuery;
import com.thinkaurelius.titan.util.interval.AtomicInterval;
import com.thinkaurelius.titan.graphdb.edgequery.AtomicEdgeQuery;
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
    private final boolean bufferMutations;
    private final int bufferSize;
	
	private final Serializer serializer;
	
	private final NodeIDAssigner idAssigner;

	
	public StandardGraphDB(GraphDatabaseConfiguration configuration) {
		this.config=configuration;

		this.storage = configuration.getStorageManager();
		this.edgeStore = configuration.getEdgeStore(this.storage);
		this.propertyIndex = configuration.getPropertyIndex(this.storage);
        this.bufferMutations = configuration.hasBufferMutations();
        this.bufferSize = configuration.getBufferSize();
        Preconditions.checkArgument(bufferSize>0);

        this.idAssigner = config.getIDAssigner(this.storage);
        this.idManager = idAssigner.getIDManager();
		
		this.serializer = config.getSerializer();
        this.etManager = new SimpleEdgeTypeManager(this);
	}
	
	@Override
	public IDInspector getIDInspector() {
		return idManager;
	}

    @Override
    public boolean isReferenceNodeID(long nodeID) {
        return false;
    }


	@Override
	public synchronized void close() throws GraphStorageException {
		etManager.close();
        idAssigner.close();

		edgeStore.close();
		propertyIndex.close();
		storage.close();
	}

	@Override
	public GraphDatabaseConfiguration getConfiguration() {
		return config;
	}

	@Override
	public GraphTransaction startTransaction() {
        return startTransaction(GraphTransactionConfig.Standard);
	}

	@Override
	public GraphTx startTransaction(GraphTransactionConfig configuration) {
		return new StandardPersistGraphTx(this,etManager, configuration,storage.beginTransaction());
	}

    // ################### READ #########################

	@Override
	public boolean containsNodeID(long id, GraphTx tx) {
		log.debug("Checking node existence for {}",id);
		return edgeStore.containsKey(IDHandler.getKey(id), tx.getTxHandle());
	}

    @Override
	public long[] indexRetrieval(Object key, PropertyType pt, GraphTx tx) {
		Preconditions.checkArgument(pt.getCategory()==EdgeCategory.Simple,
					"Currently, only simple properties are supported for hasIndex retrieval!");
		Preconditions.checkArgument(pt.hasIndex(),
					"Cannot retrieve for given property type - it does not have an hasIndex.");

		long[] nodes = null;
			
        Preconditions.checkArgument(pt.getDataType().isInstance(key),"Specified object is incompatible with property data type ["+pt.getName()+"].");

        if (pt.isKeyed()) {
            ByteBuffer value = propertyIndex.get(getIndexKey(key), getKeyedIndexColumn(pt), tx.getTxHandle());
            if (value!=null) {
                nodes = new long[1];
                nodes[0]=VariableLong.readPositive(value);
            }
        } else {
            ByteBuffer startColumn = VariableLong.positiveByteBuffer(pt.getID());
            List<Entry> entries = propertyIndex.getSlice(getIndexKey(key), startColumn,
                                                        ByteBufferUtil.nextBiggerBuffer(startColumn), tx.getTxHandle());
            nodes = new long[entries.size()];
            int i = 0;
            for (Entry ent : entries) {
                nodes[i++] = VariableLong.readPositive(ent.getValue());
            }
        }


		if (nodes==null) return new long[0];
		else return nodes;
	}


	@Override
	public AbstractLongList getRawNeighborhood(InternalEdgeQuery query, GraphTx tx) {
        Preconditions.checkArgument(EdgeQueryUtil.queryCoveredByDiskIndexes(query),
                "Raw retrieval is currently does not support in-memory filtering. To be implemented!");
		List<Entry> entries = queryForEntries(query,tx.getTxHandle());
		
        InternalNode node = query.getNode();
		EdgeType edgeType = null;
		if (query.hasEdgeTypeCondition()) edgeType = query.getEdgeTypeCondition();
		
		AbstractLongList result = new LongArrayList();
		
		for (Entry entry : entries) {
            if (!query.hasEdgeTypeCondition()) {
                long etid = IDHandler.readEdgeType(entry.getColumn(), idManager);
                if (edgeType==null || edgeType.getID()!=etid) {
                    edgeType=(EdgeType)tx.getNode(etid);
                }
            }
			if (edgeType.isPropertyType() || (!edgeType.isModifiable() && !query.queryUnmodifiable())) {
				continue; //Skip since it does not match query
			}
			//Get neighboring node id
            long iddiff = VariableLong.read(entry.getValue());
			long nghid = iddiff + node.getID();
			result.add(nghid);

            if (result.size()>=query.getLimit()) break;
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
        Map<String,EdgeType> etCache = new HashMap<String,EdgeType>();

		for (Entry entry : entries) {
			ByteBuffer column = entry.getColumn();
            int dirID = IDHandler.getDirectionID(column.get(column.position()));
			long etid = IDHandler.readEdgeType(column, idManager);

			if (edgeType==null || edgeType.getID()!=etid) {
				edgeType = (EdgeType)tx.getNode(etid);
			}

            Object[] keys = null;
            if (edgeType.getCategory()==EdgeCategory.Labeled) { 
                EdgeTypeDefinition def = ((InternalEdgeType)edgeType).getDefinition();
                String[] keysig = def.getKeySignature();
                keys = new Object[keysig.length];
                for (int i=0;i<keysig.length;i++) 
                    keys[i] = readInline(column,getEdgeType(keysig[i],etCache,tx),tx);
            }

            InternalEdge edge;
            long edgeid=0;
            if (!edgeType.isFunctional()) {
                edgeid = VariableLong.readPositive(column);
            }
            
            ByteBuffer value = entry.getValue();
            if (edgeType.isRelationshipType()) {
                long nodeIDDiff = VariableLong.read(value);
                if (edgeType.isFunctional()) edgeid = VariableLong.readPositive(value);
                assert edgeid>0;
                long otherid = node.getID() + nodeIDDiff;
                InternalNode othernode = tx.getExistingNode(otherid);
                if (dirID==3) {
                    assert (edgeType.getDirectionality()==Directionality.Directed);
                    edge = factory.createExistingRelationship(edgeid, (RelationshipType) edgeType, othernode,node);
                } else {
                    edge = factory.createExistingRelationship(edgeid, (RelationshipType) edgeType, node,othernode);
                }

            } else {
                assert edgeType.isPropertyType();
                assert dirID == 0;
                PropertyType propType = ((PropertyType)edgeType);
                Object attribute = serializer.readObjectNotNull(value, propType.getDataType());
                if (edgeType.isFunctional()) edgeid = VariableLong.readPositive(value);
                assert edgeid>0;
                edge = factory.createExistingProperty(edgeid, propType, node, attribute);
            }
            
			//Read value inline edges if any
			if (edgeType.getCategory()==EdgeCategory.Labeled) { // || EdgeCategory.LabeledRestricted
                EdgeTypeDefinition def = ((InternalEdgeType)edgeType).getDefinition();
                //First create all keys buffered above
                String[] keysig = def.getKeySignature();
                for (int i=0;i<keysig.length;i++) {
                    createInlineEdge(edge,getEdgeType(keysig[i],etCache,tx),keys[i],factory);
                }
                
                //value signature
				for (String str : def.getCompactSignature())
                    readLabel(edge,value,getEdgeType(str,etCache,tx),factory,tx);
				
				//Third: read rest
				while (value.hasRemaining()) {
					EdgeType type = (EdgeType)tx.getExistingNode(IDHandler.readInlineEdgeType(value,idManager));
					readLabel(edge,value,type,factory,tx);
				}
			}
		}
		query.getNode().loadedEdges(query);
	}

    private Object readInline(ByteBuffer read, EdgeType type, GraphTx tx) {
        if (type.isPropertyType()) {
            PropertyType proptype = ((PropertyType) type);
            return serializer.readObject(read,proptype.getDataType());
        } else {
            assert type.isRelationshipType();
            long id = VariableLong.readPositive(read);
            if (id==0) return null;
            else return tx.getExistingNode(id);
        }
    }
    
    private void createInlineEdge(InternalEdge edge, EdgeType type, Object entity, EdgeLoader loader) {
        if (entity!=null) {
            if (type.isRelationshipType()) {
                assert entity instanceof InternalNode;
                loader.createExistingRelationship((RelationshipType)type, edge, (InternalNode)entity);
            } else {
                assert type.isPropertyType();
                loader.createExistingProperty((PropertyType)type, edge, entity);
            }
        }
    }
    
	private void readLabel(InternalEdge edge, ByteBuffer read, EdgeType type, EdgeLoader loader, GraphTx tx) {
		createInlineEdge(edge,type,readInline(read,type,tx),loader);
	}

    private EdgeType getEdgeType(String name, Map<String,EdgeType> etCache, GraphTx tx) {
        EdgeType et = etCache.get(name);
        if (et==null) {
            et = tx.getEdgeType(name);
            etCache.put(name, et);
        }
        return et;
    }
	
    private static boolean[] getAllowedDirections(InternalEdgeQuery query) {
        boolean[] dirs = new boolean[4];
        if (query.queryProperties()) {
            assert query.isAllowedDirection(EdgeDirection.Out);
            dirs[0]=true;
        }
        if (query.queryRelationships()) {
            if (query.hasDirectionCondition()) {
                Direction d = query.getDirectionCondition();
                switch (d) {
                    case In: dirs[3]=true; break;
                    case Out: dirs[2]=true; break;
                    case Both: dirs[2]=true; dirs[3]=true; break;
                    case Undirected: dirs[1]=true; break;
                }
            } else {
                dirs[1]=true; dirs[2]=true; dirs[3]=true;
            }
        }
        return dirs;
    }
    
	private List<Entry> queryForEntries(InternalEdgeQuery query, TransactionHandle txh) {
        Preconditions.checkArgument(query.isAtomic());
		ByteBuffer key = IDHandler.getKey(query.getNodeID());
		List<Entry> entries = null;
		LimitTracker limit = new LimitTracker(query);
		
        boolean dirs[] = getAllowedDirections(query);
        
		if (query.hasEdgeTypeCondition()) {
			EdgeType et = query.getEdgeTypeCondition();
			if (!et.isNew()) { //Result set must be empty if EdgeType is new
                ArrayList<Object> applicableConstraints = null;
                boolean isRange = false;
                if (query.hasConstraints()) {
                    assert et.getCategory()==EdgeCategory.Labeled;
                    EdgeTypeDefinition def = ((InternalEdgeType)et).getDefinition();
                    String[] keysig = def.getKeySignature();
                    applicableConstraints = new ArrayList<Object>(keysig.length);
                    Map<String,Object> constraints = query.getConstraints();
                    for (int i=0;i<keysig.length;i++) {
                        if (constraints.containsKey(keysig[i])) {
                            Object iv = constraints.get(keysig[i]);
                            applicableConstraints.add(iv);
                            if (iv!=null && (iv instanceof AtomicInterval) && ((AtomicInterval)iv).isRange()) {
                                isRange=true;
                                break;
                            }
                        } else break;
                    }
                    if (applicableConstraints.isEmpty()) applicableConstraints=null;
                }
                
                for (int dirID=0;dirID<4;dirID++) {
                    if (dirs[dirID]) {
                        if (applicableConstraints!=null) {
                            assert !applicableConstraints.isEmpty();

                            DataOutput start = serializer.getDataOutput(defaultOutputCapacity, true);
                            DataOutput end = null;
                            if (isRange) end = serializer.getDataOutput(defaultOutputCapacity, true);
                            
                            IDHandler.writeEdgeType(start,et.getID(),dirID,idManager);
                            if (isRange) IDHandler.writeEdgeType(end,et.getID(),dirID,idManager);
                            
                            //Write all applicable key constraints
                            for (Object iv : applicableConstraints) {
                                if (iv instanceof AtomicInterval) {
                                    AtomicInterval interval = (AtomicInterval)iv;
                                    if (interval.isPoint()) {
                                        start.writeObject(interval.getStartPoint());
                                        if (isRange) end.writeObject(interval.getStartPoint());
                                    } else {
                                        assert isRange;
                                        assert interval.isRange();

                                        ByteBuffer startColumn, endColumn;
                                        
                                        if (interval.getStartPoint()!=null) {
                                            start.writeObject(interval.getStartPoint());
                                            startColumn = start.getByteBuffer();
                                            if (!interval.startInclusive())
                                                startColumn = ByteBufferUtil.nextBiggerBuffer(startColumn);
                                        } else {
                                            assert interval.startInclusive();
                                            startColumn = start.getByteBuffer();
                                        }
                                        
                                        if (interval.getEndPoint()!=null) {
                                            end.writeObject(interval.getEndPoint());
                                        } else {
                                            assert interval.endInclusive();
                                        }
                                        endColumn = end.getByteBuffer();
                                        if (interval.endInclusive())
                                            endColumn = ByteBufferUtil.nextBiggerBuffer(endColumn);

                                        entries = appendResults(key,start.getByteBuffer(),end.getByteBuffer(),entries,limit,txh);
                                        break; //redundant, this must be the last iteration because its a range
                                    }
                                } else {
                                    assert iv instanceof Node;
                                    long id = 0;
                                    if (iv!=null) id = ((Node)iv).getID();
                                    VariableLong.writePositive(start,id);
                                    if (isRange) VariableLong.writePositive(end,id);
                                }
                            }
                            if (!isRange) entries = appendResults(key,start.getByteBuffer(),entries,limit,txh);
                        } else {
                            ByteBuffer columnStart = IDHandler.getEdgeType(et.getID(),dirID,idManager);
                            entries = appendResults(key,columnStart,entries,limit,txh);
                        }
                        
                    }
                }
			}
		} else if (query.hasEdgeTypeGroupCondition()) {
			int groupid = query.getEdgeTypeGroupCondition().getID();
            for (int dirID=0;dirID<4;dirID++) {
                if (dirs[dirID]) {
                    ByteBuffer columnStart = IDHandler.getEdgeTypeGroup(groupid,dirID,idManager);
                    entries = appendResults(key,columnStart,entries,limit,txh);
                }
            }
		} else {
            int lastDirID = -1;
            for (int dirID=0;dirID<=4;dirID++) {
                if ( (dirID>=4 || !dirs[dirID]) && lastDirID>=0) {
                    ByteBuffer columnStart = IDHandler.getEdgeTypeGroup(0,lastDirID,idManager);
                    ByteBuffer columnEnd = IDHandler.getEdgeTypeGroup(idManager.getMaxGroupID()+1,dirID-1,idManager);
                    entries = appendResults(key,columnStart,columnEnd,entries,limit,txh);
                    lastDirID = -1;
                }
                if (dirID<4) {
                    if (dirs[dirID] && lastDirID==-1) lastDirID = dirID;
                }
            }
		}

		if (entries==null) return ImmutableList.of();
		else return entries;
	}

    private List<Entry> appendResults(ByteBuffer key, ByteBuffer columnPrefix,
                                      List<Entry> entries, LimitTracker limit, TransactionHandle txh) {
        return appendResults(key,columnPrefix,ByteBufferUtil.nextBiggerBuffer(columnPrefix),entries,limit,txh);
    }

    private List<Entry> appendResults(ByteBuffer key, ByteBuffer columnStart, ByteBuffer columnEnd,
                                      List<Entry> entries, LimitTracker limit, TransactionHandle txh) {
		if (limit.limitExhausted()) return null;
		List<Entry> results = null;
        results = edgeStore.getSlice(key, columnStart, columnEnd, limit.getLimit(), txh);
        limit.retrieved(results.size());

		if (results==null) return null;
		else if (entries==null) return results;
		else {
			entries.addAll(results);
			return entries;
		}
	}

    // ################### WRITE #########################
	
	private final StoreMutator getStoreMutator(TransactionHandle txh) {
		if (bufferMutations) {
            if (edgeStore instanceof MultiWriteKeyColumnValueStore &&
                    propertyIndex instanceof MultiWriteKeyColumnValueStore) {
                return new BatchStoreMutator(txh, (MultiWriteKeyColumnValueStore)edgeStore, (MultiWriteKeyColumnValueStore)propertyIndex, bufferSize);
            } else {
				log.error("Storage backend does not support buffered writes, hence disabled: {}", storage.getClass().getCanonicalName());
			}
		}
		return new DirectStoreMutator(txh, edgeStore, propertyIndex);
	}


    @Override
    public void assignID(InternalNode node) {
        assert !node.hasID();
        node.setID(idAssigner.getNewID(node));
    }

	private void assignIDs(Collection<InternalEdge> addedEdges,GraphTx tx) {
		for (InternalEdge edge : addedEdges) {
			if (edge.isDeleted()) continue;
			if (edge.hasID()) continue;
			
			for (int i=0;i<edge.getArity();i++) {
				InternalNode node = edge.getNodeAt(i);
				if (!node.hasID()) {
					assert node.isNew();
                    assignID(node);
				}
			}
			assert !edge.hasID();
			assignID(edge);
		}
	}
	
	@Override
	public boolean save(Collection<InternalEdge> addedEdges,
			Collection<InternalEdge> deletedEdges, GraphTx tx) throws GraphStorageException {
		log.debug("Saving transaction. Added {}, removed {}",addedEdges.size(),deletedEdges.size());
		Map<EdgeType,EdgeTypeSignature> signatures = new HashMap<EdgeType,EdgeTypeSignature>();
		TransactionHandle txh = tx.getTxHandle();
		TransactionStatistics stats = new TransactionStatistics();
		
		StoreMutator mutator = getStoreMutator(txh);

		//1. Assign Node IDs
		assignIDs(addedEdges,tx);

        //2. Collect deleted edges
        ListMultimap<InternalNode,InternalEdge> mutations = ArrayListMultimap.create();
        if (deletedEdges!=null && !deletedEdges.isEmpty()) {
            for (InternalEdge del : deletedEdges) {
                assert del.isDeleted();
                for (int pos=0;pos<del.getArity();pos++) {
                    InternalNode node = del.getNodeAt(pos);
                    if (pos==0 || !del.isUnidirected() ) {
                        mutations.put(node,del);
                        if (node.isDeleted()) stats.removedNode(node);
                    }
                    if (pos==0 && del.getEdgeType().isFunctional()) {
                        Entry entry = getEntry(tx,del,node,signatures);
                        mutator.acquireEdgeLock(IDHandler.getKey(node.getID()),entry.getColumn(),entry.getValue());
                    }
                }
                if (del.isProperty()) {
                    lockKeyedProperty((Property)del,mutator);
                }

                stats.removedEdge(del);
            }
        }
        deletedEdges=null;


		ListMultimap<InternalEdgeType,InternalEdge> simpleEdgeTypes = null;
		ListMultimap<InternalEdgeType,InternalEdge> otherEdgeTypes = null;
		
		//3. Sort Added Edges
		for (InternalEdge edge : addedEdges) {
			if (edge.isDeleted()) continue;
            assert edge.isNew();
            
			EdgeType et = edge.getEdgeType();
			stats.addedEdge(edge);
			
			//Give special treatment to edge type definitions
			if (et==SystemPropertyType.EdgeTypeName || et==SystemPropertyType.PropertyTypeDefinition 
					|| et==SystemPropertyType.RelationshipTypeDefinition) {
				assert edge.getNodeAt(0) instanceof InternalEdgeType;
				InternalEdgeType node = (InternalEdgeType)edge.getNodeAt(0);
				assert node.hasID();
				if (node.getCategory()==EdgeCategory.Simple) {
					if (simpleEdgeTypes==null) simpleEdgeTypes=ArrayListMultimap.create();
					if (node.isNew() && !simpleEdgeTypes.containsKey(node)) stats.addedNode(node);
					simpleEdgeTypes.put(node,edge);
				} else {
					if (otherEdgeTypes==null) otherEdgeTypes=ArrayListMultimap.create();
					otherEdgeTypes.put(node,edge);					
				}
			} else { //Standard Edge
				assert (edge.getArity()==1 && edge.isProperty()) || (edge.getArity()==2 && edge.isRelationship());
				for (int pos=0;pos<edge.getArity();pos++) {
					InternalNode node = edge.getNodeAt(pos);
					assert node.hasID();
					if (pos==0 || !edge.isUnidirected()) {
						if (node.isNew() && !mutations.containsKey(node)) stats.addedNode(node);
						mutations.put(node, edge);
					}
                    if (pos==0 && edge.getEdgeType().isFunctional() && !node.isNew()) {
                        Entry entry = getEntry(tx,edge,node,signatures,true);
                        mutator.acquireEdgeLock(IDHandler.getKey(node.getID()),entry.getColumn(),null);
                    }
				}
			}
            if (edge.isProperty()) {
                lockKeyedProperty((Property)edge,mutator);
            }
		}
		addedEdges.clear(); addedEdges=null;
		
		//3. Persist
		if (simpleEdgeTypes!=null) persist(simpleEdgeTypes,signatures,tx,mutator);
		if (otherEdgeTypes!=null) persist(otherEdgeTypes,signatures,tx,mutator);
        mutator.flush();

        //Commit saved EdgeTypes to EdgeTypeManager
        if (simpleEdgeTypes!=null) commitEdgeTypes(simpleEdgeTypes.keySet());
        if (otherEdgeTypes!=null) commitEdgeTypes(otherEdgeTypes.keySet());

		if (!mutations.isEmpty()) persist(mutations,signatures,tx,mutator);
        mutator.flush();

		//Update statistics: statistics.update(stats);

		return true;
	}
	
	private void commitEdgeTypes(Iterable<InternalEdgeType> edgeTypes) {
		for (InternalEdgeType et : edgeTypes) etManager.committed(et);
	}
	

	private<N extends InternalNode> void persist(ListMultimap<N,InternalEdge> mutatedEdges, Map<EdgeType,EdgeTypeSignature> signatures,
			GraphTx tx, StoreMutator mutator) {
		assert mutatedEdges!=null && !mutatedEdges.isEmpty();

		Collection<N> nodes = mutatedEdges.keySet();
//		if (sortNodes) {
//			List<N> sortednodes = new ArrayList<N>(nodes);
//			Collections.sort(sortednodes, new Comparator<N>(){
//
//				@Override
//				public int compare(N o1, N o2) {
//					assert o1.getID()!=o2.getID();
//					if (o1.getID()<o2.getID()) return -1;
//					else return 1;
//				}
//
//			});
//			nodes=sortednodes;
//		}

		for (N node : nodes) {
			List<InternalEdge> edges = mutatedEdges.get(node);
			List<Entry> additions = new ArrayList<Entry>(edges.size());
            List<ByteBuffer> deletions = new ArrayList<ByteBuffer>(Math.max(10,edges.size()/10));
            List<Property> properties = new ArrayList<Property>();
			for (InternalEdge edge : edges) {
                if (edge.isDeleted()) {
                    if (edge.isProperty()) {
                        deleteIndexEntry((Property)edge, mutator);
                    }
                    deletions.add(getEntry(tx,edge,node,signatures,true).getColumn());
                } else {
                    assert edge.isNew();
                    if (edge.isProperty()) properties.add((Property)edge);
                    additions.add(getEntry(tx,edge,node,signatures));
                }

			}
			mutator.mutateEdges(IDHandler.getKey(node.getID()), additions, deletions);
            //Persist property index for retrieval
            for (Property prop : properties) {
                addIndexEntry(prop, mutator);
            }
        }

	}

	private Entry getEntry(GraphTx tx,InternalEdge edge, InternalNode perspective, Map<EdgeType,EdgeTypeSignature> signatures) {
		return getEntry(tx,edge,perspective,signatures,false);
	}

	private Entry getEntry(GraphTx tx,InternalEdge edge, InternalNode perspective, Map<EdgeType,EdgeTypeSignature> signatures, boolean columnOnly) {
		EdgeType et = edge.getEdgeType();
        long etid = et.getID();

        int dirID;
        if (edge.isProperty()) {
            dirID = 0;
        } else if (et.getDirectionality()==Directionality.Undirected) {
            assert edge.isUndirected();
            dirID = 1;
        } else if (edge.getNodeAt(0).equals(perspective)) {
            //Out Edge
            assert edge.isDirected() || edge.isUnidirected();
            dirID = 2;
        } else {
            //In Edge
            assert edge.isDirected() && edge.getNodeAt(1).equals(perspective);
            dirID = 3;
        }
		
        int etIDLength = IDHandler.edgeTypeLength(etid,idManager);
        
		ByteBuffer column=null,value=null;
		switch(et.getCategory()) {
		case Simple:
			if (et.isFunctional()) {
                column = ByteBuffer.allocate(etIDLength);
                IDHandler.writeEdgeType(column,etid,dirID,idManager);
			} else {
                column = ByteBuffer.allocate(etIDLength + VariableLong.positiveLength(edge.getID()));
                IDHandler.writeEdgeType(column,etid,dirID,idManager);
                VariableLong.writePositive(column,edge.getID());
			}
            column.flip();
			if (columnOnly) break;
			
			if (edge.isRelationship()) {
                long nodeIDDiff = ((Relationship) edge).getOtherNode(perspective).getID() - perspective.getID();
                int nodeIDDiffLength = VariableLong.length(nodeIDDiff);
				if (et.isFunctional()) {
					value = ByteBuffer.allocate(nodeIDDiffLength + VariableLong.positiveLength(edge.getID()));
                    VariableLong.write(value,nodeIDDiff);
                    VariableLong.writePositive(value, edge.getID());
				} else {
					value = ByteBuffer.allocate(nodeIDDiffLength);
                    VariableLong.write(value,nodeIDDiff);
				}
				value.flip();
			} else {
				assert edge.isProperty();
				DataOutput out = serializer.getDataOutput(defaultOutputCapacity, true);
				//Write object
				out.writeObjectNotNull(((Property)edge).getAttribute());
                if (et.isFunctional()) {
                    VariableLong.writePositive(out,edge.getID());
                }
				value = out.getByteBuffer();
			}
			break;
		case Labeled:
			EdgeTypeSignature ets = getSignature(tx,et,signatures);
			
			InternalEdge[] keys = new InternalEdge[ets.keyLength()], 
						   values = new InternalEdge[ets.valueLength()];
			List<InternalEdge> rest = new ArrayList<InternalEdge>();
			ets.sort(edge.getEdges(AtomicEdgeQuery.queryAll(edge), false),keys,values,rest);
			
			DataOutput out = serializer.getDataOutput(defaultOutputCapacity, true);
            IDHandler.writeEdgeType(out,etid,dirID,idManager);

			for (int i=0;i<keys.length;i++) writeInlineEdge(out, keys[i], ets.getKeyEdgeType(i));
			
			if (!et.isFunctional()) {
				VariableLong.writePositive(out,edge.getID());
			}
			column = out.getByteBuffer();
			
			if (columnOnly) break;
			out = serializer.getDataOutput(defaultOutputCapacity, true);
            
            if (edge.isRelationship()) {
                long nodeIDDiff = ((Relationship) edge).getOtherNode(perspective).getID() - perspective.getID();
                VariableLong.write(out,nodeIDDiff);
            } else {
                assert edge.isProperty();
                out.writeObjectNotNull(((Property)edge).getAttribute());
            }

			if (et.isFunctional()) {
				assert edge.isRelationship();
                VariableLong.writePositive(out, edge.getID());
			}
			for (int i=0;i<values.length;i++) writeInlineEdge(out, values[i], ets.getValueEdgeType(i));
			for (InternalEdge v: rest) writeInlineEdge(out, v);
			value = out.getByteBuffer();
			
			break;
		default: throw new AssertionError("Unexpected edge category: " + et.getCategory());
		}
		return new Entry(column,value);
	}
	
	private void writeInlineEdge(DataOutput out, InternalEdge edge) {
		assert edge!=null;
		writeVirtualEdge(out,edge,edge.getEdgeType(),true);
	}
	
	private void writeInlineEdge(DataOutput out, InternalEdge edge, EdgeType edgeType) {
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
                VariableLong.writePositive(out, 0);
			}
		} else {
			if (writeEdgeType) {
                IDHandler.writeInlineEdgeType(out, edgeType.getID(), idManager);
			}
			if (edge.isProperty()) {
				out.writeObject(((Property)edge).getAttribute());
			} else {
				assert edge.isUnidirected() && edge.isRelationship();
                VariableLong.writePositive(out, edge.getNodeAt(1).getID());
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

    // ################### PROPERTY INDEX HANDLING #########################

	
    private void lockKeyedProperty(Property prop, StoreMutator mutator) {
        PropertyType pt = prop.getPropertyType();
        assert pt.getCategory()==EdgeCategory.Simple;
        if (pt.hasIndex() && pt.isKeyed()) {
            if (prop.isNew()) {
                mutator.acquireIndexLock(getIndexKey(prop.getAttribute()), getKeyedIndexColumn(pt), null);
            } else {
                assert prop.isDeleted();
                mutator.acquireIndexLock(getIndexKey(prop.getAttribute()), getKeyedIndexColumn(pt), getIndexValue(prop));
            }
        }
    }
    
    
	private void deleteIndexEntry(Property prop, StoreMutator mutator) {
		PropertyType pt = prop.getPropertyType();
		assert pt.getCategory()==EdgeCategory.Simple;
		if (pt.hasIndex()) {
            if (pt.isKeyed()) {
                mutator.mutateIndex(getIndexKey(prop.getAttribute()), null,
                        Lists.newArrayList(getKeyedIndexColumn(prop.getPropertyType())));
            } else {
                mutator.mutateIndex(getIndexKey(prop.getAttribute()), null,
                        Lists.newArrayList(getIndexColumn(prop.getPropertyType(), prop.getID())));
            }

		}
	}
	
	private void addIndexEntry(Property prop, StoreMutator mutator) {
		PropertyType pt = prop.getPropertyType();
		assert pt.getCategory()==EdgeCategory.Simple;
		if (pt.hasIndex()) {
            if (pt.isKeyed()) {
                mutator.mutateIndex(getIndexKey(prop.getAttribute()),
                        Lists.newArrayList(new Entry(getKeyedIndexColumn(pt), getIndexValue(prop))), null);
            } else {
                mutator.mutateIndex(getIndexKey(prop.getAttribute()),
                        Lists.newArrayList(new Entry(getIndexColumn(pt, prop.getID()), getIndexValue(prop))), null);
            }
		}
	}
	
	private ByteBuffer getIndexKey(Object att) {
		DataOutput out = serializer.getDataOutput(defaultOutputCapacity, true);
		out.writeObjectNotNull(att);
		return out.getByteBuffer();
	}
	
	private ByteBuffer getIndexValue(Property prop) {
		assert prop.getEdgeType().getCategory()==EdgeCategory.Simple;
        return VariableLong.positiveByteBuffer(prop.getStart().getID());
	}
	
	private ByteBuffer getKeyedIndexColumn(PropertyType type) {
		assert type.isKeyed();
        return VariableLong.positiveByteBuffer(type.getID());
	}

	private ByteBuffer getIndexColumn(PropertyType type, long propertyID) {
		assert !type.isKeyed();
		return VariableLong.positiveByteBuffer(new long[]{ type.getID(),propertyID });
	}
	
}
