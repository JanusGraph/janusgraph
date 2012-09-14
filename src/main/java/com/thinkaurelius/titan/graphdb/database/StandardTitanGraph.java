package com.thinkaurelius.titan.graphdb.database;

import cern.colt.list.AbstractLongList;
import cern.colt.list.LongArrayList;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.writeaggregation.*;
import com.thinkaurelius.titan.graphdb.blueprints.TitanBlueprintsGraph;
import com.thinkaurelius.titan.graphdb.blueprints.TitanFeatures;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.writeaggregation.DirectStoreMutator;
import com.thinkaurelius.titan.diskstorage.writeaggregation.StoreMutator;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.graphdb.database.idassigner.VertexIDAssigner;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.database.util.TypeSignature;
import com.thinkaurelius.titan.graphdb.database.util.LimitTracker;
import com.thinkaurelius.titan.graphdb.query.AtomicQuery;
import com.thinkaurelius.titan.graphdb.query.SimpleAtomicQuery;
import com.thinkaurelius.titan.graphdb.query.QueryUtil;
import com.thinkaurelius.titan.graphdb.relations.InternalRelation;
import com.thinkaurelius.titan.graphdb.relations.factory.RelationLoader;
import com.thinkaurelius.titan.graphdb.types.InternalTitanType;
import com.thinkaurelius.titan.graphdb.types.manager.SimpleTypeManager;
import com.thinkaurelius.titan.graphdb.types.manager.TypeManager;
import com.thinkaurelius.titan.graphdb.types.system.SystemTypeManager;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;
import com.thinkaurelius.titan.graphdb.transaction.TransactionConfig;
import com.thinkaurelius.titan.util.interval.AtomicInterval;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.thinkaurelius.titan.graphdb.types.TypeDefinition;
import com.thinkaurelius.titan.graphdb.idmanagement.IDInspector;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.transaction.StandardPersistTitanTx;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Features;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

public class StandardTitanGraph extends TitanBlueprintsGraph implements InternalTitanGraph {

	private static final int defaultOutputCapacity = 128;
	
	private static final Logger log =
		LoggerFactory.getLogger(StandardTitanGraph.class);
	
	private final GraphDatabaseConfiguration config;
	private final IDManager idManager;
	private final TypeManager etManager;
	
	private final StorageManager storage;
	private final OrderedKeyColumnValueStore edgeStore;
	private final OrderedKeyColumnValueStore propertyIndex;
    private final boolean bufferMutations;
    private final int bufferSize;

    private final int maxSaveRetryAttempts;
    private final int retrySaveWaitTime;

	
	private final Serializer serializer;
	
	private final VertexIDAssigner idAssigner;
    private boolean isOpen;
	
	public StandardTitanGraph(GraphDatabaseConfiguration configuration) {
		this.config=configuration;
		this.storage = configuration.getStorageManager();
		this.edgeStore = configuration.getEdgeStore(this.storage);
		this.propertyIndex = configuration.getPropertyIndex(this.storage);
        this.bufferMutations = configuration.hasBufferMutations();
        this.bufferSize = configuration.getBufferSize();
        Preconditions.checkArgument(bufferSize>0);
        this.maxSaveRetryAttempts=config.getPersistAttempts();
        this.retrySaveWaitTime=config.getPersistWaittime();

        this.idAssigner = config.getIDAssigner(this.storage);
        this.idManager = idAssigner.getIDManager();
		
		this.serializer = config.getSerializer();
        this.etManager = new SimpleTypeManager(this);
        isOpen = true;
	}
	
	@Override
	public IDInspector getIDInspector() {
		return idManager;
	}

    @Override
    public boolean isReferenceVertexID(long vertexid) {
        return false;
    }

    @Override
    public boolean isOpen() {
        return isOpen;
    }

	@Override
	public synchronized void shutdown() throws TitanException {
        if (!isOpen) return;
        super.shutdown();
		etManager.close();
        idAssigner.close();

        try {
            edgeStore.close();
            propertyIndex.close();
            storage.close();
        } catch (StorageException e) {
            throw new TitanException("Could not close storage backend",e);
        }
        isOpen=false;
	}

	public GraphDatabaseConfiguration getConfiguration() {
		return config;
	}

    @Override
    public Features getFeatures() {
        return TitanFeatures.getFeatures(getConfiguration(),storage.getFeatures());
    }

	@Override
	public TitanTransaction startTransaction() {
        return startTransaction(new TransactionConfig(config));
	}

	@Override
	public InternalTitanTransaction startTransaction(TransactionConfig configuration) {
        try {
		    return new StandardPersistTitanTx(this,etManager, configuration,storage.beginTransaction());
        } catch (StorageException e) {
            throw new TitanException("Could not start new transaction",e);
        }
	}

    // ################### READ #########################

    private final TitanException readException(StorageException e) {
        return new TitanException("Could not read from storage",e);
    }

	@Override
	public boolean containsVertexID(long id, InternalTitanTransaction tx) {
		log.trace("Checking node existence for {}", id);
        try {
		    return edgeStore.containsKey(IDHandler.getKey(id), tx.getTxHandle());
        } catch(StorageException e) {throw readException(e); }
	}

    @Override
    public RecordIterator<Long> getVertexIDs(final InternalTitanTransaction tx) {
        if (!(edgeStore instanceof ScanKeyColumnValueStore))
            throw new UnsupportedOperationException("The configured storage backend does not support global graph operations - use Faunus instead");

        try {
            final RecordIterator<ByteBuffer> keyiter = ((ScanKeyColumnValueStore)edgeStore).getKeys(tx.getTxHandle());
            return new RecordIterator<Long>() {

                @Override
                public boolean hasNext() throws StorageException {
                    return keyiter.hasNext();
                }

                @Override
                public Long next() throws StorageException {
                    return IDHandler.getKeyID(keyiter.next());
                }

                @Override
                public void close() throws StorageException {
                    keyiter.close();
                }
            };
        } catch (StorageException e) { throw readException(e); }
    }

    @Override
	public long[] indexRetrieval(Object key, TitanKey pt, InternalTitanTransaction tx) {
		Preconditions.checkArgument(pt.isSimple(),
					"Currently, only simple properties are supported for index retrieval");
		Preconditions.checkArgument(pt.hasIndex(),
					"Cannot retrieve for given property key - it does not have an index");

		long[] vertices = null;
			
        Preconditions.checkArgument(pt.getDataType().isInstance(key),"Specified object is incompatible with property data type ["+pt.getName()+"]");

        try {
            if (pt.isUnique()) {
                ByteBuffer value = propertyIndex.get(getIndexKey(key), getKeyedIndexColumn(pt), tx.getTxHandle());
                if (value!=null) {
                    vertices = new long[1];
                    vertices[0]=VariableLong.readPositive(value);
                }
            } else {
                ByteBuffer startColumn = VariableLong.positiveByteBuffer(pt.getID());
                List<Entry> entries = propertyIndex.getSlice(getIndexKey(key), startColumn,
                                                            ByteBufferUtil.nextBiggerBuffer(startColumn), tx.getTxHandle());
                vertices = new long[entries.size()];
                int i = 0;
                for (Entry ent : entries) {
                    vertices[i++] = VariableLong.readPositive(ent.getValue());
                }
            }
        } catch (StorageException e) { throw readException(e); }

		if (vertices==null) return new long[0];
		else return vertices;
	}

    private final TitanType getTypeFromID(long etid, InternalTitanTransaction tx) {
        if (idManager.getGroupID(etid)==SystemTypeManager.SYSTEM_TYPE_GROUP.getID()) {
            //its a systemtype
            return SystemTypeManager.getSystemEdgeType(etid);
        } else {
            return (TitanType)tx.getVertex(etid);
        }
    }

	@Override
	public AbstractLongList getRawNeighborhood(AtomicQuery query, InternalTitanTransaction tx) {
        Preconditions.checkArgument(QueryUtil.queryCoveredByDiskIndexes(query),
                "Raw retrieval is currently does not support in-memory filtering");
		List<Entry> entries = queryForEntries(query,tx.getTxHandle());
		
        InternalTitanVertex node = query.getNode();
		TitanType titanType = null;
		if (query.hasEdgeTypeCondition()) titanType = query.getTypeCondition();
		
		AbstractLongList result = new LongArrayList();
		
		for (Entry entry : entries) {
            if (!query.hasEdgeTypeCondition()) {
                long etid = IDHandler.readEdgeType(entry.getColumn(), idManager);
                if (titanType==null || titanType.getID()!=etid) {
                    titanType = getTypeFromID(etid,tx);
                }
            }
			if (titanType.isPropertyKey() || (!titanType.isModifiable() && !query.queryUnmodifiable())) {
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
	public void loadRelations(AtomicQuery query, InternalTitanTransaction tx) {
        List<Entry> entries = queryForEntries(query,tx.getTxHandle());
        RelationLoader factory = tx.getRelationFactory();
        VertexRelationLoader loader = new StandardVertexRelationLoader(query.getNode(),factory);
        loadRelations(entries,loader,tx);
        query.getNode().loadedEdges(query);
    }
    
    protected void loadRelations(Iterable<Entry> entries, VertexRelationLoader loader, InternalTitanTransaction tx) {
        Map<String,TitanType> etCache = new HashMap<String,TitanType>();
        TitanType titanType = null;

		for (Entry entry : entries) {
			ByteBuffer column = entry.getColumn();
            int dirID = IDHandler.getDirectionID(column.get(column.position()));
			long etid = IDHandler.readEdgeType(column, idManager);

			if (titanType ==null || titanType.getID()!=etid) {
                titanType = getTypeFromID(etid,tx);
			}

            Object[] keys = null;
            if (!titanType.isSimple()) {
                TypeDefinition def = ((InternalTitanType) titanType).getDefinition();
                String[] keysig = def.getKeySignature();
                keys = new Object[keysig.length];
                for (int i=0;i<keysig.length;i++) 
                    keys[i] = readInline(column,getEdgeType(keysig[i],etCache,tx));
            }

            long edgeid=0;
            if (!titanType.isFunctional()) {
                edgeid = VariableLong.readPositive(column);
            }
            
            ByteBuffer value = entry.getValue();
            if (titanType.isEdgeLabel()) {
                long nodeIDDiff = VariableLong.read(value);
                if (titanType.isFunctional()) edgeid = VariableLong.readPositive(value);
                assert edgeid>0;
                long otherid = loader.getVertexId() + nodeIDDiff;
                assert dirID==3 || dirID==2;
                Direction dir = dirID==3?Direction.IN:Direction.OUT;
                loader.loadEdge(edgeid,(TitanLabel)titanType,dir,otherid);
            } else {
                assert titanType.isPropertyKey();
                assert dirID == 0;
                TitanKey propType = ((TitanKey) titanType);
                Object attribute = null;

                if (hasGenericDataType(propType)) {
                    attribute = serializer.readClassAndObject(value);
                } else {
                    attribute = serializer.readObjectNotNull(value, propType.getDataType());
                }
                assert attribute!=null;

                if (titanType.isFunctional()) edgeid = VariableLong.readPositive(value);
                assert edgeid>0;
                loader.loadProperty(edgeid,propType,attribute);

            }
            
			//Read value inline edges if any
			if (!titanType.isSimple()) {
                TypeDefinition def = ((InternalTitanType) titanType).getDefinition();
                //First create all keys buffered above
                String[] keysig = def.getKeySignature();
                for (int i=0;i<keysig.length;i++) {
                    createInlineEdge(loader,getEdgeType(keysig[i],etCache,tx),keys[i]);
                }
                
                //value signature
				for (String str : def.getCompactSignature())
                    readLabel(loader,value,getEdgeType(str,etCache,tx));
				
				//Third: read rest
				while (value.hasRemaining()) {
					TitanType type = (TitanType)tx.getExistingVertex(IDHandler.readInlineEdgeType(value, idManager));
					readLabel(loader,value,type);
				}
			}
		}
	}

    private Object readInline(ByteBuffer read, TitanType type) {
        if (type.isPropertyKey()) {
            TitanKey proptype = ((TitanKey) type);
            if (hasGenericDataType(proptype))
                return serializer.readClassAndObject(read);
            else return serializer.readObject(read, proptype.getDataType());
        } else {
            assert type.isEdgeLabel();
            Long id = Long.valueOf(VariableLong.readPositive(read));
            if (id.longValue()==0) return null;
            else return id;
        }
    }
    
    private void createInlineEdge(VertexRelationLoader loader, TitanType type, Object entity) {
        if (entity!=null) {
            if (type.isEdgeLabel()) {
                assert entity instanceof Long;
                loader.addRelationEdge((TitanLabel)type,(Long)entity);
            } else {
                assert type.isPropertyKey();
                loader.addRelationProperty((TitanKey)type,entity);
            }
        }
    }
    
	private void readLabel(VertexRelationLoader loader, ByteBuffer read, TitanType type) {
		createInlineEdge(loader,type,readInline(read,type));
	}

    private TitanType getEdgeType(String name, Map<String,TitanType> etCache, InternalTitanTransaction tx) {
        TitanType et = etCache.get(name);
        if (et==null) {
            et = tx.getType(name);
            etCache.put(name, et);
        }
        return et;
    }
	
    private static boolean[] getAllowedDirections(AtomicQuery query) {
        boolean[] dirs = new boolean[4];
        if (query.queryProperties()) {
            assert query.isAllowedDirection(EdgeDirection.OUT);
            dirs[0]=true;
        }
        if (query.queryRelationships()) {
            Direction d = Direction.BOTH;
            if (query.hasDirectionCondition()) {
                d = query.getDirectionCondition();
            }
            switch (d) {
                case IN: dirs[3]=true; break;
                case OUT: dirs[2]=true; break;
                case BOTH: dirs[2]=true; dirs[3]=true; break;
            }
        }
        return dirs;
    }

	private List<Entry> queryForEntries(AtomicQuery query, TransactionHandle txh) {
		ByteBuffer key = IDHandler.getKey(query.getVertexID());
		List<Entry> entries = null;
		LimitTracker limit = new LimitTracker(query);
		
        boolean dirs[] = getAllowedDirections(query);
        
		if (query.hasEdgeTypeCondition()) {
			TitanType et = query.getTypeCondition();
			if (!et.isNew()) { //Result set must be empty if TitanType is new
                ArrayList<Object> applicableConstraints = null;
                boolean isRange = false;
                if (query.hasConstraints()) {
                    assert !et.isSimple();
                    TypeDefinition def = ((InternalTitanType)et).getDefinition();
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

                                        entries = appendResults(key,startColumn,endColumn,entries,limit,txh);
                                        break; //redundant, this must be the last iteration because its a range
                                    }
                                } else {
                                    assert iv==null || (iv instanceof TitanVertex);
                                    long id = 0;
                                    if (iv!=null) id = ((TitanVertex)iv).getID();
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
		} else if (query.hasGroupCondition()) {
			int groupid = query.getGroupCondition().getID();
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
        try {
            results = edgeStore.getSlice(key, columnStart, columnEnd, limit.getLimit(), txh);
        } catch (StorageException e) { throw readException(e); }
        limit.retrieved(results.size());

		if (entries==null) return results;
		else {
			entries.addAll(results);
			return entries;
		}
	}

    // ################### WRITE #########################
	
	private final StoreMutator getStoreMutator(TransactionHandle txh) {
        if (edgeStore instanceof MultiWriteKeyColumnValueStore &&
                propertyIndex instanceof MultiWriteKeyColumnValueStore) {
            if (config.isBatchLoading()) {
//                return new BatchStoreMutator(txh, (MultiWriteKeyColumnValueStore)edgeStore, (MultiWriteKeyColumnValueStore)propertyIndex, bufferSize, 8);
                return new BufferStoreMutator(txh, (MultiWriteKeyColumnValueStore)edgeStore, (MultiWriteKeyColumnValueStore)propertyIndex, bufferSize);
            } else if (bufferMutations) {
                return new BufferStoreMutator(txh, (MultiWriteKeyColumnValueStore)edgeStore, (MultiWriteKeyColumnValueStore)propertyIndex, bufferSize);
            }
        }
		return new DirectStoreMutator(txh, edgeStore, propertyIndex);
	}


    @Override
    public void assignID(InternalTitanVertex vertex) {
        assert !vertex.hasID();
        vertex.setID(idAssigner.getNewID(vertex));
    }

	private void assignIDs(Collection<InternalRelation> addedEdges,InternalTitanTransaction tx) {
		for (InternalRelation edge : addedEdges) {
			if (edge.isRemoved()) continue;
			if (edge.hasID()) continue;
			
			for (int i=0;i<edge.getArity();i++) {
				InternalTitanVertex node = edge.getVertex(i);
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
	public void save(final Collection<InternalRelation> addedRelations,
			final Collection<InternalRelation> deletedRelations, final InternalTitanTransaction tx) throws StorageException {
		//Setup
        log.debug("Saving transaction. Added {}, removed {}", addedRelations.size(), deletedRelations.size());
		final Map<TitanType,TypeSignature> signatures = new HashMap<TitanType,TypeSignature>();
		final TransactionHandle txh = tx.getTxHandle();

		final StoreMutator mutator = getStoreMutator(txh);
        final boolean acquireLocks = tx.getTxConfiguration().hasAcquireLocks();

        //1. Assign TitanVertex IDs
        assignIDs(addedRelations,tx);

        int saveAttempts = 0;

        while (true) { //Indefinite loop, broken if no exception occurs, otherwise retried or failed immediately
        try {    
            //2. Collect deleted edges
            ListMultimap<InternalTitanVertex,InternalRelation> mutations = ArrayListMultimap.create();
            if (deletedRelations !=null && !deletedRelations.isEmpty()) {
                for (InternalRelation del : deletedRelations) {
                    assert del.isRemoved();
                    for (int pos=0;pos<del.getArity();pos++) {
                        InternalTitanVertex node = del.getVertex(pos);
                        if (pos==0 || !del.isUnidirected() ) {
                            mutations.put(node,del);
                        }
                        if (pos==0 && acquireLocks && del.getType().isFunctional() &&
                                ((InternalTitanType)del.getType()).isFunctionalLocking()) {
                            Entry entry = getEntry(tx,del,node,signatures);
                            mutator.acquireEdgeLock(IDHandler.getKey(node.getID()),entry.getColumn(),entry.getValue());
                        }
                    }
                    if (acquireLocks && del.isProperty()) {
                        lockKeyedProperty((TitanProperty)del,mutator);
                    }
    
                }
            }
    
            ListMultimap<InternalTitanType,InternalRelation> simpleEdgeTypes = null;
            ListMultimap<InternalTitanType,InternalRelation> otherEdgeTypes = null;
            
            //3. Sort Added Edges
            for (InternalRelation edge : addedRelations) {
                if (edge.isRemoved()) continue;
                assert edge.isNew();
                
                TitanType et = edge.getType();
    
                //Give special treatment to edge type definitions
                if (SystemTypeManager.prepersistedSystemTypes.contains(et)) {
                    assert edge.getVertex(0) instanceof InternalTitanType;
                    InternalTitanType node = (InternalTitanType)edge.getVertex(0);
                    assert node.hasID();
                    if (node.isSimple()) {
                        if (simpleEdgeTypes==null) simpleEdgeTypes=ArrayListMultimap.create();
                        simpleEdgeTypes.put(node,edge);
                    } else {
                        if (otherEdgeTypes==null) otherEdgeTypes=ArrayListMultimap.create();
                        otherEdgeTypes.put(node,edge);					
                    }
                } else { //STANDARD TitanRelation
                    assert (edge.getArity()==1 && edge.isProperty()) || (edge.getArity()==2 && edge.isEdge());
                    for (int pos=0;pos<edge.getArity();pos++) {
                        InternalTitanVertex node = edge.getVertex(pos);
                        assert node.hasID();
                        if (pos==0 || !edge.isUnidirected()) {
                            mutations.put(node, edge);
                        }
                        if (pos==0 && acquireLocks && edge.getType().isFunctional() && !node.isNew()
                                && ((InternalTitanType)edge.getType()).isFunctionalLocking()) {
                            Entry entry = getEntry(tx,edge,node,signatures,true);
                            mutator.acquireEdgeLock(IDHandler.getKey(node.getID()),entry.getColumn(),null);
                        }
                    }
                }
                if (acquireLocks && edge.isProperty()) {
                    lockKeyedProperty((TitanProperty)edge,mutator);
                }
            }
            
            //3. Persist
            if (simpleEdgeTypes!=null) persist(simpleEdgeTypes,signatures,tx,mutator);
            if (otherEdgeTypes!=null) persist(otherEdgeTypes,signatures,tx,mutator);
            mutator.flush();
    
            //Commit saved EdgeTypes to TypeManager
            if (simpleEdgeTypes!=null) commitEdgeTypes(simpleEdgeTypes.keySet());
            if (otherEdgeTypes!=null) commitEdgeTypes(otherEdgeTypes.keySet());
    
            if (!mutations.isEmpty()) persist(mutations,signatures,tx,mutator);
            mutator.flush();
            
            //Successfully completed - break out of indefinite loop
            break;
        } catch (Throwable e) {
            if (e instanceof TemporaryStorageException) {
                if (saveAttempts<maxSaveRetryAttempts) {
                    saveAttempts++;
                    log.info("Temporary exception during commit. Attempting retry in {} ms. {}",retrySaveWaitTime,e);
                    //Wait before retry
                    if (retrySaveWaitTime>0) {
                        try {
                            Thread.sleep(retrySaveWaitTime);
                        } catch (InterruptedException r) {
                            throw new PermanentStorageException("Interrupted while waiting to retry failed persistence",e);
                        }
                    }
                } else {
                    throw new PermanentStorageException("Retried "+maxSaveRetryAttempts+" on temporary exception without success",e);
                }
            } else if (e instanceof StorageException) {
                throw (StorageException)e;
            } else {
                throw new PermanentStorageException("Unidentified exception occurred during persistence",e);
            }
        }
        }
            
	}
	
	private void commitEdgeTypes(Iterable<InternalTitanType> edgeTypes) {
		for (InternalTitanType et : edgeTypes) etManager.committed(et);
	}
	

	private<N extends InternalTitanVertex> void persist(ListMultimap<N,InternalRelation> mutatedEdges, Map<TitanType,TypeSignature> signatures,
			InternalTitanTransaction tx, StoreMutator mutator) throws StorageException {
		assert mutatedEdges!=null && !mutatedEdges.isEmpty();

		Collection<N> vertices = mutatedEdges.keySet();
//		if (sortNodes) {
//			List<N> sortedvertices = new ArrayList<N>(vertices);
//			Collections.sort(sortedvertices, new Comparator<N>(){
//
//				@Override
//				public int compare(N o1, N o2) {
//					assert o1.getID()!=o2.getID();
//					if (o1.getID()<o2.getID()) return -1;
//					else return 1;
//				}
//
//			});
//			vertices=sortedvertices;
//		}

		for (N node : vertices) {
			List<InternalRelation> edges = mutatedEdges.get(node);
			List<Entry> additions = new ArrayList<Entry>(edges.size());
            List<ByteBuffer> deletions = new ArrayList<ByteBuffer>(Math.max(10,edges.size()/10));
            List<TitanProperty> properties = new ArrayList<TitanProperty>();
			for (InternalRelation edge : edges) {
                if (edge.isRemoved()) {
                    if (edge.isProperty()) {
                        deleteIndexEntry((TitanProperty)edge, mutator);
                    }
                    deletions.add(getEntry(tx,edge,node,signatures,true).getColumn());
                } else {
                    assert edge.isNew();
                    if (edge.isProperty()) properties.add((TitanProperty)edge);
                    additions.add(getEntry(tx,edge,node,signatures));
                }

			}
			mutator.mutateEdges(IDHandler.getKey(node.getID()), additions, deletions);
            //Persist property index for retrieval
            for (TitanProperty prop : properties) {
                addIndexEntry(prop, mutator);
            }
        }

	}

	private Entry getEntry(InternalTitanTransaction tx,InternalRelation edge, InternalTitanVertex perspective, Map<TitanType,TypeSignature> signatures) {
		return getEntry(tx,edge,perspective,signatures,false);
	}

	private Entry getEntry(InternalTitanTransaction tx,InternalRelation edge, InternalTitanVertex perspective, Map<TitanType,TypeSignature> signatures, boolean columnOnly) {
		TitanType et = edge.getType();
        long etid = et.getID();

        int dirID;
        if (edge.isProperty()) {
            dirID = 0;
        } else if (edge.getVertex(0).equals(perspective)) {
            //Out TitanRelation
            assert edge.isDirected() || edge.isUnidirected();
            dirID = 2;
        } else {
            //In TitanRelation
            assert !edge.isUnidirected() && edge.getVertex(1).equals(perspective);
            dirID = 3;
        }
		
        int etIDLength = IDHandler.edgeTypeLength(etid,idManager);
        
		ByteBuffer column=null,value=null;
		if (et.isSimple()) {
			if (et.isFunctional()) {
                column = ByteBuffer.allocate(etIDLength);
                IDHandler.writeEdgeType(column,etid,dirID,idManager);
			} else {
                column = ByteBuffer.allocate(etIDLength + VariableLong.positiveLength(edge.getID()));
                IDHandler.writeEdgeType(column,etid,dirID,idManager);
                VariableLong.writePositive(column,edge.getID());
			}
            column.flip();
			if (!columnOnly) {
                if (edge.isEdge()) {
                    long nodeIDDiff = ((TitanEdge) edge).getOtherVertex(perspective).getID() - perspective.getID();
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
                    writeAttribute(out,(TitanProperty)edge);
                    if (et.isFunctional()) {
                        VariableLong.writePositive(out,edge.getID());
                    }
                    value = out.getByteBuffer();
                }
            }
        } else {
			TypeSignature ets = getSignature(tx,et,signatures);
			
			InternalRelation[] keys = new InternalRelation[ets.keyLength()],
						   values = new InternalRelation[ets.valueLength()];
			List<InternalRelation> rest = new ArrayList<InternalRelation>();
			ets.sort(edge.getRelations(SimpleAtomicQuery.queryAll(edge), false),keys,values,rest);
			
			DataOutput out = serializer.getDataOutput(defaultOutputCapacity, true);
            IDHandler.writeEdgeType(out,etid,dirID,idManager);

			for (int i=0;i<keys.length;i++) writeInlineEdge(out, keys[i], ets.getKeyType(i));
			
			if (!et.isFunctional()) {
				VariableLong.writePositive(out,edge.getID());
			}
			column = out.getByteBuffer();
			
			if (!columnOnly) {
                out = serializer.getDataOutput(defaultOutputCapacity, true);

                if (edge.isEdge()) {
                    long nodeIDDiff = ((TitanEdge) edge).getOtherVertex(perspective).getID() - perspective.getID();
                    VariableLong.write(out,nodeIDDiff);
                } else {
                    assert edge.isProperty();
                    writeAttribute(out, (TitanProperty) edge);
                }

                if (et.isFunctional()) {
                    assert edge.isEdge();
                    VariableLong.writePositive(out, edge.getID());
                }
                for (int i=0;i<values.length;i++) writeInlineEdge(out, values[i], ets.getValueType(i));
                for (InternalRelation v: rest) writeInlineEdge(out, v);
                value = out.getByteBuffer();
            }
		}
		return new Entry(column,value);
	}


    private static void writeAttribute(DataOutput out, TitanProperty property) {
        Object attribute = property.getAttribute();
        TitanKey key = (TitanKey)property.getType();
        assert attribute!=null;
        assert key.getDataType().isInstance(attribute);
        if (hasGenericDataType(key)) {
            out.writeClassAndObject(attribute);
        } else {
            out.writeObjectNotNull(attribute);
        }
    }


    private void writeInlineEdge(DataOutput out, InternalRelation edge) {
		assert edge!=null;
        writeInlineEdge(out, edge, edge.getType(), true);
	}
	
	private void writeInlineEdge(DataOutput out, InternalRelation edge, TitanType titanType) {
        writeInlineEdge(out, edge, titanType, false);
	}
	
	private void writeInlineEdge(DataOutput out, InternalRelation edge, TitanType type, boolean writeEdgeType) {
		assert type.isSimple();
        assert writeEdgeType || type.isEdgeLabel() || 
                (type.isPropertyKey() && !hasGenericDataType((TitanKey) type) );

		if (edge==null) {
			assert !writeEdgeType;
			if (type.isPropertyKey()) {
				out.writeObject(null);
			} else {
				assert type.isEdgeLabel();
                VariableLong.writePositive(out, 0);
			}
		} else {
			if (writeEdgeType) {
                IDHandler.writeInlineEdgeType(out, type.getID(), idManager);
			}
			if (edge.isProperty()) {
                Object attribute = ((TitanProperty)edge).getAttribute();
                if (hasGenericDataType((TitanKey) type))
                    out.writeClassAndObject(attribute);
				else out.writeObject(attribute);
			} else {
				assert edge.isUnidirected() && edge.isEdge();
                VariableLong.writePositive(out, edge.getVertex(1).getID());
			}
		}
	}
	
	private static TypeSignature getSignature(InternalTitanTransaction tx,TitanType et, Map<TitanType,TypeSignature> signatures) {
		TypeSignature ets = signatures.get(et);
		if (ets==null) {
			ets = new TypeSignature(et,tx);
			signatures.put(et, ets);
		}
		return ets;
	}
    
    private static final boolean hasGenericDataType(TitanKey key) {
        return key.getDataType().equals(Object.class);
    }

    // ################### PROPERTY INDEX HANDLING #########################

	
    private void lockKeyedProperty(TitanProperty prop, StoreMutator mutator) throws StorageException {
        TitanKey pt = prop.getPropertyKey();
        assert pt.isSimple();
        if (pt.hasIndex() && pt.isUnique()) {
            if (prop.isNew()) {
                mutator.acquireIndexLock(getIndexKey(prop.getAttribute()), getKeyedIndexColumn(pt), null);
            } else {
                assert prop.isRemoved();
                mutator.acquireIndexLock(getIndexKey(prop.getAttribute()), getKeyedIndexColumn(pt), getIndexValue(prop));
            }
        }
    }
    
    
	private void deleteIndexEntry(TitanProperty prop, StoreMutator mutator) throws StorageException  {
		TitanKey pt = prop.getPropertyKey();
		assert pt.isSimple();
		if (pt.hasIndex()) {
            if (pt.isUnique()) {
                mutator.mutateIndex(getIndexKey(prop.getAttribute()), null,
                        Lists.newArrayList(getKeyedIndexColumn(prop.getPropertyKey())));
            } else {
                mutator.mutateIndex(getIndexKey(prop.getAttribute()), null,
                        Lists.newArrayList(getIndexColumn(prop.getPropertyKey(), prop.getID())));
            }

		}
	}
	
	private void addIndexEntry(TitanProperty prop, StoreMutator mutator) throws StorageException  {
		TitanKey pt = prop.getPropertyKey();
		assert pt.isSimple();
		if (pt.hasIndex()) {
            if (pt.isUnique()) {
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
	
	private ByteBuffer getIndexValue(TitanProperty prop) {
		assert prop.getType().isSimple();
        return VariableLong.positiveByteBuffer(prop.getVertex().getID());
	}
	
	private ByteBuffer getKeyedIndexColumn(TitanKey type) {
		assert type.isUnique();
        return VariableLong.positiveByteBuffer(type.getID());
	}

	private ByteBuffer getIndexColumn(TitanKey type, long propertyID) {
		assert !type.isUnique();
		return VariableLong.positiveByteBuffer(new long[]{ type.getID(),propertyID });
	}
	
}
