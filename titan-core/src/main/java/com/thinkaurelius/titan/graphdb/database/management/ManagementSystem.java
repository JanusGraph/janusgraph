package com.thinkaurelius.titan.graphdb.database.management;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.configuration.*;
import com.thinkaurelius.titan.diskstorage.configuration.backend.KCVSConfiguration;
import com.thinkaurelius.titan.diskstorage.log.Log;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

import com.thinkaurelius.titan.graphdb.database.IndexSerializer;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.internal.ElementCategory;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.internal.TitanSchemaCategory;
import com.thinkaurelius.titan.graphdb.internal.Token;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.*;
import com.thinkaurelius.titan.graphdb.types.indextype.IndexTypeWrapper;
import com.thinkaurelius.titan.graphdb.types.system.BaseKey;
import com.thinkaurelius.titan.graphdb.types.system.BaseLabel;
import com.thinkaurelius.titan.graphdb.types.vertices.TitanKeyVertex;
import com.thinkaurelius.titan.graphdb.types.vertices.TitanSchemaVertex;
import com.thinkaurelius.titan.graphdb.types.vertices.TitanTypeVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Element;
import org.apache.commons.lang.StringUtils;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ManagementSystem implements TitanManagement {

    private static final String GRAPH_INDEX_NAME_PREFIX = "graphindex";
    private static final String TYPE_INDEX_NAME_PREFIX = "typeindex";

    private final StandardTitanGraph graph;
    private final Log sysLog;
    private final ManagementLogger mgmtLogger;

    private final KCVSConfiguration baseConfig;
    private final TransactionalConfiguration transactionalConfig;
    private final ModifiableConfiguration modifyConfig;
    private final UserModifiableConfiguration userConfig;

    private final StandardTitanTx transaction;

    private final Set<TitanSchemaVertex> updatedTypes;
    private final Set<Callable<Boolean>> updatedTypeTriggers;

    private boolean graphShutdownRequired;
    private boolean isOpen;

    public ManagementSystem(StandardTitanGraph graph, KCVSConfiguration config, Log sysLog, ManagementLogger mgmtLogger) {
        Preconditions.checkArgument(config!=null && graph!=null && sysLog!=null && mgmtLogger!=null);
        this.graph = graph;
        this.baseConfig = config;
        this.sysLog = sysLog;
        this.mgmtLogger = mgmtLogger;
        this.transactionalConfig = new TransactionalConfiguration(baseConfig);
        this.modifyConfig = new ModifiableConfiguration(ROOT_NS,
                transactionalConfig, BasicConfiguration.Restriction.GLOBAL);
        this.userConfig = new UserModifiableConfiguration(modifyConfig,configVerifier);

        this.updatedTypes = new HashSet<TitanSchemaVertex>();
        this.updatedTypeTriggers = new HashSet<Callable<Boolean>>();
        this.graphShutdownRequired = false;

        this.transaction = (StandardTitanTx) graph.newTransaction();

        this.isOpen = true;
    }

    private final UserModifiableConfiguration.ConfigVerifier configVerifier = new UserModifiableConfiguration.ConfigVerifier() {
        @Override
        public void verifyModification(ConfigOption option) {
            Preconditions.checkArgument(option.getType()!= ConfigOption.Type.FIXED,"Cannot change the fixed configuration option: %s",option);
            Preconditions.checkArgument(option.getType()!= ConfigOption.Type.LOCAL,"Cannot change the local configuration option: %s",option);
            if (option.getType() == ConfigOption.Type.GLOBAL_OFFLINE) {
                //Verify that there no other open Titan graph instance and no open transactions
                Set<String> openInstances = getOpenInstances();
                assert openInstances.size()>0;
                Preconditions.checkArgument(openInstances.size()<2,"Cannot change offline config option [%s] since multiple instances are currently open: %s",option,openInstances);
                Preconditions.checkArgument(openInstances.contains(graph.getConfiguration().getUniqueGraphId()),
                        "Only one open instance ("
                        + openInstances.iterator().next() + "), but it's not the current one ("
                        + graph.getConfiguration().getUniqueGraphId() + ")");
                //Indicate that this graph must be closed
                graphShutdownRequired = true;
            }
        }
    };

    private Set<String> getOpenInstances() {
        return modifyConfig.getContainedNamespaces(REGISTRATION_NS);
    }

    private void ensureOpen() {
        Preconditions.checkState(isOpen,"This management system instance has been closed");
    }

    @Override
    public synchronized void commit() {
        ensureOpen();
        //Commit config changes
        if (transactionalConfig.hasMutations()) {
            DataOutput out = graph.getDataSerializer().getDataOutput(128);
            out.writeObjectNotNull(MgmtLogType.CONFIG_MUTATION);
            transactionalConfig.logMutations(out);
            sysLog.add(out.getStaticBuffer());
        }
        transactionalConfig.commit();

        //Commit underlying transaction
        transaction.commit();

        //Communicate schema changes
        if (!updatedTypes.isEmpty()) {
            mgmtLogger.sendCacheEviction(updatedTypes,updatedTypeTriggers,getOpenInstances());
        }

        if (graphShutdownRequired) graph.shutdown();
        close();
    }

    @Override
    public synchronized void rollback() {
        ensureOpen();
        transactionalConfig.rollback();
        transaction.rollback();
        close();
    }

    @Override
    public boolean isOpen() {
        return isOpen;
    }

    private void close() {
        isOpen = false;
    }

    // ###### INDEXING SYSTEM #####################

    /* --------------
    Type Indexes
     --------------- */

    public TitanSchemaElement getSchemaElement(long id) {
        TitanVertex v = transaction.getVertex(id);
        if (v==null) return null;
        if (v instanceof TitanType) {
            if (((InternalType)v).getBaseType()==null) return (TitanType)v;
            return new TitanTypeIndexWrapper((InternalType)v);
        }
        if (v instanceof TitanSchemaVertex) {
            TitanSchemaVertex sv = (TitanSchemaVertex)v;
            if (sv.getDefinition().containsKey(TypeDefinitionCategory.INTERNAL_INDEX)) {
                return new TitanGraphIndexWrapper(sv.asIndexType());
            }
        }
        throw new IllegalArgumentException("Not a valid schema element vertex: "+id);
    }

    @Override
    public TitanTypeIndex createTypeIndex(TitanLabel label, String name, Direction direction, TitanType... sortKeys) {
        return createTypeIndex(label,name,direction,Order.ASC,sortKeys);
    }

    @Override
    public TitanTypeIndex createTypeIndex(TitanLabel label, String name, Direction direction, Order sortOrder, TitanType... sortKeys) {
        return createTypeIndexInternal(label, name, direction, sortOrder, sortKeys);
    }

    @Override
    public TitanTypeIndex createTypeIndex(TitanKey key, String name, TitanType... sortKeys) {
        return createTypeIndex(key,name,Order.ASC,sortKeys);
    }

    @Override
    public TitanTypeIndex createTypeIndex(TitanKey key, String name, Order sortOrder, TitanType... sortKeys) {
        return createTypeIndexInternal(key,name,Direction.OUT,sortOrder,sortKeys);
    }

    private TitanTypeIndex createTypeIndexInternal(TitanType type, String name, Direction direction, Order sortOrder, TitanType... sortKeys) {
        Preconditions.checkArgument(type!=null && direction!=null && sortOrder!=null && sortKeys!=null);
        Preconditions.checkArgument(StringUtils.isNotBlank(name),"Name cannot be blank: %s",name);
        Token.verifyName(name);
        Preconditions.checkArgument(sortKeys.length>0,"Need to specify sort keys");
        for (TitanType key : sortKeys) Preconditions.checkArgument(key!=null,"Keys cannot be null");
        Preconditions.checkArgument(type.isNew(),"Can only install indexes on new types (current limitation)");

        String composedName = Token.getSeparatedName(TYPE_INDEX_NAME_PREFIX, type.getName(), name);
        StandardTypeMaker maker;
        if (type.isEdgeLabel()) {
            StandardLabelMaker lm = (StandardLabelMaker)transaction.makeLabel(composedName);
            lm.unidirected(direction);
            maker = lm;
        } else {
            assert type.isPropertyKey();
            assert direction==Direction.OUT;
            StandardKeyMaker lm = (StandardKeyMaker)transaction.makeKey(composedName);
            lm.dataType(((TitanKey)type).getDataType());
            maker = lm;
        }
        maker.hidden();
        maker.multiplicity(Multiplicity.MULTI);
        maker.sortKey(sortKeys);
        maker.sortOrder(sortOrder);

        //Compose signature
        long[] typeSig = ((InternalType)type).getSignature();
        Set<TitanType> signature = Sets.newHashSet();
        for (long typeId : typeSig) signature.add(transaction.getExistingType(typeId));
        for (TitanType sortType : sortKeys) signature.remove(sortType);
        if (!signature.isEmpty()) {
            TitanType[] sig = signature.toArray(new TitanType[signature.size()]);
            maker.signature(sig);
        }
        TitanType typeIndex = maker.make();
        addSchemaEdge(type, typeIndex, TypeDefinitionCategory.RELATIONTYPE_INDEX, null);
        return new TitanTypeIndexWrapper((InternalType)typeIndex);
    }

    private TitanEdge addSchemaEdge(TitanVertex out, TitanVertex in, TypeDefinitionCategory def, Object modifier) {
        assert def.isEdge();
        TitanEdge edge = transaction.addEdge(out,in, BaseLabel.TypeDefinitionEdge);
        TypeDefinitionDescription desc = new TypeDefinitionDescription(def,modifier);
        edge.setProperty(BaseKey.TypeDefinitionDesc,desc);
        return edge;
    }

    @Override
    public boolean containsTypeIndex(TitanType type, String name) {
        return getTypeIndex(type, name)!=null;
    }

    @Override
    public TitanTypeIndex getTypeIndex(TitanType type, String name) {
        Preconditions.checkArgument(type!=null);
        Preconditions.checkArgument(StringUtils.isNotBlank(name));
        String composedName = Token.getSeparatedName(TYPE_INDEX_NAME_PREFIX, type.getName(), name);

        TitanVertex v = Iterables.getOnlyElement(transaction.getVertices(BaseKey.TypeName,composedName),null);
        if (v==null) return null;
        assert v instanceof InternalType;
        return new TitanTypeIndexWrapper((InternalType)v);
    }

    @Override
    public Iterable<TitanTypeIndex> getTypeIndexes(TitanType type) {
        assert type instanceof InternalType;
        return Iterables.transform(((InternalType) type).getRelationIndexes(),new Function<InternalType, TitanTypeIndex>() {
            @Nullable
            @Override
            public TitanTypeIndex apply(@Nullable InternalType internalType) {
                return new TitanTypeIndexWrapper(internalType);
            }
        });
    }

    /* --------------
    Graph Indexes
     --------------- */

    public static IndexType getGraphIndexDirect(String name, StandardTitanTx transaction) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name));
        String composedName = composeIndexName(name);

        TitanVertex v = Iterables.getOnlyElement(transaction.getVertices(BaseKey.TypeName,composedName),null);
        if (v==null) return null;
        assert v instanceof TitanSchemaVertex;
        return ((TitanSchemaVertex)v).asIndexType();
    }

    private static final String composeIndexName(String indexName) {
        return Token.getSeparatedName(GRAPH_INDEX_NAME_PREFIX, indexName);
    }

    @Override
    public boolean containsGraphIndex(String name) {
        return getGraphIndex(name)!=null;
    }

    @Override
    public TitanGraphIndex getGraphIndex(String name) {
        IndexType index = getGraphIndexDirect(name, transaction);
        return index==null?null:new TitanGraphIndexWrapper(index);
    }

    @Override
    public Iterable<TitanGraphIndex> getGraphIndexes(final Class<? extends Element> elementType) {
        return Iterables.transform(Iterables.filter(Iterables.transform(
                transaction.getVertices(BaseKey.TypeCategory, TitanSchemaCategory.INDEX),
                new Function<TitanVertex, IndexType>() {
                    @Nullable
                    @Override
                    public IndexType apply(@Nullable TitanVertex titanVertex) {
                        assert titanVertex instanceof TitanSchemaVertex;
                        return ((TitanSchemaVertex) titanVertex).asIndexType();
                    }
                }), new Predicate<IndexType>() {
            @Override
            public boolean apply(@Nullable IndexType indexType) {
                return indexType.getElement().subsumedBy(elementType);
            }
        }), new Function<IndexType, TitanGraphIndex>() {
            @Nullable
            @Override
            public TitanGraphIndex apply(@Nullable IndexType indexType) {
                return new TitanGraphIndexWrapper(indexType);
            }
        });
    }

    @Override
    public TitanGraphIndex createExternalIndex(String indexName, Class<? extends Element> elementType, String backingIndex) {
        Preconditions.checkArgument(graph.getIndexSerializer().containsIndex(backingIndex),"Unknown external index backend: %s",backingIndex);
        Preconditions.checkArgument(StringUtils.isNotBlank(indexName));
        Preconditions.checkArgument(getGraphIndex(indexName)==null,"An index with name '%s' has already been defined",indexName);
        ElementCategory elementCategory = ElementCategory.getByClazz(elementType);
        TypeDefinitionMap def = new TypeDefinitionMap();
        def.setValue(TypeDefinitionCategory.INTERNAL_INDEX,false);
        def.setValue(TypeDefinitionCategory.ELEMENT_CATEGORY,elementCategory);
        def.setValue(TypeDefinitionCategory.BACKING_INDEX,backingIndex);
        def.setValue(TypeDefinitionCategory.INDEXSTORE_NAME,indexName);
        def.setValue(TypeDefinitionCategory.INDEX_CARDINALITY,Cardinality.LIST);
        def.setValue(TypeDefinitionCategory.STATUS,SchemaStatus.ENABLED);
        TitanSchemaVertex v = transaction.makeSchemaVertex(TitanSchemaCategory.INDEX,composeIndexName(indexName),def);
        return new TitanGraphIndexWrapper(v.asIndexType());
    }

    @Override
    public void addIndexKey(final TitanGraphIndex index, final TitanKey key, Parameter... parameters) {
        Preconditions.checkArgument(index!=null && key!=null && index instanceof TitanGraphIndexWrapper
                && !(key instanceof BaseKey),"Need to provide valid index and key");
        if (parameters==null) parameters=new Parameter[0];
        IndexType indexType = ((TitanGraphIndexWrapper)index).getBaseIndex();
        Preconditions.checkArgument(indexType instanceof ExternalIndexType,"Can only add keys to an external index, not %s",index.getName());
        Preconditions.checkArgument(indexType instanceof IndexTypeWrapper && key instanceof TitanSchemaVertex
            && ((IndexTypeWrapper)indexType).getSchemaBase() instanceof TitanSchemaVertex);
        Preconditions.checkArgument(key.isNew(),"Can only index new keys (current limitation)");
        TitanSchemaVertex indexVertex = (TitanSchemaVertex)((IndexTypeWrapper)indexType).getSchemaBase();

        for (IndexField field : indexType.getFieldKeys())
            Preconditions.checkArgument(!field.getFieldKey().equals(key),"Key [%s] has already been added to index %s",key.getName(),index.getName());

        Parameter[] extendedParas = new Parameter[parameters.length+1];
        System.arraycopy(parameters,0,extendedParas,0,parameters.length);
        extendedParas[parameters.length]=ParameterType.STATUS.getParameter(SchemaStatus.ENABLED);
        addSchemaEdge(indexVertex, key, TypeDefinitionCategory.INDEX_FIELD, extendedParas);
        indexType.resetCache();
        try {
            IndexSerializer.register((ExternalIndexType) indexType,key,transaction.getTxHandle());
        } catch (StorageException e) {
            throw new TitanException("Could not register new index field with index backend",e);
        }

        //TODO: it is possible to have a race condition here if two threads add the same field at the same time
    }

    private class IndexRegistration {
        private final ExternalIndexType index;
        private final TitanKey key;

        private IndexRegistration(ExternalIndexType index, TitanKey key) {
            this.index = index;
            this.key = key;
        }

        private void register() throws StorageException {

        }
    }

    @Override
    public TitanGraphIndex createInternalIndex(String indexName, Class<? extends Element> elementType, TitanKey... keys) {
        return createInternalIndex(indexName, elementType, false, keys);
    }

    @Override
    public TitanGraphIndex createInternalIndex(String indexName, Class<? extends Element> elementType, boolean unique, TitanKey... keys) {
        Preconditions.checkArgument(StringUtils.isNotBlank(indexName));
        Preconditions.checkArgument(getGraphIndex(indexName)==null,"An index with name '%s' has already been defined",indexName);
        ElementCategory elementCategory = ElementCategory.getByClazz(elementType);
        Preconditions.checkArgument(keys!=null && keys.length>0,"Need to provide keys to index");
        boolean allSingleKeys = true;
        boolean oneNewKey = false;
        for (TitanKey key : keys) {
            Preconditions.checkArgument(key!=null && key instanceof TitanKeyVertex,"Need to provide valid keys: %s",key);
            if (key.getCardinality()!=Cardinality.SINGLE) allSingleKeys=false;
            if (key.isNew()) oneNewKey = true;
        }
        Preconditions.checkArgument(oneNewKey,"At least one of the indexed keys must be new (current limitation)");

        Cardinality indexCardinality;
        if (unique) indexCardinality=Cardinality.SINGLE;
        else indexCardinality=(allSingleKeys?Cardinality.SET:Cardinality.LIST);

        TypeDefinitionMap def = new TypeDefinitionMap();
        def.setValue(TypeDefinitionCategory.INTERNAL_INDEX,true);
        def.setValue(TypeDefinitionCategory.ELEMENT_CATEGORY,elementCategory);
        def.setValue(TypeDefinitionCategory.BACKING_INDEX,Token.INTERNAL_INDEX_NAME);
        def.setValue(TypeDefinitionCategory.INDEXSTORE_NAME,indexName);
        def.setValue(TypeDefinitionCategory.INDEX_CARDINALITY,indexCardinality);
        def.setValue(TypeDefinitionCategory.STATUS,SchemaStatus.ENABLED);
        TitanSchemaVertex indexVertex = transaction.makeSchemaVertex(TitanSchemaCategory.INDEX,composeIndexName(indexName),def);
        for (int i = 0; i <keys.length; i++) {
            Parameter[] paras = {ParameterType.INDEX_POSITION.getParameter(i)};
            addSchemaEdge(indexVertex,keys[i],TypeDefinitionCategory.INDEX_FIELD,paras);
        }

        return new TitanGraphIndexWrapper(indexVertex.asIndexType());

    }


    /* --------------
    Schema Consistency
     --------------- */

    /**
     * Retrieves the consistency level for a schema element (types and internal indexes)
     *
     * @param element
     * @return
     */
    @Override
    public ConsistencyModifier getConsistency(TitanSchemaElement element) {
        Preconditions.checkArgument(element!=null);
        if (element instanceof TitanType) return ((InternalType)element).getConsistencyModifier();
        else if (element instanceof TitanGraphIndex) {
            IndexType index = ((TitanGraphIndexWrapper)element).getBaseIndex();
            if (index.isExternalIndex()) return ConsistencyModifier.DEFAULT;
            return ((InternalIndexType)index).getConsistencyModifier();
        } else return ConsistencyModifier.DEFAULT;
    }

    /**
     * Sets the consistency level for those schema elements that support it (types and internal indexes)
     * </p>
     * Note, that it is possible to have a race condition here if two threads simultaneously try to change the
     * consistency level. However, this is resolved when the consistency level is being read by taking the
     * first one and deleting all existing attached consistency levels upon modification.
     *
     * @param element
     * @param consistency
     */
    @Override
    public void setConsistency(TitanSchemaElement element, ConsistencyModifier consistency) {
        Preconditions.checkArgument(consistency!=null);
        if (getConsistency(element)==consistency) return; //Already got the right consistency
        TitanSchemaVertex vertex;
        if (element instanceof TitanType) vertex = (TitanTypeVertex)element;
        else if (element instanceof TitanGraphIndex) {
            IndexType index = ((TitanGraphIndexWrapper)element).getBaseIndex();
            if (index.isExternalIndex()) throw new IllegalArgumentException("Cannot change consistency on an external index: " + element);
            assert index instanceof IndexTypeWrapper;
            SchemaSource base = ((IndexTypeWrapper)index).getSchemaBase();
            assert base instanceof TitanSchemaVertex;
            vertex = (TitanSchemaVertex)base;
        } else throw new IllegalArgumentException("Cannot change consistency of schema element: "+element);
        for (TitanEdge edge : vertex.getEdges(TypeDefinitionCategory.CONSISTENCY_MODIFIER,Direction.OUT)) {
            edge.remove();
            edge.getVertex(Direction.IN).remove();
        }

        TypeDefinitionMap def = new TypeDefinitionMap();
        def.setValue(TypeDefinitionCategory.CONSISTENCY_LEVEL,consistency);
        TitanSchemaVertex cVertex = transaction.makeSchemaVertex(TitanSchemaCategory.MODIFIER,null,def);
        addSchemaEdge(vertex,cVertex,TypeDefinitionCategory.CONSISTENCY_MODIFIER,null);
        updatedTypes.add(vertex);
    }

    // ###### TRANSACTION PROXY #########

    @Override
    public boolean containsType(String name) {
        return transaction.containsType(name);
    }

    @Override
    public TitanType getType(String name) {
        return transaction.getType(name);
    }

    @Override
    public TitanKey getPropertyKey(String name) {
        return transaction.getPropertyKey(name);
    }

    @Override
    public TitanLabel getEdgeLabel(String name) {
        return transaction.getEdgeLabel(name);
    }

    @Override
    public KeyMaker makeKey(String name) {
        return transaction.makeKey(name);
    }

    @Override
    public LabelMaker makeLabel(String name) {
        return transaction.makeLabel(name);
    }

    @Override
    public <T extends TitanType> Iterable<T> getTypes(Class<T> clazz) {
        Preconditions.checkNotNull(clazz);
        Iterable<? extends TitanVertex> types = null;
        if (TitanKey.class.equals(clazz)) {
            types = transaction.getVertices(BaseKey.TypeCategory, TitanSchemaCategory.KEY);
        } else if (TitanLabel.class.equals(clazz)) {
            types = transaction.getVertices(BaseKey.TypeCategory, TitanSchemaCategory.LABEL);
        } else if (TitanType.class.equals(clazz)) {
            types = Iterables.concat(getTypes(TitanLabel.class),getTypes(TitanKey.class));
        } else throw new IllegalArgumentException("Unknown type class: " + clazz);
        return Iterables.filter(types, clazz);
    }

    // ###### USERMODIFIABLECONFIGURATION PROXY #########

    @Override
    public synchronized String get(String path) {
        ensureOpen();
        return userConfig.get(path);
    }

    @Override
    public synchronized TitanConfiguration set(String path, Object value) {
        ensureOpen();
        return userConfig.set(path,value);
    }
}
