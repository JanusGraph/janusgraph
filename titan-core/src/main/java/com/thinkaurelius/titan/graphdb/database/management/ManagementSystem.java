package com.thinkaurelius.titan.graphdb.database.management;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.*;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.configuration.*;
import com.thinkaurelius.titan.diskstorage.configuration.backend.KCVSConfiguration;
import com.thinkaurelius.titan.diskstorage.log.Log;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

import com.thinkaurelius.titan.graphdb.database.IndexSerializer;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.internal.ElementCategory;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.internal.TitanSchemaCategory;
import com.thinkaurelius.titan.graphdb.internal.Token;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.*;
import com.thinkaurelius.titan.graphdb.types.indextype.IndexTypeWrapper;
import com.thinkaurelius.titan.graphdb.types.system.BaseKey;
import com.thinkaurelius.titan.graphdb.types.system.BaseLabel;
import com.thinkaurelius.titan.graphdb.types.vertices.PropertyKeyVertex;
import com.thinkaurelius.titan.graphdb.types.vertices.RelationTypeVertex;
import com.thinkaurelius.titan.graphdb.types.vertices.TitanSchemaVertex;
import static com.thinkaurelius.titan.graphdb.database.management.RelationTypeIndexWrapper.RELATION_INDEX_SEPARATOR;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Element;
import org.apache.commons.lang.StringUtils;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ManagementSystem implements TitanManagement {

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

    public StandardTitanTx getWrappedTx() {
        return transaction;
    }

    private TitanEdge addSchemaEdge(TitanVertex out, TitanVertex in, TypeDefinitionCategory def, Object modifier) {
        assert def.isEdge();
        TitanEdge edge = transaction.addEdge(out,in, BaseLabel.SchemaDefinitionEdge);
        TypeDefinitionDescription desc = new TypeDefinitionDescription(def,modifier);
        edge.setProperty(BaseKey.SchemaDefinitionDesc,desc);
        return edge;
    }

    // ###### INDEXING SYSTEM #####################

    /* --------------
    Type Indexes
     --------------- */

    public TitanSchemaElement getSchemaElement(long id) {
        TitanVertex v = transaction.getVertex(id);
        if (v==null) return null;
        if (v instanceof RelationType) {
            if (((InternalRelationType)v).getBaseType()==null) return (RelationType)v;
            return new RelationTypeIndexWrapper((InternalRelationType)v);
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
    public RelationTypeIndex createEdgeIndex(EdgeLabel label, String name, Direction direction, RelationType... sortKeys) {
        return createEdgeIndex(label, name, direction, Order.ASC, sortKeys);
    }

    @Override
    public RelationTypeIndex createEdgeIndex(EdgeLabel label, String name, Direction direction, Order sortOrder, RelationType... sortKeys) {
        return createRelationTypeIndex(label, name, direction, sortOrder, sortKeys);
    }

    @Override
    public RelationTypeIndex createPropertyIndex(PropertyKey key, String name, RelationType... sortKeys) {
        return createPropertyIndex(key, name, Order.ASC, sortKeys);
    }

    @Override
    public RelationTypeIndex createPropertyIndex(PropertyKey key, String name, Order sortOrder, RelationType... sortKeys) {
        return createRelationTypeIndex(key, name, Direction.OUT, sortOrder, sortKeys);
    }

    private RelationTypeIndex createRelationTypeIndex(RelationType type, String name, Direction direction, Order sortOrder, RelationType... sortKeys) {
        Preconditions.checkArgument(type!=null && direction!=null && sortOrder!=null && sortKeys!=null);
        Preconditions.checkArgument(StringUtils.isNotBlank(name),"Name cannot be blank: %s",name);
        Token.verifyName(name);
        Preconditions.checkArgument(sortKeys.length>0,"Need to specify sort keys");
        for (RelationType key : sortKeys) Preconditions.checkArgument(key!=null,"Keys cannot be null");
        Preconditions.checkArgument(!(type instanceof EdgeLabel) || !((EdgeLabel)type).isUnidirected() || direction==Direction.OUT,
                "Can only index uni-directed labels in the out-direction: %s",type);
        Preconditions.checkArgument(!((InternalRelationType)type).getMultiplicity().isConstrained(direction),
                "The relation type [%s] has a multiplicity or cardinality constraint in direction [%s] and can therefore not be indexed",type,direction);

        String composedName = composeRelationTypeIndexName(type,name);
        StandardRelationTypeMaker maker;
        if (type.isEdgeLabel()) {
            StandardEdgeLabelMaker lm = (StandardEdgeLabelMaker)transaction.makeEdgeLabel(composedName);
            lm.unidirected(direction);
            maker = lm;
        } else {
            assert type.isPropertyKey();
            assert direction==Direction.OUT;
            StandardPropertyKeyMaker lm = (StandardPropertyKeyMaker)transaction.makePropertyKey(composedName);
            lm.dataType(((PropertyKey)type).getDataType());
            maker = lm;
        }
        maker.status(type.isNew()?SchemaStatus.ENABLED:SchemaStatus.INSTALLED);
        maker.hidden();
        maker.multiplicity(Multiplicity.MULTI);
        maker.sortKey(sortKeys);
        maker.sortOrder(sortOrder);

        //Compose signature
        long[] typeSig = ((InternalRelationType)type).getSignature();
        Set<RelationType> signature = Sets.newHashSet();
        for (long typeId : typeSig) signature.add(transaction.getExistingRelationType(typeId));
        for (RelationType sortType : sortKeys) signature.remove(sortType);
        if (!signature.isEmpty()) {
            RelationType[] sig = signature.toArray(new RelationType[signature.size()]);
            maker.signature(sig);
        }
        RelationType typeIndex = maker.make();
        addSchemaEdge(type, typeIndex, TypeDefinitionCategory.RELATIONTYPE_INDEX, null);
        RelationTypeIndexWrapper index = new RelationTypeIndexWrapper((InternalRelationType)typeIndex);
        if (!type.isNew()) updateIndex(index,SchemaAction.REGISTER_INDEX);
        return index;
    }

    private static String composeRelationTypeIndexName(RelationType type, String name) {
        return type.getName()+RELATION_INDEX_SEPARATOR+name;
    }

    @Override
    public boolean containsRelationIndex(RelationType type, String name) {
        return getRelationIndex(type, name)!=null;
    }

    @Override
    public RelationTypeIndex getRelationIndex(RelationType type, String name) {
        Preconditions.checkArgument(type!=null);
        Preconditions.checkArgument(StringUtils.isNotBlank(name));
        String composedName = composeRelationTypeIndexName(type, name);

        //Don't use SchemaCache to make code more compact and since we don't need the extra performance here
        TitanVertex v = Iterables.getOnlyElement(transaction.getVertices(BaseKey.SchemaName,TitanSchemaCategory.getRelationTypeName(composedName)),null);
        if (v==null) return null;
        assert v instanceof InternalRelationType;
        return new RelationTypeIndexWrapper((InternalRelationType)v);
    }

    @Override
    public Iterable<RelationTypeIndex> getRelationIndexes(final RelationType type) {
        Preconditions.checkArgument(type!=null && type instanceof InternalRelationType,"Invalid relation type provided: %s",type);
        return Iterables.transform(Iterables.filter(((InternalRelationType) type).getRelationIndexes(), new Predicate<InternalRelationType>() {
            @Override
            public boolean apply(@Nullable InternalRelationType internalRelationType) {
                return !type.equals(internalRelationType);
            }
        }),new Function<InternalRelationType, RelationTypeIndex>() {
            @Nullable
            @Override
            public RelationTypeIndex apply(@Nullable InternalRelationType internalType) {
                return new RelationTypeIndexWrapper(internalType);
            }
        });
    }

    /* --------------
    Graph Indexes
     --------------- */

    public static IndexType getGraphIndexDirect(String name, StandardTitanTx transaction) {
        TitanSchemaVertex v = transaction.getSchemaVertex(TitanSchemaCategory.GRAPHINDEX.getSchemaName(name));
        if (v==null) return null;
        return v.asIndexType();
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
                transaction.getVertices(BaseKey.SchemaCategory, TitanSchemaCategory.GRAPHINDEX),
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

    private void checkIndexName(String indexName) {
        Preconditions.checkArgument(StringUtils.isNotBlank(indexName));
        Preconditions.checkArgument(getGraphIndex(indexName) == null, "An index with name '%s' has already been defined", indexName);
    }

    private TitanGraphIndex createMixedIndex(String indexName, ElementCategory elementCategory,
                                             TitanSchemaType constraint, String backingIndex) {
        Preconditions.checkArgument(graph.getIndexSerializer().containsIndex(backingIndex),"Unknown external index backend: %s",backingIndex);
        checkIndexName(indexName);

        TypeDefinitionMap def = new TypeDefinitionMap();
        def.setValue(TypeDefinitionCategory.INTERNAL_INDEX,false);
        def.setValue(TypeDefinitionCategory.ELEMENT_CATEGORY,elementCategory);
        def.setValue(TypeDefinitionCategory.BACKING_INDEX,backingIndex);
        def.setValue(TypeDefinitionCategory.INDEXSTORE_NAME,indexName);
        def.setValue(TypeDefinitionCategory.INDEX_CARDINALITY, Cardinality.LIST);
        def.setValue(TypeDefinitionCategory.STATUS,SchemaStatus.ENABLED);
        TitanSchemaVertex v = transaction.makeSchemaVertex(TitanSchemaCategory.GRAPHINDEX,indexName,def);

        Preconditions.checkArgument(constraint==null || (elementCategory.isValidConstraint(constraint) && constraint instanceof TitanSchemaVertex));
        if (constraint!=null) {
            addSchemaEdge(v,(TitanSchemaVertex)constraint,TypeDefinitionCategory.INDEX_SCHEMA_CONSTRAINT,null);
        }

        return new TitanGraphIndexWrapper(v.asIndexType());
    }

    @Override
    public void addIndexKey(final TitanGraphIndex index, final PropertyKey key, Parameter... parameters) {
        Preconditions.checkArgument(index!=null && key!=null && index instanceof TitanGraphIndexWrapper
                && !(key instanceof BaseKey),"Need to provide valid index and key");
        if (parameters==null) parameters=new Parameter[0];
        IndexType indexType = ((TitanGraphIndexWrapper)index).getBaseIndex();
        Preconditions.checkArgument(indexType instanceof MixedIndexType,"Can only add keys to an external index, not %s",index.getName());
        Preconditions.checkArgument(indexType instanceof IndexTypeWrapper && key instanceof TitanSchemaVertex
            && ((IndexTypeWrapper)indexType).getSchemaBase() instanceof TitanSchemaVertex);
        Preconditions.checkArgument(key.getCardinality()==Cardinality.SINGLE || indexType.getElement()!=ElementCategory.VERTEX,
                "Can only index single-valued property keys on vertices [%s]",key);
        TitanSchemaVertex indexVertex = (TitanSchemaVertex)((IndexTypeWrapper)indexType).getSchemaBase();

        for (IndexField field : indexType.getFieldKeys())
            Preconditions.checkArgument(!field.getFieldKey().equals(key),"Key [%s] has already been added to index %s",key.getName(),index.getName());

        Parameter[] extendedParas = new Parameter[parameters.length+1];
        System.arraycopy(parameters,0,extendedParas,0,parameters.length);
        extendedParas[parameters.length]= ParameterType.STATUS.getParameter(key.isNew()?SchemaStatus.ENABLED:SchemaStatus.INSTALLED);
        addSchemaEdge(indexVertex, key, TypeDefinitionCategory.INDEX_FIELD, extendedParas);
        indexType.resetCache();
        try {
            IndexSerializer.register((MixedIndexType) indexType,key,transaction.getTxHandle());
        } catch (BackendException e) {
            throw new TitanException("Could not register new index field with index backend",e);
        }
        if (!indexVertex.isNew()) updatedTypes.add(indexVertex);
        if (!key.isNew()) updateIndex(index, SchemaAction.REGISTER_INDEX);
        //TODO: it is possible to have a race condition here if two threads add the same field at the same time
    }

    private TitanGraphIndex createCompositeIndex(String indexName, ElementCategory elementCategory, boolean unique, TitanSchemaType constraint, PropertyKey... keys) {
        checkIndexName(indexName);
        Preconditions.checkArgument(keys!=null && keys.length>0,"Need to provide keys to index [%s]",indexName);
        Preconditions.checkArgument(!unique || elementCategory==ElementCategory.VERTEX,"Unique indexes can only be created on vertices [%s]",indexName);
        boolean allSingleKeys = true;
        boolean oneNewKey = false;
        for (PropertyKey key : keys) {
            Preconditions.checkArgument(key!=null && key instanceof PropertyKeyVertex,"Need to provide valid keys: %s",key);
            if (key.getCardinality()!=Cardinality.SINGLE) allSingleKeys=false;
            if (key.isNew()) oneNewKey = true;
            else updatedTypes.add((PropertyKeyVertex)key);
        }

        Cardinality indexCardinality;
        if (unique) indexCardinality=Cardinality.SINGLE;
        else indexCardinality=(allSingleKeys?Cardinality.SET:Cardinality.LIST);

        TypeDefinitionMap def = new TypeDefinitionMap();
        def.setValue(TypeDefinitionCategory.INTERNAL_INDEX,true);
        def.setValue(TypeDefinitionCategory.ELEMENT_CATEGORY,elementCategory);
        def.setValue(TypeDefinitionCategory.BACKING_INDEX,Token.INTERNAL_INDEX_NAME);
        def.setValue(TypeDefinitionCategory.INDEXSTORE_NAME,indexName);
        def.setValue(TypeDefinitionCategory.INDEX_CARDINALITY,indexCardinality);
        def.setValue(TypeDefinitionCategory.STATUS,oneNewKey?SchemaStatus.ENABLED:SchemaStatus.INSTALLED);
        TitanSchemaVertex indexVertex = transaction.makeSchemaVertex(TitanSchemaCategory.GRAPHINDEX,indexName,def);
        for (int i = 0; i <keys.length; i++) {
            Parameter[] paras = {ParameterType.INDEX_POSITION.getParameter(i)};
            addSchemaEdge(indexVertex,keys[i],TypeDefinitionCategory.INDEX_FIELD,paras);
        }

        Preconditions.checkArgument(constraint==null || (elementCategory.isValidConstraint(constraint) && constraint instanceof TitanSchemaVertex));
        if (constraint!=null) {
            addSchemaEdge(indexVertex,(TitanSchemaVertex)constraint,TypeDefinitionCategory.INDEX_SCHEMA_CONSTRAINT,null);
        }

        TitanGraphIndexWrapper index = new TitanGraphIndexWrapper(indexVertex.asIndexType());
        if (!oneNewKey) updateIndex(index,SchemaAction.REGISTER_INDEX);
        return index;
    }

    @Override
    public TitanManagement.IndexBuilder buildIndex(String indexName, Class<? extends Element> elementType) {
        return new IndexBuilder(indexName,ElementCategory.getByClazz(elementType));
    }

    private class IndexBuilder implements TitanManagement.IndexBuilder {

        private final String indexName;
        private final ElementCategory elementCategory;
        private boolean unique = false;
        private TitanSchemaType constraint = null;
        private Map<PropertyKey,Parameter[]> keys = new HashMap<PropertyKey, Parameter[]>();

        private IndexBuilder(String indexName, ElementCategory elementCategory) {
            this.indexName = indexName;
            this.elementCategory = elementCategory;
        }

        @Override
        public TitanManagement.IndexBuilder indexKey(PropertyKey key) {
            Preconditions.checkArgument(key!=null && (key instanceof PropertyKeyVertex),"Key must be a user defined key: %s",key);
            keys.put(key,null);
            return this;
        }

        @Override
        public TitanManagement.IndexBuilder indexKey(PropertyKey key, Parameter... parameters) {
            Preconditions.checkArgument(key!=null && (key instanceof PropertyKeyVertex),"Key must be a user defined key: %s",key);
            keys.put(key,parameters);
            return this;
        }

        @Override
        public TitanManagement.IndexBuilder indexOnly(TitanSchemaType schemaType) {
            Preconditions.checkNotNull(schemaType);
            Preconditions.checkArgument(elementCategory.isValidConstraint(schemaType),"Need to specify a valid schema type for this index definition: %s",schemaType);
            constraint = schemaType;
            return this;
        }

        @Override
        public TitanManagement.IndexBuilder unique() {
            unique = true;
            return this;
        }

        @Override
        public TitanGraphIndex buildCompositeIndex() {
            Preconditions.checkArgument(!keys.isEmpty(),"Need to specify at least one key for the composite index");
            PropertyKey[] keyArr = new PropertyKey[keys.size()];
            int pos = 0;
            for (Map.Entry<PropertyKey,Parameter[]> entry : keys.entrySet()) {
                Preconditions.checkArgument(entry.getValue()==null,"Cannot specify parameters for composite index: %s",entry.getKey());
                keyArr[pos++]=entry.getKey();
            }
            return createCompositeIndex(indexName, elementCategory, unique, constraint, keyArr);
        }

        @Override
        public TitanGraphIndex buildMixedIndex(String backingIndex) {
            Preconditions.checkArgument(StringUtils.isNotBlank(backingIndex),"Need to specify backing index name");
            Preconditions.checkArgument(!unique,"An external index cannot be unique");

            TitanGraphIndex index = createMixedIndex(indexName, elementCategory, constraint, backingIndex);
            for (Map.Entry<PropertyKey,Parameter[]> entry : keys.entrySet()) {
                addIndexKey(index,entry.getKey(),entry.getValue());
            }
            return index;
        }
    }

    /* --------------
    Schema Update
     --------------- */

    @Override
    public void updateIndex(TitanIndex index, SchemaAction updateAction) {
        Preconditions.checkArgument(updateAction!=null,"Need to provide update action");
        Preconditions.checkArgument(updateAction!=SchemaAction.REMOVE_INDEX);

        TitanSchemaVertex schemaVertex = getSchemaVertex(index);
        Set<TitanSchemaVertex> dependentTypes;
        Set<PropertyKeyVertex> keySubset = ImmutableSet.of();
        if (index instanceof RelationTypeIndex) {
            dependentTypes = ImmutableSet.of((TitanSchemaVertex)((InternalRelationType)schemaVertex).getBaseType());
            Preconditions.checkArgument(updateAction.getApplicableStatus().contains(schemaVertex.getStatus()),
                    "Update action [%s] does not apply for index with status [%s]",updateAction,schemaVertex.getStatus());
        } else if (index instanceof TitanGraphIndex) {
            IndexType indexType = schemaVertex.asIndexType();
            dependentTypes = Sets.newHashSet();
            if (indexType.isCompositeIndex()) {
                Preconditions.checkArgument(updateAction.getApplicableStatus().contains(schemaVertex.getStatus()),
                        "Update action [%s] does not apply for index with status [%s]",updateAction,schemaVertex.getStatus());
                for (PropertyKey key : ((TitanGraphIndex)index).getFieldKeys()) {
                    dependentTypes.add((PropertyKeyVertex)key);
                }
            } else {
                keySubset = Sets.newHashSet();
                MixedIndexType cindexType = (MixedIndexType)indexType;
                Set<SchemaStatus> applicableStatus = updateAction.getApplicableStatus();
                for (ParameterIndexField field : cindexType.getFieldKeys()) {
                    if (applicableStatus.contains(field.getStatus())) keySubset.add((PropertyKeyVertex)field.getFieldKey());
                }
                Preconditions.checkArgument(!keySubset.isEmpty(),"Update action [%s] does not apply to any fields for index [%s]",updateAction,index);
                dependentTypes.addAll(keySubset);
            }
        } else throw new UnsupportedOperationException("Updates not supported for index: " + index);

        switch(updateAction) {
            case REGISTER_INDEX:
                setStatus(schemaVertex,SchemaStatus.INSTALLED,keySubset);
                updatedTypes.add(schemaVertex);
                updatedTypes.addAll(dependentTypes);
                setUpdateTrigger(new UpdateStatusTrigger(graph, schemaVertex, SchemaStatus.REGISTERED, keySubset));
                break;
            case REINDEX:
                throw new UnsupportedOperationException(updateAction + " requires a manual step: run a MapReduce reindex on index name \"" + index.getName() + "\"");
            case ENABLE_INDEX:
                setStatus(schemaVertex,SchemaStatus.ENABLED,keySubset);
                updatedTypes.add(schemaVertex);
                if (!keySubset.isEmpty()) updatedTypes.addAll(dependentTypes);
                break;
            case DISABLE_INDEX:
                setStatus(schemaVertex,SchemaStatus.INSTALLED,keySubset);
                updatedTypes.add(schemaVertex);
                if (!keySubset.isEmpty()) updatedTypes.addAll(dependentTypes);
                setUpdateTrigger(new UpdateStatusTrigger(graph, schemaVertex, SchemaStatus.DISABLED, keySubset));
                break;
            case REMOVE_INDEX:
                throw new UnsupportedOperationException("Removing indexes is not yet supported");
            default: throw new UnsupportedOperationException("Update action not supported: " + updateAction);
        }
    }

    private static class UpdateStatusTrigger implements Callable<Boolean> {

        private final StandardTitanGraph graph;
        private final long schemaVertexId;
        private final SchemaStatus newStatus;
        private final Set<Long> propertyKeys;

        private UpdateStatusTrigger(StandardTitanGraph graph, TitanSchemaVertex vertex, SchemaStatus newStatus, Iterable<PropertyKeyVertex> keys) {
            this.graph = graph;
            this.schemaVertexId = vertex.getLongId();
            this.newStatus = newStatus;
            this.propertyKeys = Sets.newHashSet(Iterables.transform(keys,new Function<PropertyKey, Long>() {
                @Nullable
                @Override
                public Long apply(@Nullable PropertyKey propertyKey) {
                    return propertyKey.getLongId();
                }
            }));
        }

        @Override
        public Boolean call() throws Exception {
            ManagementSystem mgmt = (ManagementSystem)graph.getManagementSystem();
            try {
                TitanVertex vertex = mgmt.transaction.getVertex(schemaVertexId);
                Preconditions.checkArgument(vertex!=null && vertex instanceof TitanSchemaVertex);
                TitanSchemaVertex schemaVertex = (TitanSchemaVertex)vertex;

                Set<PropertyKeyVertex> keys = Sets.newHashSet();
                for (Long keyId : propertyKeys) keys.add((PropertyKeyVertex)mgmt.transaction.getVertex(keyId));
                mgmt.setStatus(schemaVertex,newStatus,keys);
                mgmt.updatedTypes.addAll(keys);
                mgmt.updatedTypes.add(schemaVertex);
                mgmt.commit();
                return true;
            } catch (RuntimeException e) {
                mgmt.rollback();
                throw e;
            }
        }

        @Override
        public int hashCode() {
            return Long.valueOf(schemaVertexId).hashCode();
        }

        @Override
        public boolean equals(Object oth) {
            if (this==oth) return true;
            else if (oth==null || !getClass().isInstance(oth)) return false;
            return schemaVertexId ==((UpdateStatusTrigger)oth).schemaVertexId;
        }

    }

    private void setUpdateTrigger(Callable<Boolean> trigger) {
        //Make sure the most current is the one set
        if (updatedTypeTriggers.contains(trigger)) updatedTypeTriggers.remove(trigger);
        updatedTypeTriggers.add(trigger);
    }

    private void setStatus(TitanSchemaVertex vertex, SchemaStatus status, Set<PropertyKeyVertex> keys) {
        if (keys.isEmpty()) setStatusVertex(vertex,status);
        else setStatusEdges(vertex,status,keys);
    }

    private void setStatusVertex(TitanSchemaVertex vertex, SchemaStatus status) {
        Preconditions.checkArgument(vertex instanceof RelationTypeVertex || vertex.asIndexType().isCompositeIndex());

        //Delete current status
        for (TitanProperty p : vertex.getProperties(BaseKey.SchemaDefinitionProperty)) {
            if (p.<TypeDefinitionDescription>getProperty(BaseKey.SchemaDefinitionDesc).getCategory()==TypeDefinitionCategory.STATUS) {
                if (p.getValue().equals(status)) return;
                else p.remove();
            }
        }
        //Add new status
        TitanProperty p = transaction.addProperty(vertex, BaseKey.SchemaDefinitionProperty, status);
        p.setProperty(BaseKey.SchemaDefinitionDesc,TypeDefinitionDescription.of(TypeDefinitionCategory.STATUS));

        vertex.resetCache();
    }

    private void setStatusEdges(TitanSchemaVertex vertex, SchemaStatus status, Set<PropertyKeyVertex> keys) {
        Preconditions.checkArgument(vertex.asIndexType().isMixedIndex());

        for (TitanEdge edge : vertex.getEdges(TypeDefinitionCategory.INDEX_FIELD,Direction.OUT)) {
            if (!keys.contains(edge.getVertex(Direction.IN))) continue; //Only address edges with matching keys
            TypeDefinitionDescription desc = edge.getProperty(BaseKey.SchemaDefinitionDesc);
            assert desc.getCategory()==TypeDefinitionCategory.INDEX_FIELD;
            Parameter[] parameters = (Parameter[])desc.getModifier();
            assert parameters[parameters.length-1].getKey().equals(ParameterType.STATUS.getName());
            if (parameters[parameters.length-1].getValue().equals(status)) continue;

            Parameter[] paraCopy = Arrays.copyOf(parameters,parameters.length);
            paraCopy[parameters.length-1]=ParameterType.STATUS.getParameter(status);
            edge.remove();
            addSchemaEdge(vertex,edge.getVertex(Direction.IN),TypeDefinitionCategory.INDEX_FIELD,paraCopy);
        }

        for (PropertyKeyVertex prop : keys) prop.resetCache();
        vertex.resetCache();
    }


    @Override
    public void changeName(TitanSchemaElement element, String newName) {
        Preconditions.checkArgument(StringUtils.isNotBlank(newName),"Invalid name: %s",newName);
        TitanSchemaVertex schemaVertex = getSchemaVertex(element);
        if (schemaVertex.getName().equals(newName)) return;

        TitanSchemaCategory schemaCategory = schemaVertex.getProperty(BaseKey.SchemaCategory);
        Preconditions.checkArgument(schemaCategory.hasName(),"Invalid schema element: %s",element);

        if (schemaVertex instanceof RelationType) {
            InternalRelationType relType = (InternalRelationType)schemaVertex;
            if (relType.getBaseType()!=null) {
                newName = composeRelationTypeIndexName(relType.getBaseType(),newName);
            } else assert !(element instanceof RelationTypeIndex);
            StandardRelationTypeMaker.checkName(newName);
        } else if (element instanceof VertexLabel) {
            StandardVertexLabelMaker.checkName(newName);
        } else if (element instanceof TitanGraphIndex) {
            checkIndexName(newName);
        }

        transaction.addProperty(schemaVertex, BaseKey.SchemaName, schemaCategory.getSchemaName(newName));
        schemaVertex.resetCache();
        updatedTypes.add(schemaVertex);
    }

    public TitanSchemaVertex getSchemaVertex(TitanSchemaElement element) {
        if (element instanceof RelationType) {
            Preconditions.checkArgument(element instanceof RelationTypeVertex,"Invalid schema element provided: %s",element);
            return (RelationTypeVertex)element;
        } else if (element instanceof RelationTypeIndex) {
            return (RelationTypeVertex)((RelationTypeIndexWrapper)element).getWrappedType();
        } else if (element instanceof VertexLabel) {
            Preconditions.checkArgument(element instanceof VertexLabelVertex,"Invalid schema element provided: %s",element);
            return (VertexLabelVertex)element;
        } else if (element instanceof TitanGraphIndex) {
            Preconditions.checkArgument(element instanceof TitanGraphIndexWrapper,"Invalid schema element provided: %s",element);
            IndexType index = ((TitanGraphIndexWrapper)element).getBaseIndex();
            assert index instanceof IndexTypeWrapper;
            SchemaSource base = ((IndexTypeWrapper)index).getSchemaBase();
            assert base instanceof TitanSchemaVertex;
            return (TitanSchemaVertex)base;
        }
        throw new IllegalArgumentException("Invalid schema element provided: "+element);
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
        if (element instanceof RelationType) return ((InternalRelationType)element).getConsistencyModifier();
        else if (element instanceof TitanGraphIndex) {
            IndexType index = ((TitanGraphIndexWrapper)element).getBaseIndex();
            if (index.isMixedIndex()) return ConsistencyModifier.DEFAULT;
            return ((CompositeIndexType)index).getConsistencyModifier();
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
        if (element instanceof RelationType) {
            vertex = (RelationTypeVertex)element;
            Preconditions.checkArgument(consistency!=ConsistencyModifier.FORK || !((RelationTypeVertex)vertex).getMultiplicity().isConstrained(),
                    "Cannot apply FORK consistency mode to constraint relation type: %s",vertex.getName());
        } else if (element instanceof TitanGraphIndex) {
            IndexType index = ((TitanGraphIndexWrapper)element).getBaseIndex();
            if (index.isMixedIndex()) throw new IllegalArgumentException("Cannot change consistency on an external index: " + element);
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
    public boolean containsRelationType(String name) {
        return transaction.containsRelationType(name);
    }

    @Override
    public RelationType getRelationType(String name) {
        return transaction.getRelationType(name);
    }

    @Override
    public PropertyKey getPropertyKey(String name) {
        return transaction.getPropertyKey(name);
    }

    @Override
    public EdgeLabel getEdgeLabel(String name) {
        return transaction.getEdgeLabel(name);
    }

    @Override
    public PropertyKeyMaker makePropertyKey(String name) {
        return transaction.makePropertyKey(name);
    }

    @Override
    public EdgeLabelMaker makeEdgeLabel(String name) {
        return transaction.makeEdgeLabel(name);
    }

    @Override
    public <T extends RelationType> Iterable<T> getRelationTypes(Class<T> clazz) {
        Preconditions.checkNotNull(clazz);
        Iterable<? extends TitanVertex> types = null;
        if (PropertyKey.class.equals(clazz)) {
            types = transaction.getVertices(BaseKey.SchemaCategory, TitanSchemaCategory.PROPERTYKEY);
        } else if (EdgeLabel.class.equals(clazz)) {
            types = transaction.getVertices(BaseKey.SchemaCategory, TitanSchemaCategory.EDGELABEL);
        } else if (RelationType.class.equals(clazz)) {
            types = Iterables.concat(getRelationTypes(EdgeLabel.class), getRelationTypes(PropertyKey.class));
        } else throw new IllegalArgumentException("Unknown type class: " + clazz);
        return Iterables.filter(Iterables.filter(types, clazz),new Predicate<T>() {
            @Override
            public boolean apply(@Nullable T t) {
                //Filter out all relation type indexes
                return ((InternalRelationType)t).getBaseType()==null;
            }
        });
    }

    @Override
    public boolean containsVertexLabel(String name) {
        return transaction.containsVertexLabel(name);
    }

    @Override
    public VertexLabel getVertexLabel(String name) {
        return transaction.getVertexLabel(name);
    }

    @Override
    public VertexLabelMaker makeVertexLabel(String name) {
        return transaction.makeVertexLabel(name);
    }

    @Override
    public Iterable<VertexLabel> getVertexLabels() {
        return Iterables.filter(transaction.getVertices(BaseKey.SchemaCategory,TitanSchemaCategory.VERTEXLABEL),VertexLabel.class);
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
