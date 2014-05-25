package com.thinkaurelius.titan.hadoop.formats.titan.input.titan03;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.RelationType;
import com.thinkaurelius.titan.core.attribute.Decimal;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.core.attribute.Precision;
import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.schema.ConsistencyModifier;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.graphdb.database.RelationReader;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.relations.RelationCache;
import com.thinkaurelius.titan.graphdb.types.IndexType;
import com.thinkaurelius.titan.graphdb.types.SchemaStatus;
import com.thinkaurelius.titan.graphdb.types.TypeInspector;
import com.thinkaurelius.titan.graphdb.types.system.EmptyVertex;
import com.thinkaurelius.titan.hadoop.formats.titan.TitanInputFormat;
import com.thinkaurelius.titan.hadoop.formats.titan.input.SystemTypeInspector;
import com.thinkaurelius.titan.hadoop.formats.titan.input.TitanHadoopSetupCommon;
import com.thinkaurelius.titan.hadoop.formats.titan.input.VertexReader;
import com.thinkaurelius.titan.hadoop.formats.titan.util.ConfigurationUtil;
import com.tinkerpop.blueprints.Direction;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;

import titan03.com.thinkaurelius.titan.core.TitanVertex;
import titan03.com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import titan03.com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import titan03.com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import titan03.com.thinkaurelius.titan.graphdb.types.TitanTypeClass;
import titan03.com.thinkaurelius.titan.graphdb.types.TypeDefinition;
import titan03.com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import titan03.com.thinkaurelius.titan.graphdb.types.vertices.TitanKeyVertex;
import titan03.com.thinkaurelius.titan.graphdb.types.vertices.TitanLabelVertex;
import titan03.com.thinkaurelius.titan.graphdb.types.vertices.TitanTypeVertex;
import titan03.com.thinkaurelius.titan.util.datastructures.ImmutableLongObjectMap;

import static titan03.com.thinkaurelius.titan.graphdb.database.EdgeSerializer.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanHadoopSetupImpl extends TitanHadoopSetupCommon {

    private final GraphDatabaseConfiguration graphConfig;
    private StandardTitanGraph graph;
    private StandardTitanTx tx;

    public TitanHadoopSetupImpl(final Configuration config) {
        BaseConfiguration titan = ConfigurationUtil.extractConfiguration(config, TitanInputFormat.TITAN_HADOOP_GRAPH_INPUT_TITAN);
        graphConfig = new GraphDatabaseConfiguration(titan);
        graph = new StandardTitanGraph(graphConfig);
        tx = (StandardTitanTx) graph.newTransaction();
    }

    @Override
    public TypeInspector getTypeInspector() {
        //Pre-load schema
        for (TitanTypeClass typeclass : TitanTypeClass.values()) {
            for (TitanVertex sv : tx.getVertices(SystemKey.TypeClass,typeclass)) {
                assert sv!=null && sv instanceof TitanTypeVertex;
                String name = ((TitanTypeVertex) sv).getName();
                Preconditions.checkNotNull(name);
                TypeDefinition def=null;
                if (sv instanceof TitanKeyVertex) {
                    def = ((TitanKeyVertex) sv).getDefinition();
                } else if (sv instanceof TitanLabelVertex) {
                    def = ((TitanLabelVertex) sv).getDefinition();
                }
                Preconditions.checkNotNull(def);
            }
        }
        return new TypeInspector() {

            @Override
            public RelationType getExistingRelationType(long id) {
                return convert(tx.getExistingType(id));
            }

            @Override
            public boolean containsRelationType(String name) {
                return tx.containsType(name);
            }

            @Override
            public RelationType getRelationType(String name) {
                return convert(tx.getType(name));
            }
        };
    }

    private RelationType convert(titan03.com.thinkaurelius.titan.core.TitanType base) {
        if (base instanceof titan03.com.thinkaurelius.titan.graphdb.types.vertices.TitanLabelVertex) {
            return new TitanLabelWrapper((titan03.com.thinkaurelius.titan.graphdb.types.vertices.TitanLabelVertex)base);
        } else if (base instanceof titan03.com.thinkaurelius.titan.graphdb.types.vertices.TitanKeyVertex) {
            return new TitanKeyWrapper((titan03.com.thinkaurelius.titan.graphdb.types.vertices.TitanKeyVertex)base);
        } else throw new IllegalArgumentException("Unexpected type vertex: " + base);
    }


    private abstract class RelationTypeWrapper extends EmptyVertex implements InternalRelationType {

        protected final TitanTypeVertex base;

        private RelationTypeWrapper(TitanTypeVertex base) {
            this.base = base;
        }

        @Override
        public long getID() {
            return base.getID();
        }

        @Override
        public boolean hasId() {
            return true;
        }

        @Override
        public boolean isHiddenType() {
            return false;
        }

        @Override
        public String getName() {
            return base.getName();
        }

        @Override
        public boolean isHidden() {
            return true;
        }

        @Override
        public InternalRelationType getBaseType() {
            return null;
        }

        @Override
        public Iterable<InternalRelationType> getRelationIndexes() {
            return ImmutableSet.of((InternalRelationType) this);
        }

        @Override
        public SchemaStatus getStatus() {
            return SchemaStatus.ENABLED;
        }

        @Override
        public Iterable<IndexType> getKeyIndexes() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConsistencyModifier getConsistencyModifier() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long[] getSignature() {
            return base.getDefinition().getSignature();
        }

        @Override
        public long[] getSortKey() {
            return base.getDefinition().getPrimaryKey();
        }

        @Override
        public Order getSortOrder() {
            return Order.ASC;
        }


    }

    private class TitanLabelWrapper extends RelationTypeWrapper implements EdgeLabel {

        protected final titan03.com.thinkaurelius.titan.graphdb.types.vertices.TitanLabelVertex base;

        private TitanLabelWrapper(titan03.com.thinkaurelius.titan.graphdb.types.vertices.TitanLabelVertex base) {
            super(base);
            this.base=base;
        }

        @Override
        public boolean isDirected() {
            return base.isDirected();
        }

        @Override
        public boolean isUnidirected() {
            return isUnidirected(Direction.OUT);
        }

        @Override
        public boolean isPropertyKey() {
            return false;
        }

        @Override
        public boolean isEdgeLabel() {
            return true;
        }

        @Override
        public Multiplicity getMultiplicity() {
            if (base.isUnique(titan03.com.tinkerpop.blueprints.Direction.OUT) && base.isUnique(titan03.com.tinkerpop.blueprints.Direction.IN)) {
                return Multiplicity.ONE2ONE;
            } else if (base.isUnique(titan03.com.tinkerpop.blueprints.Direction.OUT)) {
                return Multiplicity.MANY2ONE;
            } else if (base.isUnique(titan03.com.tinkerpop.blueprints.Direction.IN)) {
                return Multiplicity.ONE2MANY;
            } else return Multiplicity.MULTI;
        }

        @Override
        public boolean isUnidirected(Direction dir) {
            return (base.isUnidirected() && dir==Direction.OUT) || (base.isDirected() && dir==Direction.BOTH);
        }
    }

    private class TitanKeyWrapper extends RelationTypeWrapper implements PropertyKey {

        protected final titan03.com.thinkaurelius.titan.graphdb.types.vertices.TitanKeyVertex base;

        private TitanKeyWrapper(titan03.com.thinkaurelius.titan.graphdb.types.vertices.TitanKeyVertex base) {
            super(base);
            this.base=base;
        }

        @Override
        public boolean isPropertyKey() {
            return true;
        }

        @Override
        public boolean isEdgeLabel() {
            return false;
        }

        @Override
        public Class<?> getDataType() {
            return convertDatatype(base.getDataType());
        }

        @Override
        public Cardinality getCardinality() {
            return base.getDefinition().isUnique(titan03.com.tinkerpop.blueprints.Direction.OUT)?
                    Cardinality.SINGLE:Cardinality.LIST;
        }

        @Override
        public Multiplicity getMultiplicity() {
            return Multiplicity.convert(getCardinality());
        }

        @Override
        public boolean isUnidirected(Direction dir) {
            return dir==Direction.OUT;
        }
    }


//    @Override
//    public TypeInspector getTypeInspector() {
//        TypeReferenceContainer types = new TypeReferenceContainer();
//        for (TitanVertex v : tx.getVertices(SystemKey.TypeClass, TitanTypeClass.KEY)) {
//            TitanKeyVertex k = (TitanKeyVertex) v;
//            PropertyKeyDefinition def = k.getDefinition();
//            TypeAttribute.Map definition = new TypeAttribute.Map();
//            transfer(def, definition);
//            List<IndexType> indexes = Lists.newArrayList();
//            for (String indexname : def.getIndexes(titan03.com.tinkerpop.blueprints.Vertex.class)) {
//                indexes.add(new IndexType(indexname, com.tinkerpop.blueprints.Vertex.class));
//            }
//            for (String indexname : def.getIndexes(titan03.com.tinkerpop.blueprints.Edge.class)) {
//                indexes.add(new IndexType(indexname, com.tinkerpop.blueprints.Edge.class));
//            }
//            definition.setValue(TypeAttributeType.INDEXES, indexes.toArray(new IndexType[indexes.size()]));
//            IndexParameters[] indexparas = new IndexParameters[indexes.size()];
//            for (int i = 0; i < indexparas.length; i++)
//                indexparas[i] = new IndexParameters(indexes.get(i).getIndexName(), new Parameter[0]);
//            definition.setValue(TypeAttributeType.INDEX_PARAMETERS, indexparas);
//            definition.setValue(TypeAttributeType.DATATYPE, convertDatatype(def.getDataType()));
//            types.add(new TitanKeyReference(k.getID(), k.getName(), definition));
//        }
//        for (TitanVertex v : tx.getVertices(SystemKey.TypeClass, TitanTypeClass.LABEL)) {
//            TitanLabelVertex l = (TitanLabelVertex) v;
//            EdgeLabelDefinition def = l.getDefinition();
//            TypeAttribute.Map definition = new TypeAttribute.Map();
//            transfer(def, definition);
//            definition.setValue(TypeAttributeType.UNIDIRECTIONAL, def.isUnidirectional());
//            types.add(new TitanLabelReference(l.getID(), l.getName(), definition));
//        }
//        return types;
//    }
//
//    private static void transfer(TypeDefinition typedef, TypeAttribute.Map definition) {
//        definition.setValue(TypeAttributeType.HIDDEN, typedef.isHidden());
//        definition.setValue(TypeAttributeType.MODIFIABLE, typedef.isModifiable());
//        boolean[] uniqueness = {typedef.isUnique(titan03.com.tinkerpop.blueprints.Direction.OUT), typedef.isUnique(titan03.com.tinkerpop.blueprints.Direction.IN)};
//        boolean[] uniqunesslock = {typedef.uniqueLock(titan03.com.tinkerpop.blueprints.Direction.OUT), typedef.uniqueLock(titan03.com.tinkerpop.blueprints.Direction.IN)};
//        boolean[] isstatic = {typedef.isStatic(titan03.com.tinkerpop.blueprints.Direction.OUT), typedef.isStatic(titan03.com.tinkerpop.blueprints.Direction.IN)};
//        definition.setValue(TypeAttributeType.UNIQUENESS, uniqueness);
//        definition.setValue(TypeAttributeType.UNIQUENESS_LOCK, uniqunesslock);
//        definition.setValue(TypeAttributeType.STATIC, isstatic);
//        definition.setValue(TypeAttributeType.SORT_KEY, typedef.getPrimaryKey());
//        definition.setValue(TypeAttributeType.SIGNATURE, typedef.getSignature());
//        definition.setValue(TypeAttributeType.SORT_ORDER, Order.ASC);
//    }

    private static titan03.com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer convert(StaticBuffer buffer) {
        return new titan03.com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer(buffer.as(StaticBuffer.ARRAY_FACTORY));

    }

    @Override
    public RelationReader getRelationReader(final long vertexid) {
        return new RelationReader() {

            @Override
            public RelationCache parseRelation(Entry entry, boolean headerOnly, TypeInspector typeInspector) {
                titan03.com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer column = convert(entry.getColumn());
                titan03.com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer value = convert(entry.getValue());

                ImmutableLongObjectMap map = graph.getEdgeSerializer().parseProperties(vertexid, titan03.com.thinkaurelius.titan.diskstorage.keycolumnvalue.StaticBufferEntry.of(column, value), headerOnly, tx);
                titan03.com.tinkerpop.blueprints.Direction dir = map.get(DIRECTION_ID);
                Direction direction;
                switch (dir) {
                    case IN:
                        direction = Direction.IN;
                        break;
                    case OUT:
                        direction = Direction.OUT;
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid direction found");
                }

                long typeid = map.get(TYPE_ID);
                long relationid = map.get(RELATION_ID);
                Object propValue = map.get(VALUE_ID);
                if (propValue != null) { //Property
                    propValue = convertPropertyValue(propValue);
                    return new RelationCache(direction, typeid, relationid, propValue, null);
                } else { //Edge
                    long otherVertexId = map.get(OTHER_VERTEX_ID);
                    LongObjectOpenHashMap<Object> properties = new LongObjectOpenHashMap<Object>();
                    //Add properties
                    for (int i = 0; i < map.size(); i++) {
                        long propTypeId = map.getKey(i);
                        if (propTypeId > 0) {
                            if (map.getValue(i) != null) {
                                properties.put(propTypeId, convertPropertyValue(map.getValue(i)));
                            }
                        }
                    }
                    return new RelationCache(direction, typeid, relationid, otherVertexId, properties);
                }
            }

        };
    }

    private static Object convertPropertyValue(Object value) {
        assert value != null;
        if (value instanceof titan03.com.thinkaurelius.titan.core.attribute.Geoshape) {
            titan03.com.thinkaurelius.titan.core.attribute.Geoshape geo = (titan03.com.thinkaurelius.titan.core.attribute.Geoshape) value;
            Geoshape newgeo;
            switch (geo.getType()) {
                case POINT:
                    titan03.com.thinkaurelius.titan.core.attribute.Geoshape.Point p = geo.getPoint();
                    newgeo = Geoshape.point(p.getLatitude(), p.getLongitude());
                    break;
                case CIRCLE:
                    p = geo.getPoint();
                    newgeo = Geoshape.circle(p.getLatitude(), p.getLongitude(), geo.getRadius());
                    break;
                case BOX:
                    titan03.com.thinkaurelius.titan.core.attribute.Geoshape.Point p1 = geo.getPoint(1);
                    titan03.com.thinkaurelius.titan.core.attribute.Geoshape.Point p2 = geo.getPoint(2);
                    newgeo = Geoshape.box(p1.getLatitude(), p1.getLongitude(), p2.getLatitude(), p2.getLongitude());
                    break;
                default:
                    throw new IllegalArgumentException("Invalid geoshape found: " + geo);
            }
            return newgeo;
        } else if (value instanceof titan03.com.thinkaurelius.titan.core.attribute.FullDouble) {
            return new Double(((titan03.com.thinkaurelius.titan.core.attribute.FullDouble) value).doubleValue());
        } else if (value instanceof titan03.com.thinkaurelius.titan.core.attribute.FullFloat) {
            return new Float(((titan03.com.thinkaurelius.titan.core.attribute.FullFloat) value).floatValue());
        } else if (value instanceof Double) {
            return new Precision(((Double) value).doubleValue());
        } else if (value instanceof Float) {
            return new Float(((Float) value).floatValue());
        } else return value;
    }

    private static Class convertDatatype(Class clazz) {
        assert clazz != null;
        if (clazz.equals(titan03.com.thinkaurelius.titan.core.attribute.Geoshape.class)) return Geoshape.class;
        else if (clazz.equals(titan03.com.thinkaurelius.titan.core.attribute.FullDouble.class)) return Double.class;
        else if (clazz.equals(titan03.com.thinkaurelius.titan.core.attribute.FullFloat.class)) return Float.class;
        else if (clazz.equals(Double.class)) return Precision.class;
        else if (clazz.equals(Float.class)) return Decimal.class;
        else return clazz;
    }

    @Override
    public SystemTypeInspector getSystemTypeInspector() {
        return new SystemTypeInspector() {
            @Override
            public boolean isSystemType(long typeid) {
                return isVertexExistsSystemType(typeid) || isTypeSystemType(typeid);
            }

            @Override
            public boolean isVertexExistsSystemType(long typeid) {
                return typeid == SystemKey.VertexState.getID();
            }

            @Override
            public boolean isTypeSystemType(long typeid) {
                return typeid == SystemKey.TypeClass.getID() ||
                        typeid == SystemKey.PropertyKeyDefinition.getID() ||
                        typeid == SystemKey.RelationTypeDefinition.getID() ||
                        typeid == SystemKey.TypeName.getID();
            }
        };
    }

    @Override
    public VertexReader getVertexReader() {
        return new VertexReader() {
            @Override
            public long getVertexId(StaticBuffer key) {
                return titan03.com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler.getKeyID(convert(key));
            }
        };
    }


    @Override
    public void close() {
        if (tx != null) tx.rollback();
        if (graph != null) graph.shutdown();
    }

}
