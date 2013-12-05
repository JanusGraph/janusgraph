package com.thinkaurelius.faunus.formats.titan.input.titan03;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.google.common.collect.Lists;
import com.thinkaurelius.faunus.formats.titan.TitanInputFormat;
import com.thinkaurelius.faunus.formats.titan.input.TitanFaunusSetupCommon;
import com.thinkaurelius.faunus.formats.titan.util.ConfigurationUtil;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.Parameter;
import com.thinkaurelius.titan.core.attribute.FullDouble;
import com.thinkaurelius.titan.core.attribute.FullFloat;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.graphdb.database.RelationReader;
import com.thinkaurelius.titan.graphdb.relations.RelationCache;
import com.thinkaurelius.titan.graphdb.types.*;
import com.thinkaurelius.titan.graphdb.types.reference.TitanKeyReference;
import com.thinkaurelius.titan.graphdb.types.reference.TitanLabelReference;
import com.thinkaurelius.titan.graphdb.types.reference.TypeReferenceContainer;
import com.tinkerpop.blueprints.Direction;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import titan03.com.thinkaurelius.titan.core.TitanVertex;
import titan03.com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import titan03.com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import titan03.com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import titan03.com.thinkaurelius.titan.graphdb.types.EdgeLabelDefinition;
import titan03.com.thinkaurelius.titan.graphdb.types.PropertyKeyDefinition;
import titan03.com.thinkaurelius.titan.graphdb.types.TitanTypeClass;
import titan03.com.thinkaurelius.titan.graphdb.types.TypeDefinition;
import titan03.com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import titan03.com.thinkaurelius.titan.graphdb.types.vertices.TitanKeyVertex;
import titan03.com.thinkaurelius.titan.graphdb.types.vertices.TitanLabelVertex;
import titan03.com.thinkaurelius.titan.util.datastructures.ImmutableLongObjectMap;

import java.util.Iterator;
import java.util.List;

import static titan03.com.thinkaurelius.titan.graphdb.database.EdgeSerializer.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanFaunusSetupImpl extends TitanFaunusSetupCommon {

    private final GraphDatabaseConfiguration graphConfig;
    private StandardTitanGraph graph;
    private StandardTitanTx tx;

    public TitanFaunusSetupImpl(final Configuration config) {
        BaseConfiguration titan = ConfigurationUtil.extractConfiguration(config, TitanInputFormat.FAUNUS_GRAPH_INPUT_TITAN);
        titan03.org.apache.commons.configuration.BaseConfiguration copyConfig = new titan03.org.apache.commons.configuration.BaseConfiguration();
        Iterator<String> keys = titan.getKeys();
        while (keys.hasNext()) {
            String key = keys.next();
            copyConfig.setProperty(key,titan.getProperty(key));
        }
        graphConfig = new GraphDatabaseConfiguration(copyConfig);
        graph = new StandardTitanGraph(graphConfig);
        tx = (StandardTitanTx)graph.newTransaction();
    }

    @Override
    public TypeInspector getTypeInspector() {
        TypeReferenceContainer types = new TypeReferenceContainer();
        for (TitanVertex v : tx.getVertices(SystemKey.TypeClass, TitanTypeClass.KEY)) {
            TitanKeyVertex k = (TitanKeyVertex) v;
            PropertyKeyDefinition def = k.getDefinition();
            TypeAttribute.Map definition = new TypeAttribute.Map();
            transfer(def,definition);
            List<IndexType> indexes = Lists.newArrayList();
            for (String indexname : def.getIndexes(titan03.com.tinkerpop.blueprints.Vertex.class)) {
                indexes.add(new IndexType(indexname,com.tinkerpop.blueprints.Vertex.class));
            }
            for (String indexname : def.getIndexes(titan03.com.tinkerpop.blueprints.Edge.class)) {
                indexes.add(new IndexType(indexname,com.tinkerpop.blueprints.Edge.class));
            }
            definition.setValue(TypeAttributeType.INDEXES,indexes.toArray(new IndexType[indexes.size()]));
            IndexParameters[] indexparas = new IndexParameters[indexes.size()];
            for (int i=0;i<indexparas.length;i++) indexparas[i]=new IndexParameters(indexes.get(i).getIndexName(),new Parameter[0]);
            definition.setValue(TypeAttributeType.INDEX_PARAMETERS,indexparas);
            definition.setValue(TypeAttributeType.DATATYPE,convertDatatype(def.getDataType()));
            types.add(new TitanKeyReference(k.getID(),k.getName(),definition));
        }
        for (TitanVertex v : tx.getVertices(SystemKey.TypeClass, TitanTypeClass.LABEL)) {
            TitanLabelVertex l = (TitanLabelVertex) v;
            EdgeLabelDefinition def = l.getDefinition();
            TypeAttribute.Map definition = new TypeAttribute.Map();
            transfer(def,definition);
            definition.setValue(TypeAttributeType.UNIDIRECTIONAL,def.isUnidirectional());
            types.add(new TitanLabelReference(l.getID(),l.getName(),definition));
        }
        return types;
    }

    private static void transfer(TypeDefinition typedef, TypeAttribute.Map definition) {
        definition.setValue(TypeAttributeType.HIDDEN,typedef.isHidden());
        definition.setValue(TypeAttributeType.MODIFIABLE,typedef.isModifiable());
        boolean[] uniqueness = {typedef.isUnique(titan03.com.tinkerpop.blueprints.Direction.OUT),typedef.isUnique(titan03.com.tinkerpop.blueprints.Direction.IN)};
        boolean[] uniqunesslock = {typedef.uniqueLock(titan03.com.tinkerpop.blueprints.Direction.OUT),typedef.uniqueLock(titan03.com.tinkerpop.blueprints.Direction.IN)};
        boolean[] isstatic = {typedef.isStatic(titan03.com.tinkerpop.blueprints.Direction.OUT),typedef.isStatic(titan03.com.tinkerpop.blueprints.Direction.IN)};
        definition.setValue(TypeAttributeType.UNIQUENESS,uniqueness);
        definition.setValue(TypeAttributeType.UNIQUENESS_LOCK,uniqunesslock);
        definition.setValue(TypeAttributeType.STATIC,isstatic);
        definition.setValue(TypeAttributeType.SORT_KEY,typedef.getPrimaryKey());
        definition.setValue(TypeAttributeType.SIGNATURE,typedef.getSignature());
        definition.setValue(TypeAttributeType.SORT_ORDER, Order.ASC);
    }

    @Override
    public RelationReader getRelationReader() {
        return new RelationReader() {

            @Override
            public RelationCache parseRelation(long vertexid, Entry entry, boolean headerOnly, TypeInspector typeInspector) {
                titan03.com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer column = new titan03.com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer(entry.getArrayColumn());
                titan03.com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer value = new titan03.com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer(entry.getArrayValue());

                ImmutableLongObjectMap map = graph.getEdgeSerializer().parseProperties(vertexid,titan03.com.thinkaurelius.titan.diskstorage.keycolumnvalue.StaticBufferEntry.of(column,value),headerOnly,tx);
                titan03.com.tinkerpop.blueprints.Direction dir = map.get(DIRECTION_ID);
                Direction direction;
                switch (dir) {
                    case IN: direction=Direction.IN; break;
                    case OUT: direction=Direction.OUT; break;
                    default: throw new IllegalArgumentException("Invalid direction found");
                }

                long typeid = map.get(TYPE_ID);
                long relationid = map.get(RELATION_ID);
                Object propValue = map.get(VALUE_ID);
                if (propValue!=null) { //Property
                    propValue = convertPropertyValue(propValue);
                    return new RelationCache(direction,typeid,relationid,propValue,null);
                } else { //Edge
                    long otherVertexId = map.get(OTHER_VERTEX_ID);
                    LongObjectOpenHashMap<Object> properties = new LongObjectOpenHashMap<Object>();
                    //Add properties
                    for (int i=0;i<map.size();i++) {
                        long propTypeId = map.getKey(i);
                        if (propTypeId>0) {
                            if (map.getValue(i)!=null) {
                                properties.put(propTypeId, convertPropertyValue(map.getValue(i)));
                            }
                        }
                    }
                    return new RelationCache(direction,typeid,relationid,otherVertexId,properties);
                }
            }

        };
    }

    private static Object convertPropertyValue(Object value) {
        assert value!=null;
        if (value instanceof titan03.com.thinkaurelius.titan.core.attribute.Geoshape) {
            titan03.com.thinkaurelius.titan.core.attribute.Geoshape geo = (titan03.com.thinkaurelius.titan.core.attribute.Geoshape)value;
            Geoshape newgeo;
            switch(geo.getType()) {
                case POINT:
                    titan03.com.thinkaurelius.titan.core.attribute.Geoshape.Point p = geo.getPoint();
                    newgeo = Geoshape.point(p.getLatitude(),p.getLongitude());
                    break;
                case CIRCLE:
                    p = geo.getPoint();
                    newgeo = Geoshape.circle(p.getLatitude(), p.getLongitude(), geo.getRadius());
                    break;
                case BOX:
                    titan03.com.thinkaurelius.titan.core.attribute.Geoshape.Point p1 = geo.getPoint(1);
                    titan03.com.thinkaurelius.titan.core.attribute.Geoshape.Point p2 = geo.getPoint(2);
                    newgeo = Geoshape.box(p1.getLatitude(),p1.getLongitude(),p2.getLatitude(),p2.getLongitude());
                    break;
                default:
                    throw new IllegalArgumentException("Invalid geoshape found: " + geo);
            }
            return newgeo;
        } else if (value instanceof titan03.com.thinkaurelius.titan.core.attribute.FullDouble) {
            return new FullDouble(((titan03.com.thinkaurelius.titan.core.attribute.FullDouble)value).doubleValue());
        } else if (value instanceof titan03.com.thinkaurelius.titan.core.attribute.FullFloat) {
            return new FullFloat(((titan03.com.thinkaurelius.titan.core.attribute.FullFloat)value).floatValue());
        } else return value;
    }

    private static Class convertDatatype(Class clazz) {
        assert clazz!=null;
        if (clazz.equals(titan03.com.thinkaurelius.titan.core.attribute.Geoshape.class)) return Geoshape.class;
        else if (clazz.equals(titan03.com.thinkaurelius.titan.core.attribute.FullDouble.class)) return FullDouble.class;
        else if (clazz.equals(titan03.com.thinkaurelius.titan.core.attribute.FullFloat.class)) return FullFloat.class;
        else return clazz;
    }


    @Override
    public void close() {
        if (tx!=null) tx.rollback();
        if (graph!=null) graph.shutdown();
    }

}
