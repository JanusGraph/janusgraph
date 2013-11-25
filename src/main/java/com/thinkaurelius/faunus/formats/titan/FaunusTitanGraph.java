package com.thinkaurelius.faunus.formats.titan;

import com.carrotsearch.hppc.cursors.LongObjectCursor;
import com.google.common.base.Preconditions;
import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.util.StaticByteBuffer;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.EdgeSerializer;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.relations.RelationCache;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.transaction.StandardTransactionBuilder;
import com.thinkaurelius.titan.graphdb.types.reference.TypeReferenceContainer;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import com.thinkaurelius.titan.graphdb.types.system.SystemType;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import org.apache.commons.configuration.Configuration;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

/**
 * The backend agnostic Titan graph reader for pulling a graph of Titan and into Faunus.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Marko A. Rodriguez (marko@markorodriguez.com)
 */

public class FaunusTitanGraph {

    private final EdgeSerializer edgeSerializer;
    private final TypeReferenceContainer typeManager;

    public FaunusTitanGraph(final Serializer serializer, final Configuration configuration) {
        this.edgeSerializer = new EdgeSerializer(serializer);
        typeManager = new TypeReferenceContainer(configuration);
    }

    protected FaunusVertex readFaunusVertex(final StaticBuffer key, Iterable<Entry> entries) {
        final long vertexId = IDHandler.getKeyID(key);
        Preconditions.checkArgument(vertexId > 0);
        FaunusVertex vertex = new FaunusVertex(vertexId);
        boolean isSystemType = false;
        boolean foundVertexState = false;
        for (final Entry data : entries) {
            try {
                RelationCache relation = edgeSerializer.parseRelation(vertexId, data, false, typeManager);
                TitanType type = typeManager.getExistingType(relation.typeId);
                if (type == SystemKey.TypeClass) {
                    isSystemType = true;
                } else if (type == SystemKey.VertexState) {
                    foundVertexState = true;
                }
                if (type instanceof SystemType) continue; //Ignore system types

                if (type.isPropertyKey()) {
                    assert !relation.hasProperties();
                    Object value = relation.getValue();
                    Preconditions.checkNotNull(value);
                    if (type.isUnique(Direction.OUT))
                        vertex.setProperty(type.getName(), value);
                    else
                        vertex.addProperty(type.getName(), value);
                } else {
                    assert type.isEdgeLabel();
                    FaunusEdge edge = null;
                    if (relation.direction.equals(Direction.IN))
                        edge = new FaunusEdge(relation.relationId, relation.getOtherVertexId(), vertexId, type.getName());
                    else if (relation.direction.equals(Direction.OUT))
                        edge = new FaunusEdge(relation.relationId, vertexId, relation.getOtherVertexId(), type.getName());
                    else if (relation.direction.equals(Direction.BOTH))
                        throw ExceptionFactory.bothIsNotSupported();

                    if (relation.hasProperties()) {
                        // load edge properties
                        for (LongObjectCursor<Object> next: relation) {
                            assert next.value!=null;
                            edge.setProperty(typeManager.getExistingType(next.key).getName(),next.value);
                        }
                        vertex.addEdge(relation.direction, edge);
                    }
                }

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return (isSystemType || !foundVertexState) ? null : vertex;
    }

    public void close() {

    }

}
