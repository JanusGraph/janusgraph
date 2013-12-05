package com.thinkaurelius.faunus.formats.titan;

import com.carrotsearch.hppc.cursors.LongObjectCursor;
import com.google.common.base.Preconditions;
import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusProperty;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.graphdb.database.EdgeSerializer;
import com.thinkaurelius.titan.graphdb.database.RelationReader;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.relations.RelationCache;
import com.thinkaurelius.titan.graphdb.types.TypeInspector;
import com.thinkaurelius.titan.graphdb.types.reference.TypeReferenceContainer;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import com.thinkaurelius.titan.graphdb.types.system.SystemType;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.util.ExceptionFactory;

/**
 * The backend agnostic Titan graph reader for pulling a graph of Titan and into Faunus.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Marko A. Rodriguez (marko@markorodriguez.com)
 */

public class FaunusTitanGraph {

    private final RelationReader relationReader;
    private final TypeInspector typeManager;

    public FaunusTitanGraph(final RelationReader relationReader, final TypeInspector types) {
        this.relationReader = relationReader;
        typeManager = types;
    }

    protected FaunusVertex readFaunusVertex(final StaticBuffer key, Iterable<Entry> entries) {
        final long vertexId = IDHandler.getKeyID(key);
        Preconditions.checkArgument(vertexId > 0);
        FaunusVertex vertex = new FaunusVertex(vertexId);
        boolean isSystemType = false;
        boolean foundVertexState = false;
        for (final Entry data : entries) {
            try {
                RelationCache relation = relationReader.parseRelation(vertexId, data, false, typeManager);
                TitanType type = typeManager.getExistingType(relation.typeId);
                if (type == SystemKey.TypeClass) {
                    isSystemType = true; //TODO: We currently ignore the entire type vertex including any additional properties/edges a user might have added!
                } else if (type == SystemKey.VertexState) {
                    foundVertexState = true;
                }
                if (type instanceof SystemType) continue; //Ignore system types

                if (type.isPropertyKey()) {
                    assert !relation.hasProperties();
                    Object value = relation.getValue();
                    Preconditions.checkNotNull(value);
                    vertex.addProperty(new FaunusProperty(relation.relationId,type.getName(),value));
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
