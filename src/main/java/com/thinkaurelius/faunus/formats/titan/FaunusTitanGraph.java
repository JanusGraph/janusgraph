package com.thinkaurelius.faunus.formats.titan;

import com.carrotsearch.hppc.cursors.LongObjectCursor;
import com.google.common.base.Preconditions;
import com.thinkaurelius.faunus.ElementState;
import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusElement;
import com.thinkaurelius.faunus.FaunusProperty;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.formats.titan.input.SystemTypeInspector;
import com.thinkaurelius.faunus.formats.titan.input.TitanFaunusSetup;
import com.thinkaurelius.faunus.formats.titan.input.VertexReader;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.graphdb.database.RelationReader;
import com.thinkaurelius.titan.graphdb.relations.RelationCache;
import com.thinkaurelius.titan.graphdb.types.TypeInspector;
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
    private final SystemTypeInspector systemTypes;
    private final VertexReader vertexReader;

    public FaunusTitanGraph(final TitanFaunusSetup setup) {
        this.relationReader = setup.getRelationReader();
        this.typeManager = setup.getTypeInspector();
        this.systemTypes = setup.getSystemTypeInspector();
        this.vertexReader = setup.getVertexReader();
    }

    protected FaunusVertex readFaunusVertex(final StaticBuffer key, Iterable<Entry> entries) {
        final long vertexId = vertexReader.getVertexId(key);
        Preconditions.checkArgument(vertexId > 0);
        FaunusVertex vertex = new FaunusVertex(FaunusElement.EMPTY_CONFIGURATION, vertexId);
        vertex.setState(ElementState.LOADED);
        boolean isSystemType = false;
        boolean foundVertexState = false;
        for (final Entry data : entries) {
            try {
                RelationCache relation = relationReader.parseRelation(vertexId, data, false, typeManager);
                if (systemTypes.isTypeSystemType(relation.typeId)) {
                    isSystemType = true; //TODO: We currently ignore the entire type vertex including any additional properties/edges a user might have added!
                } else if (systemTypes.isVertexExistsSystemType(relation.typeId)) {
                    foundVertexState = true;
                }
                if (systemTypes.isSystemType(relation.typeId)) continue; //Ignore system types

                TitanType type = typeManager.getExistingType(relation.typeId);
                if (type.isPropertyKey()) {
                    assert !relation.hasProperties();
                    Object value = relation.getValue();
                    Preconditions.checkNotNull(value);
                    FaunusProperty p = new FaunusProperty(relation.relationId, type.getName(), value);
                    p.setState(ElementState.LOADED);
                    vertex.addProperty(p);
                } else {
                    assert type.isEdgeLabel();
                    FaunusEdge edge = null;
                    if (relation.direction.equals(Direction.IN))
                        edge = new FaunusEdge(FaunusElement.EMPTY_CONFIGURATION, relation.relationId, relation.getOtherVertexId(), vertexId, type.getName());
                    else if (relation.direction.equals(Direction.OUT))
                        edge = new FaunusEdge(FaunusElement.EMPTY_CONFIGURATION, relation.relationId, vertexId, relation.getOtherVertexId(), type.getName());
                    else if (relation.direction.equals(Direction.BOTH))
                        throw ExceptionFactory.bothIsNotSupported();
                    edge.setState(ElementState.LOADED);
                    if (relation.hasProperties()) {
                        // load edge properties
                        for (LongObjectCursor<Object> next : relation) {
                            assert next.value != null;
                            edge.setProperty(typeManager.getExistingType(next.key).getName(), next.value);
                        }
                        for (FaunusProperty p : edge.getProperties()) p.setState(ElementState.LOADED);
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
