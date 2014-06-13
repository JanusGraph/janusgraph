package com.thinkaurelius.titan.hadoop.formats.titan;

import com.carrotsearch.hppc.cursors.LongObjectCursor;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.RelationType;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.graphdb.database.RelationReader;
import com.thinkaurelius.titan.graphdb.relations.RelationCache;
import com.thinkaurelius.titan.graphdb.types.TypeInspector;
import com.thinkaurelius.titan.hadoop.ElementState;
import com.thinkaurelius.titan.hadoop.HadoopEdge;
import com.thinkaurelius.titan.hadoop.HadoopProperty;
import com.thinkaurelius.titan.hadoop.HadoopVertex;
import com.thinkaurelius.titan.hadoop.formats.titan.input.SystemTypeInspector;
import com.thinkaurelius.titan.hadoop.formats.titan.input.TitanHadoopSetup;
import com.thinkaurelius.titan.hadoop.formats.titan.input.VertexReader;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.util.ExceptionFactory;

import org.apache.hadoop.conf.Configuration;

/**
 * The backend agnostic Titan graph reader for pulling a graph of Titan and into Hadoop.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Marko A. Rodriguez (marko@markorodriguez.com)
 */

public class TitanHadoopGraph {

    private final TitanHadoopSetup setup;
    private final TypeInspector typeManager;
    private final SystemTypeInspector systemTypes;
    private final VertexReader vertexReader;

    public TitanHadoopGraph(final TitanHadoopSetup setup) {
        this.setup = setup;
        this.typeManager = setup.getTypeInspector();
        this.systemTypes = setup.getSystemTypeInspector();
        this.vertexReader = setup.getVertexReader();
    }

    protected HadoopVertex readHadoopVertex(final Configuration configuration, final StaticBuffer key, Iterable<Entry> entries) {
        final long vertexId = this.vertexReader.getVertexId(key);
        Preconditions.checkArgument(vertexId > 0);
        HadoopVertex vertex = new HadoopVertex(configuration, vertexId);
        vertex.setState(ElementState.LOADED);
        boolean isSystemType = false;
        boolean foundVertexState = false;
        for (final Entry data : entries) {
            try {
                RelationReader relationReader = setup.getRelationReader(vertex.getIdAsLong());
                final RelationCache relation = relationReader.parseRelation(data, false, typeManager);
                if (this.systemTypes.isTypeSystemType(relation.typeId)) {
                    isSystemType = true; //TODO: We currently ignore the entire type vertex including any additional properties/edges a user might have added!
                } else if (this.systemTypes.isVertexExistsSystemType(relation.typeId)) {
                    foundVertexState = true;
                }
                if (systemTypes.isSystemType(relation.typeId)) continue; //Ignore system types

                final RelationType type = typeManager.getExistingRelationType(relation.typeId);
                if (type.isPropertyKey()) {
                    assert !relation.hasProperties();
                    Object value = relation.getValue();
                    Preconditions.checkNotNull(value);
                    final HadoopProperty p = new HadoopProperty(relation.relationId, type.getName(), value);
                    p.setState(ElementState.LOADED);
                    vertex.addProperty(p);
                } else {
                    assert type.isEdgeLabel();
                    HadoopEdge edge;
                    if (relation.direction.equals(Direction.IN))
                        edge = new HadoopEdge(configuration, relation.relationId, relation.getOtherVertexId(), vertexId, type.getName());
                    else if (relation.direction.equals(Direction.OUT))
                        edge = new HadoopEdge(configuration, relation.relationId, vertexId, relation.getOtherVertexId(), type.getName());
                    else
                        throw ExceptionFactory.bothIsNotSupported();
                    edge.setState(ElementState.LOADED);
                    if (relation.hasProperties()) {
                        // load edge properties
                        for (final LongObjectCursor<Object> next : relation) {
                            assert next.value != null;
                            edge.setProperty(typeManager.getExistingRelationType(next.key).getName(), next.value);
                        }
                        for (final HadoopProperty p : edge.getProperties())
                            p.setState(ElementState.LOADED);
                    }
                    vertex.addEdge(relation.direction, edge);
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
