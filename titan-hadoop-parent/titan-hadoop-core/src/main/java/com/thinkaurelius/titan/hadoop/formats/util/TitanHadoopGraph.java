package com.thinkaurelius.titan.hadoop.formats.util;

import com.carrotsearch.hppc.cursors.LongObjectCursor;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.graphdb.database.RelationReader;
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.relations.RelationCache;
import com.thinkaurelius.titan.graphdb.types.TypeInspector;
import com.thinkaurelius.titan.hadoop.*;
import com.thinkaurelius.titan.hadoop.formats.util.input.SystemTypeInspector;
import com.thinkaurelius.titan.hadoop.formats.util.input.TitanHadoopSetup;
import com.thinkaurelius.titan.hadoop.formats.util.input.VertexReader;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final boolean verifyVertexExistence = false;

    private static final Logger log =
            LoggerFactory.getLogger(TitanHadoopGraph.class);

    public TitanHadoopGraph(final TitanHadoopSetup setup) {
        this.setup = setup;
        this.typeManager = setup.getTypeInspector();
        this.systemTypes = setup.getSystemTypeInspector();
        this.vertexReader = setup.getVertexReader();
    }

    protected FaunusVertex readHadoopVertex(final Configuration configuration, final StaticBuffer key, Iterable<Entry> entries) {
        final long vertexId = this.vertexReader.getVertexId(key);
        Preconditions.checkArgument(vertexId > 0);
        FaunusVertex vertex = new FaunusVertex(configuration, vertexId);
        boolean foundVertexState = !verifyVertexExistence;
        for (final Entry data : entries) {
            try {
                RelationReader relationReader = setup.getRelationReader(vertex.getLongId());
                final RelationCache relation = relationReader.parseRelation(data, false, typeManager);
                if (this.systemTypes.isVertexExistsSystemType(relation.typeId)) {
                    foundVertexState = true;
                } else if (this.systemTypes.isVertexLabelSystemType(relation.typeId)) {
                    //Vertex Label
                    long vertexLabelId = relation.getOtherVertexId();
                    VertexLabel vl = typeManager.getExistingVertexLabel(vertexLabelId);
                    vertex.setVertexLabel(vertex.getTypeManager().getVertexLabel(vl.getName()));
                }
                if (systemTypes.isSystemType(relation.typeId)) continue; //Ignore system types

                final RelationType type = typeManager.getExistingRelationType(relation.typeId);
                if (((InternalRelationType)type).isHiddenType()) continue; //Ignore hidden types

                StandardFaunusRelation frel;
                if (type.isPropertyKey()) {
                    Object value = relation.getValue();
                    Preconditions.checkNotNull(value);
                    final StandardFaunusProperty fprop = new StandardFaunusProperty(relation.relationId, vertex, type.getName(), value);
                    vertex.addProperty(fprop);
                    frel = fprop;
                } else {
                    assert type.isEdgeLabel();
                    StandardFaunusEdge fedge;
                    if (relation.direction.equals(Direction.IN))
                        fedge = new StandardFaunusEdge(configuration, relation.relationId, relation.getOtherVertexId(), vertexId, type.getName());
                    else if (relation.direction.equals(Direction.OUT))
                        fedge = new StandardFaunusEdge(configuration, relation.relationId, vertexId, relation.getOtherVertexId(), type.getName());
                    else
                        throw ExceptionFactory.bothIsNotSupported();
                    vertex.addEdge(fedge);
                    frel = fedge;
                }
                if (relation.hasProperties()) {
                    // load relation properties
                    for (final LongObjectCursor<Object> next : relation) {
                        assert next.value != null;
                        RelationType rt = typeManager.getExistingRelationType(next.key);
                        if (rt.isPropertyKey()) {
                            PropertyKey pkey = (PropertyKey)vertex.getTypeManager().getPropertyKey(rt.getName());
                            log.debug("Retrieved key {} for name \"{}\"", pkey, rt.getName());
                            frel.setProperty(pkey, next.value);
                        } else {
                            assert next.value instanceof Long;
                            EdgeLabel el = (EdgeLabel)vertex.getTypeManager().getEdgeLabel(rt.getName());
                            log.debug("Retrieved ege label {} for name \"{}\"", el, rt.getName());
                            frel.setProperty(el, new FaunusVertex(configuration,(Long)next.value));
                        }
                    }
                    for (TitanRelation rel : frel.query().queryAll().relations())
                        ((FaunusRelation)rel).setLifeCycle(ElementLifeCycle.Loaded);
                }
                frel.setLifeCycle(ElementLifeCycle.Loaded);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        vertex.setLifeCycle(ElementLifeCycle.Loaded);

        /*Since we are filtering out system relation types, we might end up with vertices that have no incident relations.
         This is especially true for schema vertices. Those are filtered out.     */
        if (!foundVertexState || !vertex.query().relations().iterator().hasNext()) return null;
        return vertex;
    }

    public void close() {
        setup.close();
    }

}
