// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.types.vertices;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.JanusGraphVertexQuery;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.graphdb.internal.JanusGraphSchemaCategory;
import org.janusgraph.graphdb.transaction.RelationConstructor;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.IndexType;
import org.janusgraph.graphdb.types.SchemaSource;
import org.janusgraph.graphdb.types.TypeDefinitionCategory;
import org.janusgraph.graphdb.types.TypeDefinitionDescription;
import org.janusgraph.graphdb.types.TypeDefinitionMap;
import org.janusgraph.graphdb.types.indextype.CompositeIndexTypeWrapper;
import org.janusgraph.graphdb.types.indextype.MixedIndexTypeWrapper;
import org.janusgraph.graphdb.types.system.BaseKey;
import org.janusgraph.graphdb.types.system.BaseLabel;
import org.janusgraph.graphdb.vertices.CacheVertex;

import java.util.List;

public class JanusGraphSchemaVertex extends CacheVertex implements SchemaSource {

    public JanusGraphSchemaVertex(StandardJanusGraphTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
    }

    private String name = null;
    private TypeDefinitionMap definition = null;
    private ListMultimap<TypeDefinitionCategory,Entry> outRelations = null;
    private ListMultimap<TypeDefinitionCategory,Entry> inRelations = null;

    @Override
    public String name() {
        if (name == null) {
            JanusGraphVertexProperty<String> p;
            if (isLoaded()) {
                StandardJanusGraphTx tx = tx();
                p = (JanusGraphVertexProperty) Iterables.getOnlyElement(RelationConstructor.readRelation(this,
                        tx.getGraph().getSchemaCache().getSchemaRelations(longId(), BaseKey.SchemaName, Direction.OUT),
                        tx), null);
            } else {
                p = Iterables.getOnlyElement(query().type(BaseKey.SchemaName).properties(), null);
            }
            Preconditions.checkNotNull(p,"Could not find type for id: %s", longId());
            name = JanusGraphSchemaCategory.getName(p.value());
        }
        assert name != null;
        return name;
    }

    @Override
    protected Vertex getVertexLabelInternal() {
        return null;
    }

    @Override
    public TypeDefinitionMap getDefinition() {
        TypeDefinitionMap def = definition;
        if (def == null) {
            def = new TypeDefinitionMap();
            Iterable<JanusGraphVertexProperty> ps;
            if (isLoaded()) {
                StandardJanusGraphTx tx = tx();
                ps = (Iterable)RelationConstructor.readRelation(this,
                        tx.getGraph().getSchemaCache().getSchemaRelations(longId(), BaseKey.SchemaDefinitionProperty, Direction.OUT),
                        tx);
            } else {
                ps = query().type(BaseKey.SchemaDefinitionProperty).properties();
            }
            for (JanusGraphVertexProperty property : ps) {
                TypeDefinitionDescription desc = property.valueOrNull(BaseKey.SchemaDefinitionDesc);
                Preconditions.checkArgument(desc!=null && desc.getCategory().isProperty());
                def.setValue(desc.getCategory(), property.value());
            }
            assert def.size()>0;
            definition = def;
        }
        assert def!=null;
        return def;
    }

    @Override
    public List<Entry> getRelated(TypeDefinitionCategory def, Direction dir) {
        assert dir==Direction.OUT || dir==Direction.IN;
        ListMultimap<TypeDefinitionCategory,Entry> relations = dir==Direction.OUT?outRelations:inRelations;
        if (relations==null) {
            ImmutableListMultimap.Builder<TypeDefinitionCategory,Entry> b = ImmutableListMultimap.builder();
            Iterable<JanusGraphEdge> edges;
            if (isLoaded()) {
                StandardJanusGraphTx tx = tx();
                edges = (Iterable)RelationConstructor.readRelation(this,
                        tx.getGraph().getSchemaCache().getSchemaRelations(longId(), BaseLabel.SchemaDefinitionEdge, dir),
                        tx);
            } else {
                edges = query().type(BaseLabel.SchemaDefinitionEdge).direction(dir).edges();
            }
            for (JanusGraphEdge edge: edges) {
                JanusGraphVertex oth = edge.vertex(dir.opposite());
                assert oth instanceof JanusGraphSchemaVertex;
                TypeDefinitionDescription desc = edge.valueOrNull(BaseKey.SchemaDefinitionDesc);
                Object modifier = null;
                if (desc.getCategory().hasDataType()) {
                    assert desc.getModifier()!=null && desc.getModifier().getClass().equals(desc.getCategory().getDataType());
                    modifier = desc.getModifier();
                }
                b.put(desc.getCategory(), new Entry((JanusGraphSchemaVertex) oth, modifier));
            }
            relations = b.build();
            if (dir==Direction.OUT) outRelations=relations;
            else inRelations=relations;
        }
        assert relations!=null;
        return relations.get(def);
    }

    /**
     * Resets the internal caches used to speed up lookups on this index type.
     * This is needed when the type gets modified in the {@link org.janusgraph.graphdb.database.management.ManagementSystem}.
     */
    @Override
    public void resetCache() {
        name = null;
        definition=null;
        outRelations=null;
        inRelations=null;
    }

    public Iterable<JanusGraphEdge> getEdges(final TypeDefinitionCategory def, final Direction dir) {
        return getEdges(def,dir,null);
    }

    public Iterable<JanusGraphEdge> getEdges(final TypeDefinitionCategory def, final Direction dir, JanusGraphSchemaVertex other) {
        JanusGraphVertexQuery query = query().type(BaseLabel.SchemaDefinitionEdge).direction(dir);
        if (other!=null) query.adjacent(other);
        return Iterables.filter(query.edges(), (Predicate<JanusGraphEdge>) edge -> {
            final TypeDefinitionDescription desc = edge.valueOrNull(BaseKey.SchemaDefinitionDesc);
            return desc.getCategory()==def;
        });
    }

    @Override
    public String toString() {
        return name();
    }

    @Override
    public SchemaStatus getStatus() {
        return getDefinition().getValue(TypeDefinitionCategory.STATUS,SchemaStatus.class);
    }

    @Override
    public IndexType asIndexType() {
        Preconditions.checkArgument(getDefinition().containsKey(TypeDefinitionCategory.INTERNAL_INDEX),"Schema vertex is not a type vertex: [%s,%s]", longId(), name());
        return getDefinition().<Boolean>getValue(TypeDefinitionCategory.INTERNAL_INDEX) ?
            new CompositeIndexTypeWrapper(this) :
            new MixedIndexTypeWrapper(this);
    }

}
