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

import com.google.common.annotations.VisibleForTesting;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class JanusGraphSchemaVertex extends CacheVertex implements SchemaSource {

    public JanusGraphSchemaVertex(StandardJanusGraphTx tx, Object id, byte lifecycle) {
        super(tx, id, lifecycle);
    }

    //The caches below, and those of subclasses, are filled on first read and cleared by resetCache() when the schema
    //element changes, which another thread may do while this transaction reads: a commit expires the element in every
    //open transaction. A read counts the resets before it loads and keeps what it loaded only if none came in between,
    //since the schema cache entries it loaded may predate the change; see keepUnlessReset. The fields are volatile so
    //that a thread which shares the transaction never sees a reference before what it points to is complete, and so
    //that the store of a value is ordered before the check of the reset count which follows it.
    private final AtomicLong resets = new AtomicLong();
    private volatile String name = null;
    private volatile TypeDefinitionMap definition = null;
    private volatile ListMultimap<TypeDefinitionCategory,Entry> outRelations = null;
    private volatile ListMultimap<TypeDefinitionCategory,Entry> inRelations = null;

    @Override
    public long longId() {
        return ((Number) id()).longValue();
    }

    @Override
    public String name() {
        String result = name;
        if (result == null) {
            final long resetsBefore = cacheResets();
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
            result = JanusGraphSchemaCategory.getName(p.value());
            keepUnlessReset(resetsBefore, result, value -> name = value);
        }
        assert result != null;
        return result;
    }

    @Override
    protected Vertex getVertexLabelInternal() {
        return null;
    }

    /**
     * Whether the schema vertex has a definition left to read. One which a schema change removed has none, and
     * {@link #getDefinition()} cannot be called on it.
     */
    public boolean hasDefinition() {
        if (definition != null) return true;
        if (isLoaded()) {
            StandardJanusGraphTx tx = tx();
            return !tx.getGraph().getSchemaCache().getSchemaRelations(longId(), BaseKey.SchemaDefinitionProperty,
                Direction.OUT).isEmpty();
        }
        return !Iterables.isEmpty(query().type(BaseKey.SchemaDefinitionProperty).properties());
    }

    @Override
    public TypeDefinitionMap getDefinition() {
        TypeDefinitionMap def = definition;
        if (def == null) {
            final long resetsBefore = cacheResets();
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
            keepUnlessReset(resetsBefore, def, value -> definition = value);
        }
        assert def!=null;
        return def;
    }

    @Override
    public List<Entry> getRelated(TypeDefinitionCategory def, Direction dir) {
        assert dir==Direction.OUT || dir==Direction.IN;
        ListMultimap<TypeDefinitionCategory,Entry> relations = dir==Direction.OUT?outRelations:inRelations;
        if (relations==null) {
            final long resetsBefore = cacheResets();
            relations = loadRelated(dir);
            if (dir==Direction.OUT) keepUnlessReset(resetsBefore, relations, value -> outRelations = value);
            else keepUnlessReset(resetsBefore, relations, value -> inRelations = value);
        }
        assert relations!=null;
        return relations.get(def);
    }

    //Package-private and not final so that a test can override it and pause a load
    @VisibleForTesting
    ListMultimap<TypeDefinitionCategory,Entry> loadRelated(Direction dir) {
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
        return b.build();
    }

    /**
     * How many times the caches have been reset: read before a cache value is loaded, and passed to
     * {@link #keepUnlessReset}.
     */
    protected final long cacheResets() {
        return resets.get();
    }

    /**
     * Stores a loaded cache value unless the caches were reset after {@code resetsBefore} was read, and clears the
     * field again if a reset came in while the value was being stored. A value a reset overlapped is still returned to
     * the read which loaded it, which began before the reset, but a later read loads the value again.
     */
    protected final <V> void keepUnlessReset(long resetsBefore, V value, Consumer<V> field) {
        if (resets.get() != resetsBefore) return;
        field.accept(value);
        if (resets.get() != resetsBefore) field.accept(null);
    }

    /**
     * Resets the caches which speed the lookups on this schema vertex up, after a change to the schema element: by the
     * {@link org.janusgraph.graphdb.database.management.ManagementSystem}, or by an ordinary transaction which wrote
     * definition edges.
     */
    @Override
    public void resetCache() {
        //Counted before anything is cleared, so that a read which overlaps the reset does not keep what it loaded
        resets.incrementAndGet();
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
