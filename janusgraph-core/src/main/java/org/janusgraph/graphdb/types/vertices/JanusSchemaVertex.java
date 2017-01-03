package org.janusgraph.graphdb.types.vertices;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import org.janusgraph.core.JanusEdge;
import org.janusgraph.core.JanusVertexProperty;
import org.janusgraph.core.JanusVertex;
import org.janusgraph.core.JanusVertexQuery;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.graphdb.internal.JanusSchemaCategory;
import org.janusgraph.graphdb.transaction.RelationConstructor;
import org.janusgraph.graphdb.transaction.StandardJanusTx;
import org.janusgraph.graphdb.types.*;
import org.janusgraph.graphdb.types.indextype.CompositeIndexTypeWrapper;
import org.janusgraph.graphdb.types.indextype.MixedIndexTypeWrapper;
import org.janusgraph.graphdb.types.system.BaseKey;
import org.janusgraph.graphdb.types.system.BaseLabel;
import org.janusgraph.graphdb.vertices.CacheVertex;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import javax.annotation.Nullable;

public class JanusSchemaVertex extends CacheVertex implements SchemaSource {

    public JanusSchemaVertex(StandardJanusTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
    }

    private String name = null;

    @Override
    public String name() {
        if (name == null) {
            JanusVertexProperty<String> p;
            if (isLoaded()) {
                StandardJanusTx tx = tx();
                p = (JanusVertexProperty) Iterables.getOnlyElement(RelationConstructor.readRelation(this,
                        tx.getGraph().getSchemaCache().getSchemaRelations(longId(), BaseKey.SchemaName, Direction.OUT),
                        tx), null);
            } else {
                p = Iterables.getOnlyElement(query().type(BaseKey.SchemaName).properties(), null);
            }
            Preconditions.checkState(p!=null,"Could not find type for id: %s", longId());
            name = p.value();
        }
        assert name != null;
        return JanusSchemaCategory.getName(name);
    }

    @Override
    protected Vertex getVertexLabelInternal() {
        return null;
    }

    private TypeDefinitionMap definition = null;

    @Override
    public TypeDefinitionMap getDefinition() {
        TypeDefinitionMap def = definition;
        if (def == null) {
            def = new TypeDefinitionMap();
            Iterable<JanusVertexProperty> ps;
            if (isLoaded()) {
                StandardJanusTx tx = tx();
                ps = (Iterable)RelationConstructor.readRelation(this,
                        tx.getGraph().getSchemaCache().getSchemaRelations(longId(), BaseKey.SchemaDefinitionProperty, Direction.OUT),
                        tx);
            } else {
                ps = query().type(BaseKey.SchemaDefinitionProperty).properties();
            }
            for (JanusVertexProperty property : ps) {
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

    private ListMultimap<TypeDefinitionCategory,Entry> outRelations = null;
    private ListMultimap<TypeDefinitionCategory,Entry> inRelations = null;


    @Override
    public Iterable<Entry> getRelated(TypeDefinitionCategory def, Direction dir) {
        assert dir==Direction.OUT || dir==Direction.IN;
        ListMultimap<TypeDefinitionCategory,Entry> rels = dir==Direction.OUT?outRelations:inRelations;
        if (rels==null) {
            ImmutableListMultimap.Builder<TypeDefinitionCategory,Entry> b = ImmutableListMultimap.builder();
            Iterable<JanusEdge> edges;
            if (isLoaded()) {
                StandardJanusTx tx = tx();
                edges = (Iterable)RelationConstructor.readRelation(this,
                        tx.getGraph().getSchemaCache().getSchemaRelations(longId(), BaseLabel.SchemaDefinitionEdge, dir),
                        tx);
            } else {
                edges = query().type(BaseLabel.SchemaDefinitionEdge).direction(dir).edges();
            }
            for (JanusEdge edge: edges) {
                JanusVertex oth = edge.vertex(dir.opposite());
                assert oth instanceof JanusSchemaVertex;
                TypeDefinitionDescription desc = edge.valueOrNull(BaseKey.SchemaDefinitionDesc);
                Object modifier = null;
                if (desc.getCategory().hasDataType()) {
                    assert desc.getModifier()!=null && desc.getModifier().getClass().equals(desc.getCategory().getDataType());
                    modifier = desc.getModifier();
                }
                b.put(desc.getCategory(), new Entry((JanusSchemaVertex) oth, modifier));
            }
            rels = b.build();
            if (dir==Direction.OUT) outRelations=rels;
            else inRelations=rels;
        }
        assert rels!=null;
        return rels.get(def);
    }

    /**
     * Resets the internal caches used to speed up lookups on this index type.
     * This is needed when the type gets modified in the {@link org.janusgraph.graphdb.database.management.ManagementSystem}.
     */
    public void resetCache() {
        name = null;
        definition=null;
        outRelations=null;
        inRelations=null;
    }

    public Iterable<JanusEdge> getEdges(final TypeDefinitionCategory def, final Direction dir) {
        return getEdges(def,dir,null);
    }

    public Iterable<JanusEdge> getEdges(final TypeDefinitionCategory def, final Direction dir, JanusSchemaVertex other) {
        JanusVertexQuery query = query().type(BaseLabel.SchemaDefinitionEdge).direction(dir);
        if (other!=null) query.adjacent(other);
        return Iterables.filter(query.edges(),new Predicate<JanusEdge>() {
            @Override
            public boolean apply(@Nullable JanusEdge edge) {
                TypeDefinitionDescription desc = edge.valueOrNull(BaseKey.SchemaDefinitionDesc);
                return desc.getCategory()==def;
            }
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
        if (getDefinition().getValue(TypeDefinitionCategory.INTERNAL_INDEX)) {
            return new CompositeIndexTypeWrapper(this);
        } else {
            return new MixedIndexTypeWrapper(this);
        }
    }

}
