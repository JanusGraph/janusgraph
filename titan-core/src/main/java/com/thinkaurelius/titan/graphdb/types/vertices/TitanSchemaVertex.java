package com.thinkaurelius.titan.graphdb.types.vertices;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.TitanVertexQuery;
import com.thinkaurelius.titan.graphdb.internal.TitanSchemaCategory;
import com.thinkaurelius.titan.graphdb.query.vertex.VertexCentricQueryBuilder;
import com.thinkaurelius.titan.graphdb.transaction.RelationConstructor;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.*;
import com.thinkaurelius.titan.graphdb.types.indextype.ExternalIndexTypeWrapper;
import com.thinkaurelius.titan.graphdb.types.indextype.InternalIndexTypeWrapper;
import com.thinkaurelius.titan.graphdb.types.system.BaseKey;
import com.thinkaurelius.titan.graphdb.types.system.BaseLabel;
import com.thinkaurelius.titan.graphdb.vertices.CacheVertex;
import com.tinkerpop.blueprints.Direction;

import javax.annotation.Nullable;

public class TitanSchemaVertex extends CacheVertex implements SchemaSource {

    public TitanSchemaVertex(StandardTitanTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
    }

    private String name = null;

    @Override
    public String getName() {
        if (name == null) {
            TitanProperty p;
            if (isLoaded()) {
                StandardTitanTx tx = tx();
                p = (TitanProperty) Iterables.getOnlyElement(RelationConstructor.readRelation(this,
                                            tx.getGraph().getSchemaCache().getSchemaRelations(getID(), BaseKey.SchemaName, Direction.OUT, tx()),
                                            tx), null);
            } else {
                p = Iterables.getOnlyElement(query().type(BaseKey.SchemaName).properties(), null);
            }
            Preconditions.checkState(p!=null,"Could not find type for id: %s",getID());
            name = p.getValue();
        }
        assert name != null;
        return TitanSchemaCategory.getName(name);
    }

    private TypeDefinitionMap definition = null;

    @Override
    public TypeDefinitionMap getDefinition() {
        TypeDefinitionMap def = definition;
        if (def == null) {
            def = new TypeDefinitionMap();
            Iterable<TitanProperty> ps;
            if (isLoaded()) {
                StandardTitanTx tx = tx();
                ps = (Iterable)RelationConstructor.readRelation(this,
                        tx.getGraph().getSchemaCache().getSchemaRelations(getID(), BaseKey.SchemaDefinitionProperty, Direction.OUT, tx()),
                        tx);
            } else {
                ps = query().type(BaseKey.SchemaDefinitionProperty).properties();
            }
            for (TitanProperty property : ps) {
                TypeDefinitionDescription desc = property.getProperty(BaseKey.SchemaDefinitionDesc);
                Preconditions.checkArgument(desc!=null && desc.getCategory().isProperty());
                def.setValue(desc.getCategory(), property.getValue());
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
            Iterable<TitanEdge> edges;
            if (isLoaded()) {
                StandardTitanTx tx = tx();
                edges = (Iterable)RelationConstructor.readRelation(this,
                        tx.getGraph().getSchemaCache().getSchemaRelations(getID(), BaseLabel.SchemaDefinitionEdge, dir, tx()),
                        tx);
            } else {
                edges = query().type(BaseLabel.SchemaDefinitionEdge).direction(dir).titanEdges();
            }
            for (TitanEdge edge: edges) {
                TitanVertex oth = edge.getVertex(dir.opposite());
                assert oth instanceof TitanSchemaVertex;
                TypeDefinitionDescription desc = edge.getProperty(BaseKey.SchemaDefinitionDesc);
                Object modifier = null;
                if (desc.getCategory().hasDataType()) {
                    assert desc.getModifier()!=null && desc.getModifier().getClass().equals(desc.getCategory().getDataType());
                    modifier = desc.getModifier();
                }
                b.put(desc.getCategory(), new Entry((TitanSchemaVertex) oth, modifier));
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
     * This is needed when the type gets modified in the {@link com.thinkaurelius.titan.graphdb.database.management.ManagementSystem}.
     */
    public void resetCache() {
        definition=null;
        outRelations=null;
        inRelations=null;
    }

    public Iterable<TitanEdge> getEdges(final TypeDefinitionCategory def, final Direction dir) {
        return getEdges(def,dir,null);
    }

    public Iterable<TitanEdge> getEdges(final TypeDefinitionCategory def, final Direction dir, TitanSchemaVertex other) {
        TitanVertexQuery query = query().type(BaseLabel.SchemaDefinitionEdge).direction(dir);
        if (other!=null) query.adjacent(other);
        return Iterables.filter(query.titanEdges(),new Predicate<TitanEdge>() {
            @Override
            public boolean apply(@Nullable TitanEdge edge) {
                TypeDefinitionDescription desc = edge.getProperty(BaseKey.SchemaDefinitionDesc);
                return desc.getCategory()==def;
            }
        });
    }

    @Override
    public String toString() {
        return getName();
    }

    @Override
    public SchemaStatus getStatus() {
        return getDefinition().getValue(TypeDefinitionCategory.STATUS,SchemaStatus.class);
    }

    @Override
    public IndexType asIndexType() {
        Preconditions.checkArgument(getDefinition().containsKey(TypeDefinitionCategory.INTERNAL_INDEX),"Schema vertex is not a type vertex: [%s,%s]",getID(),getName());
        if (getDefinition().getValue(TypeDefinitionCategory.INTERNAL_INDEX)) {
            return new InternalIndexTypeWrapper(this);
        } else {
            return new ExternalIndexTypeWrapper(this);
        }
    }

}
