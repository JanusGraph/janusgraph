package com.thinkaurelius.titan.graphdb.types.vertices;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.query.vertex.VertexCentricQueryBuilder;
import com.thinkaurelius.titan.graphdb.transaction.RelationConstructor;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.*;
import com.thinkaurelius.titan.graphdb.types.indextype.ExternalIndexTypeWrapper;
import com.thinkaurelius.titan.graphdb.types.indextype.InternalIndexTypeWrapper;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import com.thinkaurelius.titan.graphdb.types.system.SystemLabel;
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
                                            tx.getGraph().getSchemaCache().getTypeRelations(getID(), SystemKey.TypeName, Direction.OUT, tx()),
                                            tx), null);
            } else {
                p = Iterables.getOnlyElement(query().type(SystemKey.TypeName).properties(), null);
            }
            Preconditions.checkState(p!=null,"Could not find type for id: %s",getID());
            name = p.getValue(String.class);
        }
        assert name != null;
        return name;
    }

    private TypeDefinitionMap definition = null;

    @Override
    public TypeDefinitionMap getDefinition() {
        if (definition == null) {
            TypeDefinitionMap def = new TypeDefinitionMap();
            Iterable<TitanProperty> ps;
            if (isLoaded()) {
                StandardTitanTx tx = tx();
                ps = (Iterable)RelationConstructor.readRelation(this,
                        tx.getGraph().getSchemaCache().getTypeRelations(getID(), SystemKey.TypeDefinitionProperty, Direction.OUT, tx()),
                        tx);
            } else {
                ps = query().type(SystemKey.TypeDefinitionProperty).properties();
            }
            for (TitanProperty property : ps) {
                TypeDefinitionDescription desc = property.getProperty(SystemKey.TypeDefinitionDesc);
                Preconditions.checkArgument(desc!=null && desc.getCategory().isProperty());
                def.setValue(desc.getCategory(), property.getValue());
            }
            assert def.size()>0;
            definition = def;
        }
        return definition;
    }

    private ListMultimap<TypeDefinitionCategory,Entry> outRelations = null;
    private ListMultimap<TypeDefinitionCategory,Entry> inRelations = null;

    private ListMultimap<TypeDefinitionCategory,Entry> getRelations(Direction dir) {
        assert dir==Direction.OUT || dir==Direction.IN;
        return dir==Direction.OUT?outRelations:inRelations;
    }

    @Override
    public Iterable<Entry> getRelated(TypeDefinitionCategory def, Direction dir) {
        if (getRelations(dir)==null) {
            ImmutableListMultimap.Builder<TypeDefinitionCategory,Entry> b = ImmutableListMultimap.builder();
            Iterable<TitanEdge> edges;
            if (isLoaded()) {
                StandardTitanTx tx = tx();
                edges = (Iterable)RelationConstructor.readRelation(this,
                        tx.getGraph().getSchemaCache().getTypeRelations(getID(), SystemLabel.TypeDefinitionEdge, dir, tx()),
                        tx);
            } else {
                edges = query().type(SystemLabel.TypeDefinitionEdge).direction(dir).titanEdges();
            }
            for (TitanEdge edge: edges) {
                TitanVertex oth = edge.getVertex(dir.opposite());
                assert oth instanceof TitanSchemaVertex;
                TypeDefinitionDescription desc = edge.getProperty(SystemKey.TypeDefinitionDesc);
                Object modifier = null;
                if (def.hasDataType()) {
                    assert desc.getModifier()!=null && desc.getModifier().getClass().equals(def.getDataType());
                    modifier = desc.getModifier();
                }
                b.put(desc.getCategory(), new Entry((TitanSchemaVertex) oth, modifier));
            }
            if (dir==Direction.OUT) outRelations=b.build();
            else inRelations=b.build();
        }
        assert getRelations(dir)!=null;
        return getRelations(dir).get(def);
    }

    public Iterable<TitanEdge> getEdges(final TypeDefinitionCategory def, final Direction dir) {
        return getEdges(def,dir,null);
    }

    public Iterable<TitanEdge> getEdges(final TypeDefinitionCategory def, final Direction dir, TitanSchemaVertex other) {
        VertexCentricQueryBuilder query = query().type(SystemLabel.TypeDefinitionEdge).direction(dir);
        if (other!=null) query.adjacentVertex(other);
        return Iterables.filter(query.titanEdges(),new Predicate<TitanEdge>() {
            @Override
            public boolean apply(@Nullable TitanEdge edge) {
                TypeDefinitionDescription desc = edge.getProperty(SystemKey.TypeDefinitionDesc);
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
