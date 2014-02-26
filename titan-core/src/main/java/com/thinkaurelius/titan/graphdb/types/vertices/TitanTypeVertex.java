package com.thinkaurelius.titan.graphdb.types.vertices;

import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.thinkaurelius.titan.graphdb.transaction.RelationConstructor;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.TypeDefinitionCategory;
import com.thinkaurelius.titan.graphdb.types.TypeDefinitionDescription;
import com.thinkaurelius.titan.graphdb.types.TypeDefinitionMap;
import com.thinkaurelius.titan.graphdb.types.TypeSource;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import com.thinkaurelius.titan.graphdb.types.system.SystemLabel;
import com.thinkaurelius.titan.graphdb.vertices.CacheVertex;
import com.tinkerpop.blueprints.Direction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TitanTypeVertex extends CacheVertex implements TypeSource {

    public TitanTypeVertex(StandardTitanTx tx, long id, byte lifecycle) {
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
                assert oth instanceof TitanTypeVertex;
                TypeDefinitionDescription desc = edge.getProperty(SystemKey.TypeDefinitionDesc);
                Object modifier = null;
                if (def.hasDataType()) {
                    assert desc.getModifier()!=null && desc.getModifier().getClass().equals(def.getDataType());
                    modifier = desc.getModifier();
                }
                b.put(desc.getCategory(),new Entry((TitanTypeVertex)oth,modifier));
            }
            if (dir==Direction.OUT) outRelations=b.build();
            else inRelations=b.build();
        }
        assert getRelations(dir)!=null;
        return getRelations(dir).get(def);
    }

    @Override
    public String toString() {
        return getName();
    }





}
