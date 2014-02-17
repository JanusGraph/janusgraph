package com.thinkaurelius.titan.graphdb.types.system;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.core.Titan;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.graphdb.internal.RelationCategory;
import com.thinkaurelius.titan.graphdb.internal.TitanTypeCategory;
import com.thinkaurelius.titan.graphdb.types.TypeAttribute;
import com.thinkaurelius.titan.util.datastructures.IterablesUtil;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

import java.util.Map;

public class SystemLabel extends SystemType implements TitanLabel {

    public static final SystemLabel TypeRelatedTo =
            new SystemLabel("TypeRelated", 6);

    private SystemLabel(String name, int id) {
        super(name, id, RelationCategory.EDGE, new boolean[]{false,false}, true);
    }

    @Override
    public long[] getSignature() {
        return new long[]{SystemKey.TypeRelationClassifier.getID()};
    }

    @Override
    public final boolean isPropertyKey() {
        return false;
    }

    @Override
    public final boolean isEdgeLabel() {
        return true;
    }

    @Override
    public boolean isDirected() {
        return true;
    }

    @Override
    public boolean isUnidirected() {
        return false;
    }
}
