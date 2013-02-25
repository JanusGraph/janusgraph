package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.tinkerpop.blueprints.Direction;

import java.util.List;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class VertexCentricQueryOptimizer implements QueryOptimizer<VertexCentricQuery> {

    public static final VertexCentricQueryOptimizer INSTANCE = new VertexCentricQueryOptimizer();

    private static final double LIMIT_BUFFER = 1.1;

    private VertexCentricQueryOptimizer() {}

    @Override
    public List<VertexCentricQuery> optimize(VertexCentricQuery query) {
        Preconditions.checkNotNull(query);
        if (query.isInvalid()) return ImmutableList.of();

        List<VertexCentricQuery> result = null;
        //1. Split types
        List<VertexCentricQuery> newResult = Lists.newArrayList();
        if (query.hasType() && !query.hasGroup()) {
            for (int i=0;i<query.getTypes().length;i++) {
                TitanType t = query.getTypes()[i];
                Preconditions.checkArgument(!(t instanceof TitanKey) || query.getReturnType()== RelationType.PROPERTY);
                Preconditions.checkArgument(!(t instanceof TitanLabel) || query.getReturnType()== RelationType.EDGE);

                if (t.isNew()) continue; //Filter out new types
                if (((InternalType)t).isHidden() && !query.isIncludeHidden()) continue;

                newResult.add(new VertexCentricQuery(query.getVertex(), query.getDirection(), new TitanType[]{t}, null,
                        query.getConstraints(), query.isIncludeHidden(), query.getLimit(), query.getReturnType()));
            }
        } else {
            if (query.hasType() && query.hasGroup()) {
                List<TitanType> filteredTypes = Lists.newArrayList();
                for (int i=0;i<query.numberTypes();i++) {
                    TitanType t = query.getTypes()[i];
                    Preconditions.checkArgument(t.getGroup().equals(query.getGroup()));
                    if (!t.isNew()) filteredTypes.add(t);
                }
                newResult.add(new VertexCentricQuery(query.getVertex(), query.getDirection(),
                        filteredTypes.toArray(new TitanType[filteredTypes.size()]), query.getGroup(),
                        query.getConstraints(), query.isIncludeHidden(), query.getLimit(), query.getReturnType()));
            } else newResult.add(query);
        }
        result=newResult;

        if (result.size()>1 || result.get(0).hasGroup()) {
            //2. Split groups by return type
            newResult = Lists.newArrayList();
            for (VertexCentricQuery q : result) {
                if (q.hasGroup() && q.getReturnType()== RelationType.RELATION) {
                    for (RelationType rt : new RelationType[]{RelationType.EDGE, RelationType.PROPERTY}) {
                        newResult.add(new VertexCentricQuery(q.getVertex(), q.getDirection(), q.getTypes(), q.getGroup(),
                                q.getConstraints(), q.isIncludeHidden(), q.getLimit(), rt));
                    }
                }
            }
            result=newResult;
        }

        if (result.size()>1 || !result.get(0).hasSingleDirection()) {
            //3. Split Directions
            newResult = Lists.newArrayList();
            for (VertexCentricQuery q : result) {
                if (!q.hasSingleDirection() && (q.hasType() || q.hasGroup()) ) {
                    Preconditions.checkArgument(q.getDirection()==Direction.BOTH);
                    Preconditions.checkArgument(q.getReturnType()!= RelationType.PROPERTY);
                    for (Direction d : EdgeDirection.PROPER_DIRS)
                        newResult.add(new VertexCentricQuery(q.getVertex(),d,q.getTypes(),q.getGroup(),
                                q.getConstraints(),q.isIncludeHidden(),q.getLimit(),q.getReturnType()));
                } else newResult.add(q);
            }
            result=newResult;
        }
        if (result.size()>1 || result.get(0).hasLimit()) {
            //4. Update Limits
            newResult = Lists.newArrayList();
            for (VertexCentricQuery q : result) {
                if (q.hasLimit()) {
                    int newLimit = (int)Math.min(Integer.MAX_VALUE-1,Math.round(q.getLimit()*LIMIT_BUFFER));
                    newResult.add(new VertexCentricQuery(q,newLimit));
                } else newResult.add(q);
            }
            result=newResult;
        }
        return result;
    }


}
