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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class VertexCentricQueryOptimizer implements QueryOptimizer<VertexCentricQuery> {

    private static final Logger log = LoggerFactory.getLogger(VertexCentricQueryOptimizer.class);


    public static final VertexCentricQueryOptimizer INSTANCE = new VertexCentricQueryOptimizer();

    private VertexCentricQueryOptimizer() {}

    @Override
    public List<VertexCentricQuery> optimize(VertexCentricQuery query) {
        Preconditions.checkNotNull(query);
        if (query.isInvalid()) return ImmutableList.of();

        List<VertexCentricQuery> result = null;
        //1. Split types
        List<VertexCentricQuery> newResult = Lists.newArrayList();
        if (query.hasType() && !query.hasGroup()) {
            log.trace("Splitting query [{}] on type w/o group",query);
            for (int i=0;i<query.getTypes().length;i++) {
                TitanType t = query.getTypes()[i];
                Preconditions.checkArgument(!(t instanceof TitanKey) || query.getReturnType()== RelationType.PROPERTY);
                Preconditions.checkArgument(!(t instanceof TitanLabel) || query.getReturnType()== RelationType.EDGE);

                if (((InternalType)t).isHidden() && !query.isIncludeHidden()) continue;

                newResult.add(new VertexCentricQuery(query.getVertex(), query.getDirection(), new TitanType[]{t}, null,
                        query.getConstraints(), query.isIncludeHidden(), query.getLimit(), query.getReturnType()));
            }
        } else {
            if (query.hasType() && query.hasGroup()) {
                log.trace("Splitting query [{}] on type with group",query);
                List<TitanType> filteredTypes = Lists.newArrayList();
                for (int i=0;i<query.numberTypes();i++) {
                    TitanType t = query.getTypes()[i];
                    Preconditions.checkArgument(t.getGroup().equals(query.getGroup()));
                    filteredTypes.add(t);
                }
                newResult.add(new VertexCentricQuery(query.getVertex(), query.getDirection(),
                        filteredTypes.toArray(new TitanType[filteredTypes.size()]), query.getGroup(),
                        query.getConstraints(), query.isIncludeHidden(), query.getLimit(), query.getReturnType()));
            } else newResult.add(query);
        }
        result=newResult;

        if (result.size()>1 || (!result.isEmpty() && result.get(0).hasGroup())) {
            //2. Split groups by return type
            log.trace("Splitting query [{}] on return type",query);
            newResult = Lists.newArrayList();
            for (VertexCentricQuery q : result) {
                if (q.hasGroup() && q.getReturnType()== RelationType.RELATION) {
                    for (RelationType rt : new RelationType[]{RelationType.EDGE, RelationType.PROPERTY}) {
                        newResult.add(new VertexCentricQuery(q.getVertex(), q.getDirection(), q.getTypes(), q.getGroup(),
                                q.getConstraints(), q.isIncludeHidden(), q.getLimit(), rt));
                    }
                } else newResult.add(q);
            }
            result=newResult;
        }

        if (result.size()>1 || (!result.isEmpty() && !result.get(0).hasSingleDirection())) {
            //3. Split Directions
            newResult = Lists.newArrayList();
            for (VertexCentricQuery q : result) {
                if (!q.hasSingleDirection() && (q.hasType() || q.hasGroup()) ) {
                    log.trace("Splitting sub-query [{}] on direction",q);
                    Preconditions.checkArgument(q.getDirection()==Direction.BOTH);
                    Preconditions.checkArgument(q.getReturnType()!= RelationType.PROPERTY);
                    for (Direction d : EdgeDirection.PROPER_DIRS)
                        newResult.add(new VertexCentricQuery(q.getVertex(),d,q.getTypes(),q.getGroup(),
                                q.getConstraints(),q.isIncludeHidden(),q.getLimit(),q.getReturnType()));
                } else newResult.add(q);
            }
            result=newResult;
        }

        return result;
    }


}
