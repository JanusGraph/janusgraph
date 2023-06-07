// Copyright 2023 JanusGraph Authors
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

package org.janusgraph.graphdb.tinkerpop.optimize.step.util;

import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.janusgraph.core.BaseVertexQuery;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.janusgraph.graphdb.query.vertex.BasicVertexCentricQueryBuilder;

import java.util.Iterator;
import java.util.Set;

public class PropertiesFetchingUtil {

    public static boolean isExplicitKeysPrefetchNeeded(boolean prefetchAllPropertiesRequired, Set<String> propertyKeysSet){
        return !prefetchAllPropertiesRequired || propertyKeysSet.size() < 2;
    }

    public static <Q extends BaseVertexQuery> Q makeBasePropertiesQuery(Q query,
                                                                        boolean prefetchAllPropertiesRequired,
                                                                        Set<String> propertyKeysSet,
                                                                        String[] propertyKeys,
                                                                        QueryProfiler queryProfiler) {
        if(PropertiesFetchingUtil.isExplicitKeysPrefetchNeeded(prefetchAllPropertiesRequired, propertyKeysSet)){
            query.keys(propertyKeys);
        }
        ((BasicVertexCentricQueryBuilder) query).profiler(queryProfiler);
        return query;
    }

    public static Iterator<? extends Property> filterPropertiesIfNeeded(Iterator<? extends Property> propertiesIt, boolean prefetchAllPropertiesRequired, Set<String> propertyKeysSet){
        return isExplicitKeysPrefetchNeeded(prefetchAllPropertiesRequired, propertyKeysSet) ? propertiesIt : Iterators.filter(propertiesIt, property -> propertyKeysSet.contains(property.key()));
    }
}
