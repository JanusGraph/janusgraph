// Copyright 2020 JanusGraph Authors
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

package org.janusgraph.graphdb.query.index;

import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.internal.OrderList;
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.query.condition.MultiCondition;
import org.janusgraph.graphdb.types.IndexType;

import java.util.Set;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.QUERY_NS;

/**
 * @author Florian Grieskamp (Florian.Grieskamp@gdata.de)
 */
public class ThresholdBasedIndexSelectionStrategy
    extends AbstractIndexSelectionStrategy {

    public static final String NAME = "threshold-based";

    private final int threshold;

    public static final ConfigOption<Integer> INDEX_SELECT_BRUTE_FORCE_THRESHOLD = new ConfigOption<>(QUERY_NS, "index-select-threshold",
        "Threshold of deciding whether to use brute force enumeration algorithm or fast approximation algorithm " +
            "for selecting suitable indexes. Selecting optimal indexes for a query is a NP-complete set cover problem. " +
            "When number of suitable index candidates is no larger than threshold, JanusGraph uses brute force search " +
            "with exponential time complexity to ensure the best combination of indexes is selected. Only effective when `" +
            NAME + "` index select strategy is chosen.",
        ConfigOption.Type.MASKABLE, 10);


    private final IndexSelectionStrategy usedIfLessOrEqualThreshold;
    private final IndexSelectionStrategy usedIfGreaterThanThreshold;

    public ThresholdBasedIndexSelectionStrategy(Configuration config) {
        super(config);
        this.threshold = config.get(INDEX_SELECT_BRUTE_FORCE_THRESHOLD);
        this.usedIfLessOrEqualThreshold = new BruteForceIndexSelectionStrategy(config);
        this.usedIfGreaterThanThreshold = new ApproximateIndexSelectionStrategy(config);
    }

    @Override
    public SelectedIndexQuery selectIndices(final Set<IndexType> indexCandidates,
                                            final MultiCondition<JanusGraphElement> conditions,
                                            final Set<Condition> coveredClauses, OrderList orders,
                                            IndexSerializer serializer) {
        /* TODO: smarter optimization:
        - use in-memory histograms to estimate selectivity of PredicateConditions and filter out
        low-selectivity ones if they would result in an individual index call (better to filter
        afterwards in memory)
        */
        IndexSelectionStrategy preferredStrategy = indexCandidates.size() <= threshold
            ? usedIfLessOrEqualThreshold
            : usedIfGreaterThanThreshold;
        return preferredStrategy.selectIndices(indexCandidates, conditions, coveredClauses, orders,
            serializer);
    }
}
