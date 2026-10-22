// Copyright 2026 JanusGraph Authors
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

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ElementMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LabelStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.PropertyType;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.graphdb.query.Query;
import org.janusgraph.graphdb.tinkerpop.JanusGraphBlueprintsGraph;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphElementMapStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphHasStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphLabelStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphMultiQueryStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphPropertiesStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphPropertyMapStep;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.transaction.VertexLookup;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

/**
 * What a traversal's lookup of vertices by id ({@code g.V(ids)}, or {@code g.V().hasId(ids)}) reads of each vertex
 * along with its existence: what is read of each of them anyway right after the lookup, by the has containers which the
 * step that looks them up tests them with or else by the step which follows it, so that the lookup reads it in the
 * same backend read instead of that step reading it in another one. Beyond that it reads at most the few system
 * relations of a vertex, its label among them, which lie between its existence marker and its properties.
 * <ul>
 *     <li>The step which looks the ids up tests every vertex with the has containers folded into it
 *     ({@code g.V().hasId(ids).has(...)}) before the next step gets any, and leaves out those which fail, so where it
 *     has any, only what they read counts.</li>
 *     <li>A has container on the label reads the label. One on a property reads all properties of a vertex in a step
 *     which reads its vertices in batches of all their properties, and in a step which tests one vertex after the
 *     other where {@code query.fast-property} is on, as its first access to a property reads them all; that property
 *     alone, and the properties the next step asks for, otherwise. A has container on the label or the id beside it
 *     may leave a vertex out before its properties are read, so the lookup then leaves the properties to the step.
 *     The lookup doesn't ask the schema for the key: a test of a key the schema lacks reads nothing one vertex after
 *     the other, while the lookup reads the properties for it as for any other key.</li>
 *     <li>{@code label()} reads the label. {@code elementMap(...)} reads the label and properties, {@code valueMap(...)}
 *     properties and, where its tokens include it, the label, and {@code values(...)} and {@code properties(...)}
 *     properties: all of them without keys, or with two keys or more where the step reads its vertices in batches of
 *     all their properties ({@code query.batch.properties-mode} {@code all_properties}), and those keys alone
 *     otherwise, which the lookup leaves to the step, as it leaves the properties to a step with a limit.</li>
 *     <li>A step reads the vertices of several ids at once only where one of its batches holds all of them: the
 *     barrier in front of it lets as many vertices through as a batch holds, and without {@code query.batch.limited}
 *     the multi-query step registers all of them before the first batch. Otherwise the step may stop before the last
 *     of them at a later limit, so the lookup reads the existence alone.</li>
 * </ul>
 */
public class VertexLookupUtil {

    private VertexLookupUtil() {
    }

    /**
     * The vertices of the given ids, looked up in the transaction which {@code graph.vertices(ids)} reads in, reading
     * of each what is read of it anyway right after the lookup along with its existence.
     *
     * @param step             the step which looks the vertices up
     * @param foldedContainers the has containers which that step tests every vertex with
     */
    public static Iterator<Vertex> vertices(Step<?, ?> step, List<HasContainer> foldedContainers, Object[] ids) {
        final Graph graph = (Graph) step.getTraversal().getGraph().get();
        final Object tx = graph instanceof JanusGraphBlueprintsGraph
            ? ((JanusGraphBlueprintsGraph) graph).getCurrentThreadTx() : graph;
        if (!(tx instanceof StandardJanusGraphTx)) {
            return graph.vertices(ids);
        }
        final StandardJanusGraphTx standardTx = (StandardJanusGraphTx) tx;
        return standardTx.vertices(lookupFor(step, foldedContainers, ids.length,
            standardTx.getConfiguration().hasPropertyPrefetching()), ids);
    }

    /**
     * @param step                the step which looks the vertices up
     * @param foldedContainers    the has containers which that step tests every vertex with
     * @param ids                 how many ids it looks up
     * @param propertyPrefetching whether the first access to a property of a vertex reads all of them,
     *                            {@code query.fast-property}
     * @return what the lookup reads of each vertex along with its existence
     */
    public static VertexLookup lookupFor(Step<?, ?> step, List<HasContainer> foldedContainers, int ids,
                                         boolean propertyPrefetching) {
        //The step tests every vertex with its containers before the next step gets any, and leaves out those which fail
        if (!foldedContainers.isEmpty()) {
            return readBy(foldedContainers, false, propertyPrefetching);
        }
        //The multi-query step which registers the vertices for the batches of the step after it, the barrier which
        //collects them, and the steps which profile() adds come in between
        Step<?, ?> next = step.getNextStep();
        while (next instanceof IdentityStep || next instanceof NoOpBarrierStep
            || next instanceof JanusGraphMultiQueryStep || next instanceof ProfileStep) {
            next = next.getNextStep();
        }
        if (ids > 1 && ids > batchSizeOf(next)) {
            return VertexLookup.EXISTENCE;
        }
        return readBy(next, propertyPrefetching);
    }

    private static VertexLookup readBy(Step<?, ?> step, boolean propertyPrefetching) {
        if (step instanceof HasStep) {
            final boolean batchesOfAllProperties = step instanceof JanusGraphHasStep
                && ((JanusGraphHasStep<?>) step).isUseMultiQuery()
                && ((JanusGraphHasStep<?>) step).isPrefetchAllPropertiesRequired();
            final boolean oneAfterTheOther = !(step instanceof JanusGraphHasStep)
                || !((JanusGraphHasStep<?>) step).isUseMultiQuery();
            return readBy(((HasStep<?>) step).getHasContainers(), batchesOfAllProperties,
                oneAfterTheOther && propertyPrefetching);
        }
        if (step instanceof LabelStep) {
            return VertexLookup.LABEL;
        }
        if (step instanceof ElementMapStep) {
            return readsAllProperties(((ElementMapStep<?, ?>) step).getPropertyKeys(), step)
                ? VertexLookup.LABEL_AND_PROPERTIES : VertexLookup.LABEL;
        }
        if (step instanceof PropertyMapStep) {
            final PropertyMapStep<?, ?> map = (PropertyMapStep<?, ?>) step;
            //The properties of a property traversal are known to the traversal alone
            if (map.getPropertyTraversal() == null && readsAllProperties(map.getPropertyKeys(), step)) {
                return VertexLookup.LABEL_AND_PROPERTIES;
            }
            //The tokens go into the maps of valueMap(...) alone
            return map.getReturnType() == PropertyType.VALUE && (map.getIncludedTokens() & WithOptions.labels) != 0
                ? VertexLookup.LABEL : VertexLookup.EXISTENCE;
        }
        if (step instanceof PropertiesStep) {
            //A limit the step took over reads that many properties alone
            final boolean limited = step instanceof JanusGraphPropertiesStep
                && ((JanusGraphPropertiesStep<?>) step).getHighLimit() != Query.NO_LIMIT;
            return !limited && readsAllProperties(((PropertiesStep<?>) step).getPropertyKeys(), step)
                ? VertexLookup.LABEL_AND_PROPERTIES : VertexLookup.EXISTENCE;
        }
        return VertexLookup.EXISTENCE;
    }

    /**
     * @param batchesOfAllProperties whether the containers are tested on batches of all properties of the vertices
     * @param propertyPrefetching    whether the first access to a property of a vertex which is tested on its own reads
     *                               all of them
     */
    private static VertexLookup readBy(List<HasContainer> containers, boolean batchesOfAllProperties,
                                       boolean propertyPrefetching) {
        boolean label = false;
        boolean id = false;
        boolean property = false;
        for (final HasContainer container : containers) {
            final String key = container.getKey();
            if (T.label.getAccessor().equals(key)) {
                label = true;
            } else if (T.id.getAccessor().equals(key)) {
                id = true;
            } else if (key != null && !key.startsWith("~")) {
                //The other implicit keys, whose names start with ~ as those of the id and the label do, aren't
                //properties
                property = true;
            }
        }
        //A batch tests the ids first, then the labels, then the properties of the vertices which passed, and a test
        //of the label or the id may leave out a vertex whose properties then are never read
        if (label) {
            return VertexLookup.LABEL;
        }
        return property && !id && (batchesOfAllProperties || propertyPrefetching)
            ? VertexLookup.LABEL_AND_PROPERTIES : VertexLookup.EXISTENCE;
    }

    //All properties without keys; with keys, where the step's batches read all of them, which they do for two keys or
    //more, while they read a single key alone, see PropertiesFetchingUtil
    private static boolean readsAllProperties(String[] keys, Step<?, ?> step) {
        if (keys.length == 0) {
            return true;
        }
        final boolean batchesOfAllProperties;
        if (step instanceof JanusGraphPropertiesStep) {
            final JanusGraphPropertiesStep<?> properties = (JanusGraphPropertiesStep<?>) step;
            batchesOfAllProperties = properties.isUseMultiQuery() && properties.isPrefetchAllPropertiesRequired();
        } else if (step instanceof JanusGraphPropertyMapStep) {
            final JanusGraphPropertyMapStep<?, ?> map = (JanusGraphPropertyMapStep<?, ?>) step;
            batchesOfAllProperties = map.isUseMultiQuery() && map.isPrefetchAllPropertiesRequired();
        } else if (step instanceof JanusGraphElementMapStep) {
            final JanusGraphElementMapStep<?, ?> map = (JanusGraphElementMapStep<?, ?>) step;
            batchesOfAllProperties = map.isUseMultiQuery() && map.isPrefetchAllPropertiesRequired();
        } else {
            batchesOfAllProperties = false;
        }
        return batchesOfAllProperties
            && !PropertiesFetchingUtil.isExplicitKeysPrefetchNeeded(true, new HashSet<>(Arrays.asList(keys)));
    }

    //The most vertices one batch of the step reads, as many as the barrier in front of it lets through, or all of them
    //without query.batch.limited, where no barrier holds them back; 0 for a step which reads them one after the other
    private static int batchSizeOf(Step<?, ?> step) {
        if (step instanceof JanusGraphHasStep) {
            final JanusGraphHasStep<?> has = (JanusGraphHasStep<?>) step;
            return has.isUseMultiQuery() ? has.getBatchSize() : 0;
        }
        if (step instanceof JanusGraphPropertiesStep) {
            final JanusGraphPropertiesStep<?> properties = (JanusGraphPropertiesStep<?>) step;
            return properties.isUseMultiQuery() ? properties.getBatchSize() : 0;
        }
        if (step instanceof JanusGraphPropertyMapStep) {
            final JanusGraphPropertyMapStep<?, ?> map = (JanusGraphPropertyMapStep<?, ?>) step;
            return map.isUseMultiQuery() ? map.getBatchSize() : 0;
        }
        if (step instanceof JanusGraphElementMapStep) {
            final JanusGraphElementMapStep<?, ?> map = (JanusGraphElementMapStep<?, ?>) step;
            return map.isUseMultiQuery() ? map.getBatchSize() : 0;
        }
        if (step instanceof JanusGraphLabelStep) {
            final JanusGraphLabelStep<?> label = (JanusGraphLabelStep<?>) step;
            return label.isUseMultiQuery() ? label.getBatchSize() : 0;
        }
        return 0;
    }
}
