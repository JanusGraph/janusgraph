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

package org.janusgraph.graphdb.tinkerpop.optimize.step.service;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphPropertiesStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.NoOpBarrierVertexOnlyStep;

import java.util.Iterator;

public class TraversalPropertiesFetchingService implements PropertiesFetchingService {

    private final JanusGraphPropertiesStep propertiesStep;
    private final Traversal.Admin<Element, ? extends Property> propertyTraversal;

    public TraversalPropertiesFetchingService(Traversal.Admin<Element, ? extends Property> propertyTraversal, int batchSize, boolean prefetchAllPropertiesRequired) {
        this.propertyTraversal = propertyTraversal;

        Step step = propertyTraversal.getStartStep();
        while (step instanceof IdentityStep ||
            step instanceof ProfileStep ||
            step instanceof NoOpBarrierStep ||
            step instanceof NoOpBarrierVertexOnlyStep ||
            step instanceof SideEffectStep){
            step = step.getNextStep();
        }

        if(step instanceof PropertiesStep){
            final PropertiesStep originalStep = (PropertiesStep) step;
            JanusGraphPropertiesStep propertiesStep;
            if(originalStep instanceof JanusGraphPropertiesStep){
                propertiesStep = (JanusGraphPropertiesStep) originalStep;
            } else {
                propertiesStep = new JanusGraphPropertiesStep(originalStep, prefetchAllPropertiesRequired, true);
                TraversalHelper.replaceStep(originalStep, propertiesStep, originalStep.getTraversal());
            }
            propertiesStep.setUseMultiQuery(true);
            propertiesStep.setBatchSize(batchSize);
            this.propertiesStep = propertiesStep;
        } else {
            this.propertiesStep = null;
        }
    }

    @Override
    public Iterator<? extends Property> fetchProperties(Traverser.Admin<Element> traverser, Traversal.Admin<?, ?> traversal) {
        return TraversalUtil.applyAll(traverser, this.propertyTraversal);
    }

    @Override
    public void registerFirstNewLoopFutureVertexForPrefetching(Vertex futureVertex, int futureVertexTraverserLoop) {
        if(propertiesStep != null){
            propertiesStep.registerFirstNewLoopFutureVertexForPrefetching(futureVertex, futureVertexTraverserLoop);
        }
    }

    @Override
    public void registerSameLoopFutureVertexForPrefetching(Vertex futureVertex, int futureVertexTraverserLoop) {
        if(propertiesStep != null){
            propertiesStep.registerSameLoopFutureVertexForPrefetching(futureVertex, futureVertexTraverserLoop);
        }
    }

    @Override
    public void registerNextLoopFutureVertexForPrefetching(Vertex futureVertex, int futureVertexTraverserLoop) {
        if(propertiesStep != null){
            propertiesStep.registerNextLoopFutureVertexForPrefetching(futureVertex, futureVertexTraverserLoop);
        }
    }

    @Override
    public void setMetrics(MutableMetrics metrics) {
        if(propertiesStep != null){
            propertiesStep.setMetrics(metrics);
        }
    }

    @Override
    public void setBatchSize(int batchSize){
        if(propertiesStep != null){
            propertiesStep.setBatchSize(batchSize);
        }
    }
}
