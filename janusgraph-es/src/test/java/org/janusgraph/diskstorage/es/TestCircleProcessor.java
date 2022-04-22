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

package org.janusgraph.diskstorage.es;

import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.mixed.utils.processor.CircleProcessor;
import org.janusgraph.diskstorage.mixed.utils.processor.FixedErrorDistanceCircleProcessor;

import java.util.function.Consumer;

public class TestCircleProcessor implements CircleProcessor {

    private static Consumer<Geoshape> preProcessConsumer;
    private static Consumer<Geoshape> postProcessConsumer;

    private final CircleProcessor wrappedCircleProcessor;

    public TestCircleProcessor(Configuration config) {
        wrappedCircleProcessor = new FixedErrorDistanceCircleProcessor(config);
    }

    @Override
    public Geoshape process(Geoshape circle) {
        if(preProcessConsumer != null){
            preProcessConsumer.accept(circle);
        }
        Geoshape transformedGeoshape = wrappedCircleProcessor.process(circle);
        if(postProcessConsumer != null){
            postProcessConsumer.accept(transformedGeoshape);
        }
        return transformedGeoshape;
    }

    public static void setPreProcessConsumer(Consumer<Geoshape> preProcessConsumer){
        TestCircleProcessor.preProcessConsumer = preProcessConsumer;
    }

    public static void setPostProcessConsumer(Consumer<Geoshape> postProcessConsumer){
        TestCircleProcessor.postProcessConsumer = postProcessConsumer;
    }
}
