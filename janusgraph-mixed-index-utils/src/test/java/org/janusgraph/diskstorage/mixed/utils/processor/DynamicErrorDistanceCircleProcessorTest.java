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

package org.janusgraph.diskstorage.mixed.utils.processor;

import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.mixed.utils.MixedIndexUtilsConfigOptions;
import org.mockito.Mockito;

public class DynamicErrorDistanceCircleProcessorTest extends ErrorDistanceCircleProcessorTest {

    public DynamicErrorDistanceCircleProcessorTest() {
        super(processorWithBoundingBoxFallbackAndNormalErrorDistance(),
            processorWithoutBoundingBoxFallbackAndNormalErrorDistance(),
            processorWithBoundingBoxFallbackAndSmallErrorDistance(),
            processorWithoutBoundingBoxFallbackAndSmallErrorDistance());
    }

    private static DynamicErrorDistanceCircleProcessor processorWithBoundingBoxFallbackAndNormalErrorDistance(){
        Configuration config = Mockito.mock(Configuration.class);
        Mockito.when(config.get(MixedIndexUtilsConfigOptions.BKD_DYNAMIC_CIRCLE_PROCESSOR_MULTIPLIER))
            .thenReturn(2d);
        Mockito.when(config.get(MixedIndexUtilsConfigOptions.BKD_DYNAMIC_CIRCLE_PROCESSOR_BOUNDING_BOX_FALLBACK))
            .thenReturn(true);

        return new DynamicErrorDistanceCircleProcessor(config);
    }

    private static DynamicErrorDistanceCircleProcessor processorWithoutBoundingBoxFallbackAndNormalErrorDistance(){
        Configuration config = Mockito.mock(Configuration.class);
        Mockito.when(config.get(MixedIndexUtilsConfigOptions.BKD_DYNAMIC_CIRCLE_PROCESSOR_MULTIPLIER))
            .thenReturn(2d);
        Mockito.when(config.get(MixedIndexUtilsConfigOptions.BKD_DYNAMIC_CIRCLE_PROCESSOR_BOUNDING_BOX_FALLBACK))
            .thenReturn(false);

        return new DynamicErrorDistanceCircleProcessor(config);
    }

    private static DynamicErrorDistanceCircleProcessor processorWithBoundingBoxFallbackAndSmallErrorDistance(){
        Configuration config = Mockito.mock(Configuration.class);
        Mockito.when(config.get(MixedIndexUtilsConfigOptions.BKD_DYNAMIC_CIRCLE_PROCESSOR_MULTIPLIER))
            .thenReturn(0.0000001d);
        Mockito.when(config.get(MixedIndexUtilsConfigOptions.BKD_DYNAMIC_CIRCLE_PROCESSOR_BOUNDING_BOX_FALLBACK))
            .thenReturn(true);

        return new DynamicErrorDistanceCircleProcessor(config);
    }

    private static DynamicErrorDistanceCircleProcessor processorWithoutBoundingBoxFallbackAndSmallErrorDistance(){
        Configuration config = Mockito.mock(Configuration.class);
        Mockito.when(config.get(MixedIndexUtilsConfigOptions.BKD_DYNAMIC_CIRCLE_PROCESSOR_MULTIPLIER))
            .thenReturn(0.0000001d);
        Mockito.when(config.get(MixedIndexUtilsConfigOptions.BKD_DYNAMIC_CIRCLE_PROCESSOR_BOUNDING_BOX_FALLBACK))
            .thenReturn(false);

        return new DynamicErrorDistanceCircleProcessor(config);
    }
}
