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

import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.mixed.utils.MixedIndexUtilsConfigOptions;

/**
 * Circle processor implementing the same logic as ElasticSearch Server side
 * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ingest-circle-processor.html">Circle Processor</a>
 * but on the JanusGraph side. <br>
 * Error distance can be specified using {@link  MixedIndexUtilsConfigOptions#BKD_FIXED_CIRCLE_PROCESSOR_ERROR_DISTANCE} configuration.
 */
public class FixedErrorDistanceCircleProcessor extends ErrorDistanceCircleProcessor {

    public static final String SHORTHAND = "fixedErrorDistance";

    private final double errorDistanceMeters;

    public FixedErrorDistanceCircleProcessor(Configuration config) {
        super(config.get(MixedIndexUtilsConfigOptions.BKD_FIXED_CIRCLE_PROCESSOR_BOUNDING_BOX_FALLBACK));
        this.errorDistanceMeters = config.get(MixedIndexUtilsConfigOptions.BKD_FIXED_CIRCLE_PROCESSOR_ERROR_DISTANCE);
    }

    @Override
    double getErrorDistanceMeters(Geoshape circle) {
        return errorDistanceMeters;
    }
}
