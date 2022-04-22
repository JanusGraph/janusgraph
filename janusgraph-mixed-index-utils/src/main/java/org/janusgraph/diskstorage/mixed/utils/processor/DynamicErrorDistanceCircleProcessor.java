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
 * Circle processor which calculates error distance dynamically depending on the circle radius and the specified
 * multiplier value. The error distance calculation formula is `log(radius) * multiplier` <br>
 * Error distance multiplier can be specified using {@link  MixedIndexUtilsConfigOptions#BKD_DYNAMIC_CIRCLE_PROCESSOR_MULTIPLIER} configuration.
 */
public class DynamicErrorDistanceCircleProcessor extends ErrorDistanceCircleProcessor {

    public static final String SHORTHAND = "dynamicErrorDistance";

    private final double errorDistanceMultiplier;

    public DynamicErrorDistanceCircleProcessor(Configuration config) {
        super(config.get(MixedIndexUtilsConfigOptions.BKD_DYNAMIC_CIRCLE_PROCESSOR_BOUNDING_BOX_FALLBACK));
        errorDistanceMultiplier = config.get(MixedIndexUtilsConfigOptions.BKD_DYNAMIC_CIRCLE_PROCESSOR_MULTIPLIER);
    }

    @Override
    double getErrorDistanceMeters(Geoshape circle) {
        double radius = circle.getRadiusMeters();
        double errorDistance = (radius <= 2d ? 1d : Math.log(radius)) * errorDistanceMultiplier;
        return errorDistance <= 0d ? Double.MIN_VALUE : errorDistance;
    }
}
