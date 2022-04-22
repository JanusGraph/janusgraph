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

package org.janusgraph.diskstorage.mixed.utils;

import org.apache.commons.lang3.ClassUtils;
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.mixed.utils.processor.CircleProcessor;
import org.janusgraph.diskstorage.mixed.utils.processor.DynamicErrorDistanceCircleProcessor;
import org.janusgraph.diskstorage.mixed.utils.processor.FixedErrorDistanceCircleProcessor;
import org.janusgraph.diskstorage.mixed.utils.processor.NoTransformCircleProcessor;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;
import org.janusgraph.util.system.ConfigurationUtil;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@PreInitializeConfigOptions
public class MixedIndexUtilsConfigOptions {

    public static final ConfigNamespace BKD_CIRCLE_PROCESSOR_NS =
        new ConfigNamespace(GraphDatabaseConfiguration.INDEX_NS, "bkd-circle-processor",
            "Configuration for BKD circle processors which is used for BKD Geoshape mapping.");

    public static final Map<String, String> PREREGISTERED_CIRCLE_PROCESSORS = Collections.unmodifiableMap(
        new HashMap<String, String>(3) {{
            put(NoTransformCircleProcessor.SHORTHAND, NoTransformCircleProcessor.class.getName());
            put(FixedErrorDistanceCircleProcessor.SHORTHAND, FixedErrorDistanceCircleProcessor.class.getName());
            put(DynamicErrorDistanceCircleProcessor.SHORTHAND, DynamicErrorDistanceCircleProcessor.class.getName());
        }}
    );

    public static final ConfigNamespace BKD_FIXED_CIRCLE_PROCESSOR_NS =
        new ConfigNamespace(BKD_CIRCLE_PROCESSOR_NS, "fixed",
            "Configuration for Elasticsearch fixed circle processor which is used for BKD Geoshape mapping.");

    public static final ConfigOption<Double> BKD_FIXED_CIRCLE_PROCESSOR_ERROR_DISTANCE =
        new ConfigOption<>(BKD_FIXED_CIRCLE_PROCESSOR_NS, "error-distance",
            "The difference between the resulting inscribed distance from center to side and the circle’s radius. " +
                "Specified in meters.",
            ConfigOption.Type.MASKABLE, 10d, e -> e > 0d);

    private static final String BOUNDING_BOX_FALLBACK_NAME = "bounding-box-fallback";
    private static final String BOUNDING_BOX_FALLBACK_DESCRIPTION = "Allows to return bounding box for the circle which " +
        "cannot be converted to proper shape with the specified error distance. In case `false` is set for this configuration " +
        "an exception will be thrown whenever circle cannot be converted to another shape following error distance.";

    public static final ConfigOption<Boolean> BKD_FIXED_CIRCLE_PROCESSOR_BOUNDING_BOX_FALLBACK =
        new ConfigOption<>(BKD_FIXED_CIRCLE_PROCESSOR_NS, BOUNDING_BOX_FALLBACK_NAME,
            BOUNDING_BOX_FALLBACK_DESCRIPTION,
            ConfigOption.Type.MASKABLE, true);

    public static final ConfigNamespace BKD_DYNAMIC_CIRCLE_PROCESSOR_NS =
        new ConfigNamespace(BKD_CIRCLE_PROCESSOR_NS, "dynamic",
            "Configuration for Elasticsearch dynamic circle processor which is used for BKD Geoshape mapping.");

    public static final ConfigOption<Double> BKD_DYNAMIC_CIRCLE_PROCESSOR_MULTIPLIER =
        new ConfigOption<>(BKD_DYNAMIC_CIRCLE_PROCESSOR_NS, "error-distance-multiplier",
            "Multiplier variable for dynamic error distance calculation in the formula `log(radius) * multiplier`. " +
                "Radius and error distance specified in meters.",
            ConfigOption.Type.MASKABLE, 2d, e -> e > 0d);

    public static final ConfigOption<Boolean> BKD_DYNAMIC_CIRCLE_PROCESSOR_BOUNDING_BOX_FALLBACK =
        new ConfigOption<>(BKD_DYNAMIC_CIRCLE_PROCESSOR_NS, BOUNDING_BOX_FALLBACK_NAME,
            BOUNDING_BOX_FALLBACK_DESCRIPTION,
            ConfigOption.Type.MASKABLE, true);

    public static final ConfigOption<String> BKD_CIRCLE_PROCESSOR_CLASS =
        new ConfigOption<>(BKD_CIRCLE_PROCESSOR_NS, "class",
            "Full class name of circle processor that implements `CircleProcessor` interface. " +
                "The class is used for transformation of a Circle shape to another shape when BKD mapping is used. " +
                "The provided implementation class should have either a public constructor which accepts configuration " +
                "as a parameter (`org.janusgraph.diskstorage.configuration.Configuration`) or a public constructor with no parameters. " +
                "Usually the transforming shape is a Polygon. <br>" +
                "Following shorthands can be used: <br> " +
                "- `"+NoTransformCircleProcessor.SHORTHAND+"` Circle processor which is not transforming a circle, but instead keep the circle shape unchanged. " +
                "This implementation may be useful in situations when the user wants to control circle transformation logic on ElasticSearch " +
                "side instead of application side. For example, using " +
                "<a href=\"https://www.elastic.co/guide/en/elasticsearch/reference/current/ingest-circle-processor.html\">ElasticSearch Circle Processor</a> " +
                "or any custom plugin. <br> " +
                "- `"+FixedErrorDistanceCircleProcessor.SHORTHAND+"` Circle processor which transforms the provided Circle into Polygon, Box, or Point depending on the configuration provided in `"
                +BKD_FIXED_CIRCLE_PROCESSOR_NS.toStringWithoutRoot()+"`. The processing logic is similar to <a href=\"https://www.elastic.co/guide/en/elasticsearch/reference/current/ingest-circle-processor.html\">ElasticSearch Circle Processor</a> " +
                "except for some edge cases when the Circle is transformed into Box or Point. <br> " +
                "- `"+DynamicErrorDistanceCircleProcessor.SHORTHAND+"` Circle processor which calculates error distance dynamically depending on the circle radius and the specified multiplier value. " +
                "The error distance calculation formula is `log(radius) * multiplier`. Configuration for this class can be provided via `" +
                BKD_DYNAMIC_CIRCLE_PROCESSOR_NS.toStringWithoutRoot()+"`. The processing logic is similar to <a href=\"https://www.elastic.co/guide/en/elasticsearch/reference/current/ingest-circle-processor.html\">ElasticSearch Circle Processor</a> " +
                "except for some edge cases when the Circle is transformed into Box or Point.",
            ConfigOption.Type.MASKABLE, DynamicErrorDistanceCircleProcessor.SHORTHAND, className -> {
            if (className == null) return false;
            if (PREREGISTERED_CIRCLE_PROCESSORS.containsKey(className)) return true;
            try {
                Class<?> clazz = ClassUtils.getClass(className);
                return CircleProcessor.class.isAssignableFrom(clazz);
            } catch (ClassNotFoundException e) {
                return false;
            }
        });

    public static CircleProcessor buildBKDCircleProcessor(Configuration config){
        String circleProcessorClassName = config.get(BKD_CIRCLE_PROCESSOR_CLASS);
        if(PREREGISTERED_CIRCLE_PROCESSORS.containsKey(circleProcessorClassName)){
            circleProcessorClassName = PREREGISTERED_CIRCLE_PROCESSORS.get(circleProcessorClassName);
        }

        if(ConfigurationUtil.hasConstructor(circleProcessorClassName, new Class[]{Configuration.class})){
            return ConfigurationUtil.instantiate(circleProcessorClassName, new Object[]{config}, new Class[]{Configuration.class});
        }

        return ConfigurationUtil.instantiate(circleProcessorClassName);
    }
}
