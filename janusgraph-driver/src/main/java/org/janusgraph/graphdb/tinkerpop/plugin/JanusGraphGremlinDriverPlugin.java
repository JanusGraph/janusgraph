// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.graphdb.tinkerpop.plugin;

import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.Customizer;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.attribute.Contain;
import org.janusgraph.core.attribute.Geo;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.JtsGeoshapeHelper;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Clock;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Stream;

public class JanusGraphGremlinDriverPlugin extends AbstractGremlinPlugin {
    private static final String NAME = "janusgraph-driver.imports";

    private static final Set<Class> CLASS_IMPORTS = new LinkedHashSet<>();
    private static final Set<Enum> ENUM_IMPORTS = new LinkedHashSet<>();
    private static final Set<Method> METHOD_IMPORTS = new LinkedHashSet<>();

    static {
        /////////////
        // CLASSES //
        /////////////

        CLASS_IMPORTS.add(Cmp.class);
        CLASS_IMPORTS.add(Contain.class);
        CLASS_IMPORTS.add(Geo.class);
        CLASS_IMPORTS.add(Geoshape.class);
        CLASS_IMPORTS.add(JtsGeoshapeHelper.class);
        CLASS_IMPORTS.add(Text.class);

        CLASS_IMPORTS.add(JanusGraphIoRegistry.class);

        CLASS_IMPORTS.add(Instant.class);
        CLASS_IMPORTS.add(Clock.class);
        CLASS_IMPORTS.add(DayOfWeek.class);
        CLASS_IMPORTS.add(Duration.class);
        CLASS_IMPORTS.add(LocalDate.class);
        CLASS_IMPORTS.add(LocalTime.class);
        CLASS_IMPORTS.add(LocalDateTime.class);
        CLASS_IMPORTS.add(Month.class);
        CLASS_IMPORTS.add(MonthDay.class);
        CLASS_IMPORTS.add(OffsetDateTime.class);
        CLASS_IMPORTS.add(OffsetTime.class);
        CLASS_IMPORTS.add(Period.class);
        CLASS_IMPORTS.add(Year.class);
        CLASS_IMPORTS.add(YearMonth.class);
        CLASS_IMPORTS.add(ZonedDateTime.class);
        CLASS_IMPORTS.add(ZoneId.class);
        CLASS_IMPORTS.add(ZoneOffset.class);
        CLASS_IMPORTS.add(ChronoUnit.class);

        ///////////
        // ENUMS //
        ///////////

        // also make sure the class is imported for these enums

        Collections.addAll(ENUM_IMPORTS, ChronoUnit.values());

        /////////////
        // METHODS //
        /////////////

        // only include the static predicates
        // also make sure the class is imported for these methods

        Stream.of(Geo.values())
            .map(v -> {
                try {
                    return Geo.class.getMethod(v.toString(), Object.class);
                } catch (NoSuchMethodException e) {
                    throw new RuntimeException(e);
                }
            })
            .filter(JanusGraphGremlinDriverPlugin::isMethodStatic)
            .forEach(METHOD_IMPORTS::add);

        Stream.of(Text.values())
            .map(v -> {
                try {
                    return Text.class.getMethod(v.toString(), Object.class);
                } catch (NoSuchMethodException e) {
                    throw new RuntimeException(e);
                }
            })
            .filter(JanusGraphGremlinDriverPlugin::isMethodStatic)
            .forEach(METHOD_IMPORTS::add);
    }

    private static final JanusGraphGremlinDriverPlugin instance = new JanusGraphGremlinDriverPlugin();

    public JanusGraphGremlinDriverPlugin() {
        this(NAME, DefaultImportCustomizer.build());
    }

    public JanusGraphGremlinDriverPlugin(String name, DefaultImportCustomizer.Builder build) {
        super(name, build.addClassImports(CLASS_IMPORTS).addEnumImports(ENUM_IMPORTS).addMethodImports(METHOD_IMPORTS).create());
    }

    public static JanusGraphGremlinDriverPlugin instance() {
        return instance;
    }

    public JanusGraphGremlinDriverPlugin(String moduleName, Customizer... customizers) {
        super(moduleName, customizers);
    }

    @Override
    public boolean requireRestart() {
        return true;
    }

    private static boolean isMethodStatic(final Method method) {
        return Modifier.isStatic(method.getModifiers());
    }
}
