// Copyright 2017 JanusGraph Authors
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

package org.janusgraph.core.util;

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.util.time.Timer;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;
import org.reflections8.Reflections;
import org.reflections8.scanners.SubTypesScanner;
import org.reflections8.scanners.TypeAnnotationsScanner;
import org.reflections8.vfs.Vfs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * This class supports iteration over JanusGraph's ConfigNamespaces at runtime.
 *
 * JanusGraph's ConfigOptions and ConfigNamespaces are defined by public static final fields
 * spread across more than ten classes in various janusgraph modules/jars.  A ConfigOption
 * effectively does not exist at runtime until the static initializer of the field in
 * which it is defined is executed by the JVM.  This class contains utility methods
 * internally called by JanusGraph to preload ConfigOptions when performing lookups or
 * iterations in which a ConfigOption might not necessarily be loaded yet (such as
 * when iterating over the collection of ConfigOption children in a ConfigNamespace).
 * Normally, only JanusGraph internals should use this class.
 */
public enum ReflectiveConfigOptionLoader {
    INSTANCE;

    private static final String SYS_PROP_NAME = "janusgraph.load.cfg.opts";
    private static final String ENV_VAR_NAME  = "JANUSGRAPH_LOAD_CFG_OPTS";

    private static final Logger log =
            LoggerFactory.getLogger(ReflectiveConfigOptionLoader.class);

    private volatile LoaderConfiguration cfg = new LoaderConfiguration();

    public ReflectiveConfigOptionLoader setUseThreadContextLoader(boolean b) {
        cfg = cfg.setUseThreadContextLoader(b);
        return this;
    }

    public ReflectiveConfigOptionLoader setUseCallerLoader(boolean b) {
        cfg = cfg.setUseCallerLoader(b);
        return this;
    }

    public ReflectiveConfigOptionLoader setPreferredClassLoaders(List<ClassLoader> loaders) {
        cfg = cfg.setPreferredClassLoaders(Collections.unmodifiableList(new ArrayList<>(loaders)));
        return this;
    }

    public ReflectiveConfigOptionLoader setEnabled(boolean enabled) {
        cfg = cfg.setEnabled(enabled);
        return this;
    }

    public ReflectiveConfigOptionLoader reset() {
        cfg = new LoaderConfiguration();
        return this;
    }

    /**
     * Reflectively load types at most once over the life of this class. This
     * method is synchronized and uses a static class field to ensure that it
     * calls {@link #load(LoaderConfiguration, Class)} only on the first invocation and does nothing
     * thereafter. This is the right behavior as long as the classpath doesn't
     * change in the middle of the enclosing JVM's lifetime.
     */
    public void loadAll(Class<?> caller) {

        LoaderConfiguration cfg = this.cfg;

        if (!cfg.enabled || cfg.allInit)
            return;

        load(cfg, caller);

        cfg.allInit = true;
    }

    public void loadStandard(Class<?> caller) {

        LoaderConfiguration cfg = this.cfg;

        if (!cfg.enabled || cfg.standardInit || cfg.allInit)
            return;

        /*
         * Aside from the classes in janusgraph-core, we can't guarantee the presence
         * of these classes at runtime.  That's why they're loaded reflectively.
         * We could probably hard-code the initialization of the janusgraph-core classes,
         * but the benefit isn't substantial.
         */
        List<String> classnames = Collections.unmodifiableList(Arrays.asList(
            "org.janusgraph.diskstorage.hbase.HBaseStoreManager",
            "org.janusgraph.diskstorage.cql.CQLConfigOptions",
            "org.janusgraph.diskstorage.es.ElasticSearchIndex",
            "org.janusgraph.diskstorage.solr.SolrIndex",
            "org.janusgraph.diskstorage.log.kcvs.KCVSLog",
            "org.janusgraph.diskstorage.log.kcvs.KCVSLogManager",
            "org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration",
            "org.janusgraph.graphdb.database.idassigner.placement.SimpleBulkPlacementStrategy",
            "org.janusgraph.graphdb.database.idassigner.VertexIDAssigner",
            "org.janusgraph.graphdb.query.index.ThresholdBasedIndexSelectionStrategy",
            //"org.janusgraph.graphdb.TestMockIndexProvider",
            //"org.janusgraph.graphdb.TestMockLog",
            "org.janusgraph.diskstorage.berkeleyje.BerkeleyJEStoreManager"));

        Timer t = new Timer(TimestampProviders.MILLI);
        t.start();

        List<ClassLoader> loaders = getClassLoaders(cfg, caller);

        // Iterate over classloaders until the first successful load, then keep that
        // loader even if it fails to load classes further down the classnames list.

        boolean foundLoader = false;
        ClassLoader cl = null;
        int loadedClasses = 0;

        for (String c : classnames) {
            if (foundLoader) {
                try {
                    Class.forName(c, true, cl);
                    loadedClasses++;
                    log.debug("Loaded class {} with selected loader {}", c, cl);
                } catch (Throwable e) {
                    log.debug("Unable to load class {} with selected loader {}", c, cl, e);
                }
            } else {
                for (ClassLoader candidate : loaders) {
                    cl = candidate;
                    try {
                        Class.forName(c, true, cl);
                        loadedClasses++;
                        log.debug("Loaded class {} with loader {}", c, cl);
                        log.debug("Located functioning classloader {}; using it for remaining classload attempts", cl);
                        foundLoader = true;
                        break;
                    } catch (Throwable e) {
                        log.debug("Unable to load class {} with loader {}", c, cl, e);
                    }
                }
            }
        }

        log.info("Loaded and initialized config classes: {} OK out of {} attempts in {}", loadedClasses, classnames.size(), t.elapsed());

        cfg.standardInit = true;
    }

    private List<ClassLoader> getClassLoaders(LoaderConfiguration cfg, Class<?> caller) {

        final List<ClassLoader> builder = new ArrayList<>(cfg.preferredLoaders);

        for (ClassLoader c : cfg.preferredLoaders)
            log.debug("Added preferred classloader to config option loader chain: {}", c);

        if (cfg.useThreadContextLoader) {
            ClassLoader c = Thread.currentThread().getContextClassLoader();
            builder.add(c);
            log.debug("Added thread context classloader to config option loader chain: {}", c);
        }

        if (cfg.useCallerLoader) {
            ClassLoader c = caller.getClassLoader();
            builder.add(c);
            log.debug("Added caller classloader to config option loader chain: {}", c);
        }

        return Collections.unmodifiableList(builder);
    }

    /**
     * Use reflection to iterate over the classpath looking for
     * {@link PreInitializeConfigOptions} annotations, then load any such
     * annotated types found. This method's runtime is roughly proportional to
     * the number of elements in the classpath (and can be substantial).
     */
    private synchronized void load(LoaderConfiguration cfg, Class<?> caller) {
        try {
            loadAllClassesUnsafe(cfg, caller);
        } catch (Throwable t) {
            // We could probably narrow the caught exception type to Error or maybe even just LinkageError,
            // but in this case catching anything via Throwable seems appropriate.  RuntimeException is
            // not sufficient -- it wouldn't even catch NoClassDefFoundError.
            log.error("Failed to iterate over classpath using Reflections; this usually indicates a broken classpath/classloader", t);
        }
    }

    private void loadAllClassesUnsafe(LoaderConfiguration cfg, Class<?> caller) {
        int loadCount = 0;
        int errorCount = 0;

        List<ClassLoader> loaderList = getClassLoaders(cfg, caller);
        Collection<URL> scanUrls = forClassLoaders(loaderList);
        Iterator<URL> i = scanUrls.iterator();
        while (i.hasNext()) {
            URL u = i.next();
            File f;
            try {
                f = Vfs.getFile(u).orElse(null);
            } catch (Throwable t) {
                log.debug("Error invoking Vfs.getFile on URL {}", u, t);
                f = new File(u.getPath());
            }
            if (f == null || !f.exists() || !f.isDirectory() || !f.canRead()) {
                log.trace("Skipping nonexistent, non-directory, or unreadable classpath element {}", f);
                i.remove();
            }
            log.trace("Retaining classpath element {}", f);
        }

        org.reflections8.Configuration rc = new org.reflections8.util.ConfigurationBuilder()
            .setUrls(scanUrls)
            .setScanners(new TypeAnnotationsScanner(), new SubTypesScanner());
        Reflections reflections = new Reflections(rc);

        //for (Class<?> c : reflections.getSubTypesOf(Object.class)) {  // Returns nothing
        for (Class<?> c : reflections.getTypesAnnotatedWith(PreInitializeConfigOptions.class)) {
            try {
                loadCount += loadSingleClassUnsafe(c);
            } catch (Throwable t) {
                log.warn("Failed to load class {} or its referenced types; this usually indicates a broken classpath/classloader", c, t);
                errorCount++;
            }
        }

        log.debug("Pre-loaded {} config option(s) via Reflections ({} class(es) with errors)", loadCount, errorCount);

    }

    /**
     * This method is based on ClasspathHelper.forClassLoader from Reflections.
     *
     * We made our own copy to avoid dealing with bytecode-level incompatibilities
     * introduced by changing method signatures between 0.9.9-RC1 and 0.9.9.
     *
     * @return A set of all URLs associated with URLClassLoaders in the argument
     */
    private Set<URL> forClassLoaders(List<ClassLoader> loaders) {

        final Set<URL> result = new HashSet<>();

        for (ClassLoader classLoader : loaders) {
            while (classLoader != null) {
                if (classLoader instanceof URLClassLoader) {
                    URL[] urls = ((URLClassLoader) classLoader).getURLs();
                    if (urls != null) {
                        result.addAll(Arrays.asList(urls));
                    }
                }
                classLoader = classLoader.getParent();
            }
        }

        return result;
    }

    private int loadSingleClassUnsafe(Class<?> c) {
        int loadCount = 0;

        log.trace("Looking for ConfigOption public static fields on class {}", c);

        for (Field f : c.getDeclaredFields()) {
            final boolean pub = Modifier.isPublic(f.getModifiers());
            final boolean stat = Modifier.isStatic(f.getModifiers());
            final boolean typeMatch = ConfigOption.class.isAssignableFrom(f.getType());

            log.trace("Properties for field \"{}\": public={} static={} assignable={}", f, pub, stat, typeMatch);
            if (pub && stat && typeMatch) {
                try {
                    Object o = f.get(null);
                    Preconditions.checkNotNull(o);
                    log.debug("Initialized {}={}", f, o);
                    loadCount++;
                } catch (IllegalArgumentException | IllegalAccessException e) {
                    log.warn("ConfigOption initialization error", e);
                }
            }
        }

        return loadCount;
    }

    private static class LoaderConfiguration {

        private static final Logger log =
                LoggerFactory.getLogger(LoaderConfiguration.class);

        private final boolean enabled;
        private final List<ClassLoader> preferredLoaders;
        private final boolean useCallerLoader;
        private final boolean useThreadContextLoader;
        private volatile boolean allInit = false;
        private volatile boolean standardInit = false;

        private LoaderConfiguration(boolean enabled, List<ClassLoader> preferredLoaders,
                                    boolean useCallerLoader, boolean useThreadContextLoader) {
            this.enabled = enabled;
            this.preferredLoaders = preferredLoaders;
            this.useCallerLoader = useCallerLoader;
            this.useThreadContextLoader = useThreadContextLoader;
        }

        private LoaderConfiguration() {
            enabled = getEnabledByDefault();
            preferredLoaders = Collections.singletonList(ReflectiveConfigOptionLoader.class.getClassLoader());
            useCallerLoader = true;
            useThreadContextLoader = true;
        }

        private boolean getEnabledByDefault() {
            List<String> sources =
                    Arrays.asList(System.getProperty(SYS_PROP_NAME), System.getenv(ENV_VAR_NAME));

            for (String setting : sources) {
                if (null != setting) {
                    boolean enabled = setting.equalsIgnoreCase("true");
                    log.debug("Option loading enabled={}", enabled);
                    return enabled;
                }
            }

            log.debug("Option loading enabled by default");

            return true;
        }

        LoaderConfiguration setEnabled(boolean b) {
            return new LoaderConfiguration(b, preferredLoaders, useCallerLoader, useThreadContextLoader);
        }

        LoaderConfiguration setPreferredClassLoaders(List<ClassLoader> cl) {
            return new LoaderConfiguration(enabled, cl, useCallerLoader, useThreadContextLoader);
        }

        LoaderConfiguration setUseCallerLoader(boolean b) {
            return new LoaderConfiguration(enabled, preferredLoaders, b, useThreadContextLoader);
        }

        LoaderConfiguration setUseThreadContextLoader(boolean b) {
            return new LoaderConfiguration(enabled, preferredLoaders, useCallerLoader, b);
        }
    }
}
