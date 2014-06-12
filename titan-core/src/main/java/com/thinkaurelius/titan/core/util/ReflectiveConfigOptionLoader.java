package com.thinkaurelius.titan.core.util;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.reflections.Reflections;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.graphdb.configuration.PreInitializeConfigOptions;

public class ReflectiveConfigOptionLoader {

    private static final Logger log =
            LoggerFactory.getLogger(ReflectiveConfigOptionLoader.class);

    private static boolean loaded = false;

    /**
     * Use reflection to iterate over the classpath looking for
     * {@link PreInitializeConfigOptions} annotations, then load any such
     * annotated types found. This method's runtime is roughly proportional to
     * the number of elements in the classpath (and can be substantial).
     */
    public static void load() {

//        final Stopwatch sw =  Stopwatch.createStarted();

        Collection<URL> scanUrls = ClasspathHelper.forJavaClassPath();
        Iterator<URL> i = scanUrls.iterator();
        while (i.hasNext()) {
            File f = new File(i.next().getPath());
            if (!f.exists() || f.isDirectory()) {
                log.trace("Skipping classpath element {}", f);
                i.remove();
            }
            log.trace("Retaining classpath element {}", f);
        }

        org.reflections.Configuration rc = new org.reflections.util.ConfigurationBuilder()
            .setUrls(scanUrls)
            .setScanners(new TypeAnnotationsScanner());
    //  .setScanners(new SubTypesScanner(), new TypeAnnotationsScanner(), new FieldAnnotationsScanner());
        Reflections reflections = new Reflections(rc);

        int loadCount = 0;

    //    for (Class<?> c : reflections.getSubTypesOf(Object.class)) {  // Returns nothing
        for (Class<?> c : reflections.getTypesAnnotatedWith(PreInitializeConfigOptions.class)) {
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
                    } catch (IllegalArgumentException e) {
                        log.warn("ConfigOption initialization error", e);
                    } catch (IllegalAccessException e) {
                        log.warn("ConfigOption initialization error", e);
                    }
                }
            }
        }

        log.debug("Preloaded {} config options via reflections in {} ms", loadCount);
    }

    /**
     * Reflectively load types at most once over the life of this class. This
     * method is synchronized and uses a static class field to ensure that it
     * calls {@link #load()} only on the first invocation and does nothing
     * thereafter. This is the right behavior as long as the classpath doesn't
     * change in the middle of the enclosing JVM's lifetime.
     */
    public synchronized static void loadOnce() {

        if (loaded)
            return;

        load();

        loaded = true;
    }
}
