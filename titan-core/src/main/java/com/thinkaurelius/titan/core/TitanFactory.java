package com.thinkaurelius.titan.core;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.configuration.*;
import com.thinkaurelius.titan.diskstorage.configuration.backend.CommonsConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.PreInitializeConfigOptions;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.reflections.Reflections;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * TitanFactory is used to open or instantiate a Titan graph database.
 *
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 * @see TitanGraph
 */

public class TitanFactory {

    private static final Logger log =
            LoggerFactory.getLogger(TitanFactory.class);

    private static boolean preloadedConfigOptions = false;

    /**
     * Opens a {@link TitanGraph} database.
     * <p/>
     * If the argument points to a configuration file, the configuration file is loaded to configure the database.
     * If the argument points to a path, a graph database is created or opened at that location.
     *
     * @param shortcutOrFile Configuration file name or directory name
     * @return Titan graph database
     * @see <a href="https://github.com/thinkaurelius/titan/wiki/Graph-Configuration">Graph Configuration Wiki</a>
     */
    public static TitanGraph open(String shortcutOrFile) {
        return open(getLocalConfiguration(shortcutOrFile));
    }

    /**
     * Opens a {@link TitanGraph} database configured according to the provided configuration.
     *
     * @param configuration Configuration for the graph database
     * @return Titan graph database
     * @see <a href="https://github.com/thinkaurelius/titan/wiki/Graph-Configuration">Graph Configuration Wiki</a>
     */
    public static TitanGraph open(Configuration configuration) {
        return open(new CommonsConfiguration(configuration));
    }

    public static TitanGraph open(BasicConfiguration configuration) {
        return open(configuration.getConfiguration());
    }

    public static TitanGraph open(ReadConfiguration configuration) {
        preloadConfigOptions();
        return new StandardTitanGraph(new GraphDatabaseConfiguration(configuration));
    }

    public static Builder build() {
        return new Builder();
    }

    //--------------------- BUILDER -------------------------------------------

    public static class Builder extends UserModifiableConfiguration {

        public Builder() {
            super(GraphDatabaseConfiguration.buildConfiguration());
        }

        public Builder set(String path, Object value) {
            super.set(path,value);
            return this;
        }

        public TitanGraph open() {
            return TitanFactory.open(super.getConfiguration());
        }


    }

    //###################################
    //          HELPER METHODS
    //###################################

    private synchronized static void preloadConfigOptions() {

        if (preloadedConfigOptions)
            return;

        final Stopwatch sw = new Stopwatch().start();

        org.reflections.Configuration rc = new org.reflections.util.ConfigurationBuilder()
            .setUrls(ClasspathHelper.forJavaClassPath())
            .setScanners(new TypeAnnotationsScanner());
//      .setScanners(new SubTypesScanner(), new TypeAnnotationsScanner(), new FieldAnnotationsScanner());
        Reflections reflections = new Reflections(rc);

        int preloaded = 0;

//        for (Class<?> c : reflections.getSubTypesOf(Object.class)) {  // Returns nothing
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
                        preloaded++;
                    } catch (IllegalArgumentException e) {
                        log.warn("ConfigOption initialization error", e);
                    } catch (IllegalAccessException e) {
                        log.warn("ConfigOption initialization error", e);
                    }
                }
            }
        }

        preloadedConfigOptions = true;

        log.debug("Preloaded {} config options via reflections in {} ms",
                preloaded, sw.stop().elapsed(TimeUnit.MILLISECONDS));
    }

    private static ReadConfiguration getLocalConfiguration(String shortcutOrFile) {
        File file = new File(shortcutOrFile);
        if (file.exists()) return getLocalConfiguration(file);
        else {
            int pos = shortcutOrFile.indexOf(':');
            if (pos<0) pos = shortcutOrFile.length();
            String backend = shortcutOrFile.substring(0,pos);
            Preconditions.checkArgument(Backend.REGISTERED_STORAGE_MANAGERS_SHORTHAND.containsKey(backend.toLowerCase()), "Backend shorthand unknown: %s", backend);
            String secondArg = null;
            if (pos+1<shortcutOrFile.length()) secondArg = shortcutOrFile.substring(pos + 1).trim();
            BaseConfiguration config = new BaseConfiguration();
            ModifiableConfiguration writeConfig = new ModifiableConfiguration(TITAN_NS,new CommonsConfiguration(config), BasicConfiguration.Restriction.NONE);
            writeConfig.set(STORAGE_BACKEND,backend);
            ConfigOption option = Backend.REGISTERED_STORAGE_MANAGERS_SHORTHAND.get(backend.toLowerCase());
            if (option==null) {
                Preconditions.checkArgument(secondArg==null);
            } else if (option==STORAGE_DIRECTORY || option==STORAGE_CONF_FILE) {
                Preconditions.checkArgument(StringUtils.isNotBlank(secondArg),"Need to provide additional argument to initialize storage backend");
                writeConfig.set(option,getAbsolutePath(secondArg));
            } else if (option==STORAGE_HOSTS) {
                Preconditions.checkArgument(StringUtils.isNotBlank(secondArg),"Need to provide additional argument to initialize storage backend");
                writeConfig.set(option,new String[]{secondArg});
            } else throw new IllegalArgumentException("Invalid configuration option for backend "+option);
            return new CommonsConfiguration(config);
        }
    }

    /**
     * Load a properties file containing a Titan graph configuration.
     * <p/>
     * <ol>
     * <li>Load the file contents into a {@link org.apache.commons.configuration.PropertiesConfiguration}</li>
     * <li>For each key that points to a configuration object that is either a directory
     * or local file, check
     * whether the associated value is a non-null, non-absolute path. If so,
     * then prepend the absolute path of the parent directory of the provided configuration {@code file}.
     * This has the effect of making non-absolute backend
     * paths relative to the config file's directory rather than the JVM's
     * working directory.
     * <li>Return the {@link ReadConfiguration} for the prepared configuration file</li>
     * </ol>
     * <p/>
     *
     * @param file A properties file to load
     * @return A configuration derived from {@code file}
     */
    @SuppressWarnings("unchecked")
    private static ReadConfiguration getLocalConfiguration(File file) {
        Preconditions.checkArgument(file != null && file.exists() && file.isFile() && file.canRead(),
                "Need to specify a readable configuration file, but was given: %s", file.toString());

        try {
            PropertiesConfiguration configuration = new PropertiesConfiguration(file);

            final File tmpParent = file.getParentFile();
            final File configParent;

            if (null == tmpParent) {
                /*
                 * null usually means we were given a Titan config file path
                 * string like "foo.properties" that refers to the current
                 * working directory of the process.
                 */
                configParent = new File(System.getProperty("user.dir"));
            } else {
                configParent = tmpParent;
            }

            Preconditions.checkNotNull(configParent);
            Preconditions.checkArgument(configParent.isDirectory());

            final Pattern p = Pattern.compile(
                    Pattern.quote(STORAGE_NS.getName()) + "\\..*" +
                            "(" +
                            Pattern.quote(STORAGE_DIRECTORY.getName()) + "|" +
                            Pattern.quote(STORAGE_CONF_FILE.getName()) + "|" +
                            Pattern.quote(INDEX_DIRECTORY.getName()) + "|" +
                            Pattern.quote(INDEX_CONF_FILE.getName()) +
                            ")");

            final Iterator<String> keysToMangle = Iterators.filter(configuration.getKeys(), new Predicate<String>() {
                @Override
                public boolean apply(String key) {
                    if (null == key)
                        return false;
                    return p.matcher(key).matches();
                }
            });

            while (keysToMangle.hasNext()) {
                String k = keysToMangle.next();
                Preconditions.checkNotNull(k);
                String s = configuration.getString(k);
                Preconditions.checkArgument(StringUtils.isNotBlank(s),"Invalid Configuration: key %s has null empty value",k);
                configuration.setProperty(k,getAbsolutePath(configParent,s));
            }
            return new CommonsConfiguration(configuration);
        } catch (ConfigurationException e) {
            throw new IllegalArgumentException("Could not load configuration at: " + file, e);
        }
    }

    private static final String getAbsolutePath(String file) {
        return getAbsolutePath(new File(System.getProperty("user.dir")), file);
    }

    private static final String getAbsolutePath(final File configParent, String file) {
        File storedir = new File(file);
        if (!storedir.isAbsolute()) {
            String newFile = configParent.getAbsolutePath() + File.separator + file;
            log.debug("Overwrote relative path: was {}, now {}", file, newFile);
            return newFile;
        } else {
            log.debug("Loaded absolute path for key: {}", file);
            return file;
        }
    }

}
