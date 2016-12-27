package com.thinkaurelius.titan.core;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

import com.thinkaurelius.titan.core.log.LogProcessorFramework;
import com.thinkaurelius.titan.core.log.TransactionRecovery;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.StandardStoreManager;
import com.thinkaurelius.titan.diskstorage.configuration.*;
import com.thinkaurelius.titan.diskstorage.configuration.backend.CommonsConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;

import com.thinkaurelius.titan.graphdb.log.StandardLogProcessorFramework;
import com.thinkaurelius.titan.graphdb.log.StandardTransactionLogProcessor;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Instant;
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

    /**
     * Opens a {@link TitanGraph} database.
     * <p/>
     * If the argument points to a configuration file, the configuration file is loaded to configure the Titan graph
     * If the string argument is a configuration short-cut, then the short-cut is parsed and used to configure the returned Titan graph.
     * <p />
     * A configuration short-cut is of the form:
     * [STORAGE_BACKEND_NAME]:[DIRECTORY_OR_HOST]
     *
     * @param shortcutOrFile Configuration file name or configuration short-cut
     * @return Titan graph database configured according to the provided configuration
     * @see <a href="http://s3.thinkaurelius.com/docs/titan/current/configuration.html">"Configuration" manual chapter</a>
     * @see <a href="http://s3.thinkaurelius.com/docs/titan/current/titan-config-ref.html">Configuration Reference</a>
     */
    public static TitanGraph open(String shortcutOrFile) {
        return open(getLocalConfiguration(shortcutOrFile));
    }

    /**
     * Opens a {@link TitanGraph} database configured according to the provided configuration.
     *
     * @param configuration Configuration for the graph database
     * @return Titan graph database
     * @see <a href="http://s3.thinkaurelius.com/docs/titan/current/configuration.html">"Configuration" manual chapter</a>
     * @see <a href="http://s3.thinkaurelius.com/docs/titan/current/titan-config-ref.html">Configuration Reference</a>
     */
    public static TitanGraph open(Configuration configuration) {
        return open(new CommonsConfiguration(configuration));
    }

    /**
     * Opens a {@link TitanGraph} database configured according to the provided configuration.
     *
     * @param configuration Configuration for the graph database
     * @return Titan graph database
     */
    public static TitanGraph open(BasicConfiguration configuration) {
        return open(configuration.getConfiguration());
    }

    /**
     * Opens a {@link TitanGraph} database configured according to the provided configuration.
     *
     * @param configuration Configuration for the graph database
     * @return Titan graph database
     */
    public static TitanGraph open(ReadConfiguration configuration) {
        return new StandardTitanGraph(new GraphDatabaseConfiguration(configuration));
    }

    /**
     * Returns a {@link Builder} that allows to set the configuration options for opening a Titan graph database.
     * <p />
     * In the builder, the configuration options for the graph can be set individually. Once all options are configured,
     * the graph can be opened with {@link com.thinkaurelius.titan.core.TitanFactory.Builder#open()}.
     *
     * @return
     */
    public static Builder build() {
        return new Builder();
    }

    //--------------------- BUILDER -------------------------------------------

    public static class Builder {

        private final WriteConfiguration writeConfiguration;

        private Builder() {
            writeConfiguration = new CommonsConfiguration();
        }

        /**
         * Configures the provided configuration path to the given value.
         *
         * @param path
         * @param value
         * @return
         */
        public Builder set(String path, Object value) {
            writeConfiguration.set(path, value);
            return this;
        }

        /**
         * Opens a Titan graph with the previously configured options.
         *
         * @return
         */
        public TitanGraph open() {
            ModifiableConfiguration mc = new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,
                    writeConfiguration.copy(), BasicConfiguration.Restriction.NONE);
            return TitanFactory.open(mc);
        }


    }

    /**
     * Returns a {@link com.thinkaurelius.titan.core.log.LogProcessorFramework} for processing transaction log entries
     * against the provided graph instance.
     *
     * @param graph
     * @return
     */
    public static LogProcessorFramework openTransactionLog(TitanGraph graph) {
        return new StandardLogProcessorFramework((StandardTitanGraph)graph);
    }

    /**
     * Returns a {@link TransactionRecovery} process for recovering partially failed transactions. The recovery process
     * will start processing the write-ahead transaction log at the specified transaction time.
     *
     * @param graph
     * @param start
     * @return
     */
    public static TransactionRecovery startTransactionRecovery(TitanGraph graph, Instant start) {
        return new StandardTransactionLogProcessor((StandardTitanGraph)graph, start);
    }

    //###################################
    //          HELPER METHODS
    //###################################

    private static ReadConfiguration getLocalConfiguration(String shortcutOrFile) {
        File file = new File(shortcutOrFile);
        if (file.exists()) return getLocalConfiguration(file);
        else {
            int pos = shortcutOrFile.indexOf(':');
            if (pos<0) pos = shortcutOrFile.length();
            String backend = shortcutOrFile.substring(0,pos);
            Preconditions.checkArgument(StandardStoreManager.getAllManagerClasses().containsKey(backend.toLowerCase()), "Backend shorthand unknown: %s", backend);
            String secondArg = null;
            if (pos+1<shortcutOrFile.length()) secondArg = shortcutOrFile.substring(pos + 1).trim();
            BaseConfiguration config = new BaseConfiguration();
            ModifiableConfiguration writeConfig = new ModifiableConfiguration(ROOT_NS,new CommonsConfiguration(config), BasicConfiguration.Restriction.NONE);
            writeConfig.set(STORAGE_BACKEND,backend);
            ConfigOption option = Backend.getOptionForShorthand(backend);
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

            // TODO this mangling logic is a relic from the hardcoded string days; it should be deleted and rewritten as a setting on ConfigOption
            final Pattern p = Pattern.compile("(" +
                    Pattern.quote(STORAGE_NS.getName()) + "\\..*" +
                            "(" + Pattern.quote(STORAGE_DIRECTORY.getName()) + "|" +
                                  Pattern.quote(STORAGE_CONF_FILE.getName()) + ")"
                    + "|" +
                    Pattern.quote(INDEX_NS.getName()) + "\\..*" +
                            "(" + Pattern.quote(INDEX_DIRECTORY.getName()) + "|" +
                                  Pattern.quote(INDEX_CONF_FILE.getName()) +  ")"
            + ")");

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
