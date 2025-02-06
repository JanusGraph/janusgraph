// Copyright 2018 JanusGraph Authors
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

package org.janusgraph.graphdb.configuration;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import org.apache.commons.lang3.ClassUtils;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.jsr223.GremlinLangScriptEngine;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.LazyBarrierStrategy;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.janusgraph.core.JanusGraphConfigurationException;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.schema.DefaultSchemaMaker;
import org.janusgraph.core.schema.DisableDefaultSchemaMaker;
import org.janusgraph.core.schema.IgnorePropertySchemaMaker;
import org.janusgraph.core.schema.JanusGraphDefaultSchemaMaker;
import org.janusgraph.core.schema.SchemaInitStrategy;
import org.janusgraph.core.schema.SchemaInitType;
import org.janusgraph.core.schema.Tp3DefaultSchemaMaker;
import org.janusgraph.core.schema.IndicesActivationType;
import org.janusgraph.diskstorage.Backend;
import org.janusgraph.diskstorage.StandardIndexProvider;
import org.janusgraph.diskstorage.StandardStoreManager;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ExecutorServiceBuilder;
import org.janusgraph.diskstorage.configuration.ExecutorServiceConfiguration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.ReadConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.diskstorage.configuration.converter.ReadConfigurationConverter;
import org.janusgraph.diskstorage.idmanagement.ConflictAvoidanceMode;
import org.janusgraph.diskstorage.idmanagement.ConsistentKeyIDAuthority;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.configuration.converter.RegisteredAttributeClassesConverter;
import org.janusgraph.graphdb.database.cache.MetricInstrumentedSchemaCache;
import org.janusgraph.graphdb.database.cache.SchemaCache;
import org.janusgraph.graphdb.database.cache.StandardSchemaCache;
import org.janusgraph.graphdb.database.idassigner.VertexIDAssigner;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.database.serialize.StandardSerializer;
import org.janusgraph.graphdb.query.index.ApproximateIndexSelectionStrategy;
import org.janusgraph.graphdb.query.index.BruteForceIndexSelectionStrategy;
import org.janusgraph.graphdb.query.index.IndexSelectionStrategy;
import org.janusgraph.graphdb.query.index.ThresholdBasedIndexSelectionStrategy;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.MultiQueryDropStepStrategyMode;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.MultiQueryLabelStepStrategyMode;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.MultiQueryPropertiesStrategyMode;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.MultiQueryStrategyRepeatStepMode;
import org.janusgraph.graphdb.tinkerpop.optimize.strategy.MultiQueryHasStepStrategyMode;
import org.janusgraph.graphdb.transaction.StandardTransactionBuilder;
import org.janusgraph.graphdb.types.system.ImplicitKey;
import org.janusgraph.util.StringUtils;
import org.janusgraph.util.stats.MetricManager;
import org.janusgraph.util.stats.NumberUtil;
import org.janusgraph.util.system.ConfigurationUtil;
import org.janusgraph.util.system.NetworkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.management.MBeanServerFactory;

/**
 * Provides functionality to configure a {@link org.janusgraph.core.JanusGraph} INSTANCE.
 * <p>
 * <p>
 * A graph database configuration is uniquely associated with a graph database and must not be used for multiple
 * databases.
 * <p>
 * After a graph database has been initialized with respect to a configuration, some parameters of graph database
 * configuration may no longer be modifiable.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public class GraphDatabaseConfiguration {

    private static final Logger log = LoggerFactory.getLogger(GraphDatabaseConfiguration.class);

    public static final ConfigNamespace ROOT_NS = new ConfigNamespace(null,"root","Root Configuration Namespace for the JanusGraph Graph Database");

    // ########## Graph-level Config Options ##########
    // ################################################

    public static final ConfigNamespace GRAPH_NS = new ConfigNamespace(ROOT_NS,"graph",
            "General configuration options");

    public static final ConfigOption<Boolean> ALLOW_SETTING_VERTEX_ID = new ConfigOption<>(GRAPH_NS,"set-vertex-id",
            "Whether user provided vertex ids should be enabled and JanusGraph's automatic vertex id allocation be disabled. " +
            "Useful when operating JanusGraph in concert with another storage system that assigns long ids but disables some " +
            "of JanusGraph's advanced features which can lead to inconsistent data. For example, users must ensure the vertex ids " +
            "are unique to avoid duplication. Must use `graph.getIDManager().toVertexId(long)` to convert your id first. Once " +
            "this is enabled, you have to provide vertex id when creating new vertices. EXPERT FEATURE - USE WITH GREAT CARE.",
            ConfigOption.Type.GLOBAL_OFFLINE, false);

    public static final ConfigOption<Boolean> ALLOW_CUSTOM_VERTEX_ID_TYPES = new ConfigOption<>(GRAPH_NS, "allow-custom-vid-types",
            "Whether non long-type vertex ids are allowed. " + ALLOW_SETTING_VERTEX_ID.getName() +
            " must be enabled in order to use this functionality. Currently, only string-type is supported. This does not " +
            "prevent users from using custom ids with long type. If your storage backend does not support unordered " +
            "scan, then some scan operations will be disabled. You cannot use this feature with Berkeley DB. EXPERT FEATURE - USE WITH GREAT CARE.",
            ConfigOption.Type.GLOBAL_OFFLINE, false);

    public static final ConfigOption<String> GRAPH_NAME = new ConfigOption<>(GRAPH_NS, "graphname",
            "This config option is an optional configuration setting that you may supply when opening a graph. " +
            "The String value you provide will be the name of your graph. If you use the ConfigurationManagement APIs, " +
            "then you will be able to access your graph by this String representation using the ConfiguredGraphFactory APIs.",
            ConfigOption.Type.LOCAL, String.class);

    public static final ConfigOption<TimestampProviders> TIMESTAMP_PROVIDER = new ConfigOption<>(GRAPH_NS, "timestamps",
            "The timestamp resolution to use when writing to storage and indices. Sets the time granularity for the " +
            "entire graph cluster. To avoid potential inaccuracies, the configured time resolution should match " +
            "those of the backend systems. Some JanusGraph storage backends declare a preferred timestamp resolution that " +
            "reflects design constraints in the underlying service. When the backend provides " +
            "a preferred default, and when this setting is not explicitly declared in the config file, the backend " +
            "default is used and the general default associated with this setting is ignored.  An explicit " +
            "declaration of this setting overrides both the general and backend-specific defaults.",
            ConfigOption.Type.FIXED, TimestampProviders.class, TimestampProviders.MICRO);

    public static final ConfigOption<String> UNIQUE_INSTANCE_ID = new ConfigOption<>(GRAPH_NS,"unique-instance-id",
            "Unique identifier for this JanusGraph instance.  This must be unique among all instances " +
            "concurrently accessing the same stores or indexes.  It's automatically generated by " +
            "concatenating the hostname, process id, and a static (process-wide) counter. " +
            "Leaving it unset is recommended.",
            ConfigOption.Type.LOCAL, String.class);


    public static final ConfigOption<Short> UNIQUE_INSTANCE_ID_SUFFIX = new ConfigOption<>(GRAPH_NS,"unique-instance-id-suffix",
            "When this is set and " + UNIQUE_INSTANCE_ID.getName() + " is not, this JanusGraph " +
            "instance's unique identifier is generated by concatenating the hex encoded hostname to the " +
            "provided number.",
            ConfigOption.Type.LOCAL, Short.class);

    public static final ConfigOption<String> INITIAL_JANUSGRAPH_VERSION = new ConfigOption<>(GRAPH_NS,"janusgraph-version",
            "The version of JanusGraph with which this database was created. Automatically set on first start. Don't manually set this property.",
            ConfigOption.Type.FIXED, String.class).hide();

    public static final ConfigOption<String> INITIAL_STORAGE_VERSION = new ConfigOption<>(GRAPH_NS,"storage-version",
            "The version of JanusGraph storage schema with which this database was created. Automatically set on first start of graph. " +
            "Should only ever be changed if upgrading to a new major release version of JanusGraph that contains schema changes",
            ConfigOption.Type.FIXED, String.class);

    public static ConfigOption<Boolean> ALLOW_UPGRADE = new ConfigOption<>(GRAPH_NS, "allow-upgrade",
            "Setting this to true will allow certain fixed values to be updated such as storage-version. This should only be used for upgrading.",
            ConfigOption.Type.MASKABLE, Boolean.class, false);

    public static final ConfigOption<Boolean> UNIQUE_INSTANCE_ID_HOSTNAME = new ConfigOption<>(GRAPH_NS, "use-hostname-for-unique-instance-id",
            "When this is set, this JanusGraph's unique instance identifier is set to the hostname. If " + UNIQUE_INSTANCE_ID_SUFFIX.getName() +
            " is also set, then the identifier is set to <hostname><suffix>.",
            ConfigOption.Type.LOCAL, Boolean.class, false);

    public static final ConfigOption<Boolean> REPLACE_INSTANCE_IF_EXISTS = new ConfigOption<>(GRAPH_NS, "replace-instance-if-exists",
            "If a JanusGraph instance with the same instance identifier already exists, the usage of this " +
            "configuration option results in the opening of this graph anyway.",
            ConfigOption.Type.LOCAL, Boolean.class, false);

    public static final ConfigOption<String> TITAN_COMPATIBLE_VERSIONS = new ConfigOption<>(GRAPH_NS,"titan-version",
            "Titan version for backwards compatibility which this database was created. Automatically set on first start. Don't manually set this property.",
            ConfigOption.Type.FIXED, String.class).hide();

    public static final ConfigOption<Boolean> ALLOW_STALE_CONFIG = new ConfigOption<>(GRAPH_NS,"allow-stale-config",
            "Whether to allow the local and storage-backend-hosted copies of the configuration to contain conflicting values for " +
            "options with any of the following types: " + StringUtils.join(ConfigOption.getManagedTypes(), ", ") + ".  " +
            "These types are managed globally through the storage backend and cannot be overridden by changing the " +
            "local configuration.  This type of conflict usually indicates misconfiguration.  When this option is true, " +
            "JanusGraph will log these option conflicts, but continue normal operation using the storage-backend-hosted value " +
            "for each conflicted option.  When this option is false, JanusGraph will log these option conflicts, but then it " +
            "will throw an exception, refusing to start.",
            ConfigOption.Type.MASKABLE, Boolean.class, true);

    // ################ INSTANCE REGISTRATION (system) #######################
    // ##############################################################

    public static final ConfigNamespace REGISTRATION_NS = new ConfigNamespace(ROOT_NS,"system-registration",
            "This is used internally to keep track of open instances.",true);

    public static final ConfigOption<Instant> REGISTRATION_TIME = new ConfigOption<>(REGISTRATION_NS,"startup-time",
            "Timestamp when this instance was started.  Automatically set.", ConfigOption.Type.GLOBAL, Instant.class).hide();


    // ########## OLAP Style Processing ##########
    // ################################################

    public static final ConfigNamespace JOB_NS = new ConfigNamespace(null,"job","Root Configuration Namespace for JanusGraph OLAP jobs");

    public static final ConfigOption<Long> JOB_START_TIME = new ConfigOption<>(JOB_NS,"start-time",
            "Timestamp (ms since epoch) when the job started. Automatically set.", ConfigOption.Type.LOCAL, Long.class).hide();


    public static final ConfigNamespace COMPUTER_NS = new ConfigNamespace(ROOT_NS,"computer",
            "GraphComputer related configuration");

    public static final ConfigOption<String> COMPUTER_RESULT_MODE = new ConfigOption<>(COMPUTER_NS,"result-mode",
            "How the graph computer should return the computed results. 'persist' for writing them into the graph, " +
                    "'localtx' for writing them into the local transaction, or 'none' (default)", ConfigOption.Type.MASKABLE, "none");


    // ################ Transaction #######################
    // ################################################

    public static final ConfigNamespace TRANSACTION_NS = new ConfigNamespace(ROOT_NS,"tx",
            "Configuration options for transaction handling");

    public static final ConfigOption<Boolean> SYSTEM_LOG_TRANSACTIONS = new ConfigOption<>(TRANSACTION_NS,"log-tx",
            "Whether transaction mutations should be logged to JanusGraph's write-ahead transaction log which can be used for recovery of partially failed transactions",
            ConfigOption.Type.GLOBAL, false);

    public static final ConfigOption<Duration> MAX_COMMIT_TIME = new ConfigOption<>(TRANSACTION_NS,"max-commit-time",
            "Maximum time (in ms) that a transaction might take to commit against all backends. This is used by the distributed " +
                    "write-ahead log processing to determine when a transaction can be considered failed (i.e. after this time has elapsed)." +
                    "Must be longer than the maximum allowed write time.",
            ConfigOption.Type.GLOBAL, Duration.ofSeconds(10));


    public static final ConfigNamespace TRANSACTION_RECOVERY_NS = new ConfigNamespace(TRANSACTION_NS,"recovery",
            "Configuration options for transaction recovery processes");

    public static final ConfigOption<Boolean> VERBOSE_TX_RECOVERY = new ConfigOption<>(TRANSACTION_RECOVERY_NS,"verbose",
            "Whether the transaction recovery system should print recovered transactions and other activity to standard output",
            ConfigOption.Type.MASKABLE, false);

    // ################ Query Processing #######################
    // ################################################

    public static final ConfigNamespace QUERY_NS = new ConfigNamespace(ROOT_NS,"query",
            "Configuration options for query processing");

    public static final ConfigOption<Boolean> IGNORE_UNKNOWN_INDEX_FIELD = new ConfigOption<>(QUERY_NS, "ignore-unknown-index-key",
            "Whether to ignore undefined types encountered in user-provided index queries",
            ConfigOption.Type.MASKABLE, false);

    public static final String UNKNOWN_FIELD_NAME = "unknown_key";


    public static final ConfigOption<Boolean> FORCE_INDEX_USAGE = new ConfigOption<>(QUERY_NS,"force-index",
            "Whether JanusGraph should throw an exception if a graph query cannot be answered using an index. Doing so " +
                    "limits the functionality of JanusGraph's graph queries but ensures that slow graph queries are avoided " +
                    "on large graphs. Recommended for production use of JanusGraph.",
            ConfigOption.Type.MASKABLE, false);

    public static final ConfigOption<Boolean> PROPERTY_PREFETCHING = new ConfigOption<>(QUERY_NS,"fast-property",
            "Whether to pre-fetch all properties on first singular vertex property access. This can eliminate backend calls on subsequent " +
                    "property access for the same vertex at the expense of retrieving all properties at once. This can be " +
                    "expensive for vertices with many properties. <br>" +
                    "This setting is applicable to direct vertex properties access " +
                    "(like `vertex.properties(\"foo\")` but not to `vertex.properties(\"foo\",\"bar\")` because the latter case " +
                    "is not a singular property access). <br>" +
                    "This setting is not applicable to the next Gremlin steps: `valueMap`, `propertyMap`, `elementMap`, `properties`, `values` " +
                    "(configuration option `query.batch.properties-mode` should be used to configure their behavior).<br>" +
                    "When `true` this setting overwrites `query.batch.has-step-mode` to `"+MultiQueryHasStepStrategyMode.ALL_PROPERTIES.getConfigName()+"` unless `"+
                    MultiQueryHasStepStrategyMode.NONE.getConfigName()+"` mode is used.",
            ConfigOption.Type.MASKABLE, true);

    public static final ConfigOption<Boolean> ADJUST_LIMIT = new ConfigOption<>(QUERY_NS,"smart-limit",
            "Whether the query optimizer should try to guess a smart limit for the query to ensure responsiveness in " +
                    "light of possibly large result sets. Those will be loaded incrementally if this option is enabled.",
            ConfigOption.Type.MASKABLE, false);

    public static final ConfigOption<Integer> HARD_MAX_LIMIT = new ConfigOption<>(QUERY_NS, "hard-max-limit",
            "If smart-limit is disabled and no limit is given in the query, query optimizer adds a limit in light " +
                    "of possibly large result sets. It works in the same way as smart-limit except that hard-max-limit is " +
                    "usually a large number. Default value is Integer.MAX_VALUE which effectively disables this behavior. " +
                    "This option does not take effect when smart-limit is enabled.",
            ConfigOption.Type.MASKABLE, Integer.MAX_VALUE);

    public static final ConfigOption<String> INDEX_SELECT_STRATEGY = new ConfigOption<>(QUERY_NS, "index-select-strategy",
            String.format("Name of the index selection strategy or full class name. Following shorthands can be used: <br>" +
                    "- `%s` (Try all combinations of index candidates and pick up optimal one)<br>" +
                    "- `%s` (Use greedy algorithm to pick up approximately optimal index candidate)<br>" +
                    "- `%s` (Use index-select-threshold to pick up either `%s` or `%s` strategy on runtime)",
                    BruteForceIndexSelectionStrategy.NAME, ApproximateIndexSelectionStrategy.NAME, ThresholdBasedIndexSelectionStrategy.NAME,
                    ApproximateIndexSelectionStrategy.NAME, ThresholdBasedIndexSelectionStrategy.NAME),
            ConfigOption.Type.MASKABLE, ThresholdBasedIndexSelectionStrategy.NAME);

    public static final ConfigOption<Boolean> OPTIMIZER_BACKEND_ACCESS = new ConfigOption<>(QUERY_NS, "optimizer-backend-access",
            "Whether the optimizer should be allowed to fire backend queries during the optimization phase. Allowing these " +
                "will give the optimizer a chance to find more efficient execution plan but also increase the optimization overhead.",
            ConfigOption.Type.MASKABLE, true);

    // ################ BATCH QUERY CONFIGURATION #######################

    public static final ConfigNamespace QUERY_BATCH_NS = new ConfigNamespace(QUERY_NS,"batch",
        "Configuration options to configure batch queries optimization behavior");

    public static final ConfigOption<Boolean> USE_MULTIQUERY = new ConfigOption<>(QUERY_BATCH_NS,"enabled",
        "Whether traversal queries should be batched when executed against the storage backend. This can lead to significant " +
            "performance improvement if there is a non-trivial latency to the backend. If `false` then all other configuration options under `" +
            QUERY_BATCH_NS.toStringWithoutRoot()+"` namespace are ignored.",
        ConfigOption.Type.MASKABLE, true);

    public static final ConfigOption<Boolean> LIMITED_BATCH = new ConfigOption<>(QUERY_BATCH_NS,"limited",
        "Configure a maximum batch size for queries against the storage backend. This can be used to ensure " +
            "responsiveness if batches tend to grow very large. The used batch size is equivalent to the " +
            "barrier size of a preceding `barrier()` step. If a step has no preceding `barrier()`, the default barrier of TinkerPop " +
            "will be inserted. This option only takes effect if `"+USE_MULTIQUERY.toStringWithoutRoot()+"` is `true`.",
        ConfigOption.Type.MASKABLE, true);

    public static final ConfigOption<Integer> LIMITED_BATCH_SIZE = new ConfigOption<>(QUERY_BATCH_NS,"limited-size",
        "Default batch size (barrier() step size) for queries. This size is applied only for cases where `"
            + LazyBarrierStrategy.class.getSimpleName()+"` strategy didn't apply `barrier` step and where user didn't apply " +
            "barrier step either. This option is used only when `"+LIMITED_BATCH.toStringWithoutRoot()+"` is `true`. " +
            "Notice, value `"+Integer.MAX_VALUE+"` is considered to be unlimited.",
        ConfigOption.Type.MASKABLE, 2500);

    public static final ConfigOption<String> REPEAT_STEP_BATCH_MODE = new ConfigOption<>(QUERY_BATCH_NS,"repeat-step-mode",
        String.format("Batch mode for `repeat` step. Used only when "+USE_MULTIQUERY.toStringWithoutRoot()+" is `true`.<br>" +
                "These modes are controlling how the child steps with batch support are behaving if they are placed to the start " +
                "of the `repeat`, `emit`, or `until` traversals.<br>" +
                "Supported modes:<br>" +
                "- `%s` - Child start steps are receiving vertices for batching from the closest `repeat` step parent only.<br>" +
                "- `%s` - Child start steps are receiving vertices for batching from all `repeat` step parents.<br>" +
                "- `%s` - Child start steps are receiving vertices for batching from the closest `repeat` step parent (both for the parent start and for next iterations) " +
                "and also from all `repeat` step parents for the parent start.",
            MultiQueryStrategyRepeatStepMode.CLOSEST_REPEAT_PARENT.getConfigName(),
            MultiQueryStrategyRepeatStepMode.ALL_REPEAT_PARENTS.getConfigName(),
            MultiQueryStrategyRepeatStepMode.STARTS_ONLY_OF_ALL_REPEAT_PARENTS.getConfigName()),
        ConfigOption.Type.MASKABLE, MultiQueryStrategyRepeatStepMode.ALL_REPEAT_PARENTS.getConfigName());

    public static final ConfigOption<String> HAS_STEP_BATCH_MODE = new ConfigOption<>(QUERY_BATCH_NS,"has-step-mode",
        String.format("Properties pre-fetching mode for `has` step. Used only when `"+USE_MULTIQUERY.toStringWithoutRoot()+"` is `true`.<br>" +
                "Supported modes:<br>" +
                "- `%s` - Pre-fetch all vertex properties on any property access (fetches all vertex properties in a single slice query)<br>" +
                "- `%s` - Pre-fetch necessary vertex properties for the whole chain of foldable `has` steps (uses a separate slice query per each required property)<br>" +
                "- `%s` - Prefetch the same properties as with `%s` mode, but also prefetch<br>" +
                "properties which may be needed in the next properties access step like `values`, `properties,` `valueMap`, `elementMap`, or `propertyMap`.<br>" +
                "In case the next step is not one of those properties access steps then this mode behaves same as `%s`.<br>" +
                "In case the next step is one of the properties access steps with limited scope of properties, those properties will be<br>" +
                "pre-fetched together in the same multi-query.<br>" +
                "In case the next step is one of the properties access steps with unspecified scope of property keys then this mode<br>" +
                "behaves same as `%s`.<br>"+
                "- `%s` - Prefetch the same properties as with `%s`, but in case the next step is not<br>" +
                "`values`, `properties,` `valueMap`, `elementMap`, or `propertyMap` then acts like `%s`.<br>"+
                "- `%s` - Skips `has` step batch properties pre-fetch optimization.<br>",
            MultiQueryHasStepStrategyMode.ALL_PROPERTIES.getConfigName(),
            MultiQueryHasStepStrategyMode.REQUIRED_PROPERTIES_ONLY.getConfigName(),
            MultiQueryHasStepStrategyMode.REQUIRED_AND_NEXT_PROPERTIES.getConfigName(),
            MultiQueryHasStepStrategyMode.REQUIRED_PROPERTIES_ONLY.getConfigName(),
            MultiQueryHasStepStrategyMode.REQUIRED_PROPERTIES_ONLY.getConfigName(),
            MultiQueryHasStepStrategyMode.ALL_PROPERTIES.getConfigName(),
            MultiQueryHasStepStrategyMode.REQUIRED_AND_NEXT_PROPERTIES_OR_ALL.getConfigName(),
            MultiQueryHasStepStrategyMode.REQUIRED_AND_NEXT_PROPERTIES.getConfigName(),
            MultiQueryHasStepStrategyMode.ALL_PROPERTIES.getConfigName(),
            MultiQueryHasStepStrategyMode.NONE.getConfigName()),
        ConfigOption.Type.MASKABLE, MultiQueryHasStepStrategyMode.REQUIRED_AND_NEXT_PROPERTIES.getConfigName());

    public static final ConfigOption<String> PROPERTIES_BATCH_MODE = new ConfigOption<>(QUERY_BATCH_NS,"properties-mode",
        String.format("Properties pre-fetching mode for `values`, `properties`, `valueMap`, `propertyMap`, `elementMap` steps. Used only when `"+USE_MULTIQUERY.toStringWithoutRoot()+"` is `true`.<br>" +
                "Supported modes:<br>" +
                "- `%s` - Pre-fetch all vertex properties on non-singular property access (fetches all vertex properties in a single slice query). " +
                "On single property access this mode behaves the same as `%s` mode.<br>" +
                "- `%s` - Pre-fetch necessary vertex properties only (uses a separate slice query per each required property)<br>" +
                "- `%s` - Skips vertex properties pre-fetching optimization.<br>",
            MultiQueryPropertiesStrategyMode.ALL_PROPERTIES.getConfigName(),
            MultiQueryPropertiesStrategyMode.REQUIRED_PROPERTIES_ONLY.getConfigName(),
            MultiQueryPropertiesStrategyMode.REQUIRED_PROPERTIES_ONLY.getConfigName(),
            MultiQueryPropertiesStrategyMode.NONE.getConfigName()),
        ConfigOption.Type.MASKABLE, MultiQueryPropertiesStrategyMode.REQUIRED_PROPERTIES_ONLY.getConfigName());

    public static final ConfigOption<String> LABEL_STEP_BATCH_MODE = new ConfigOption<>(QUERY_BATCH_NS,"label-step-mode",
        String.format("Labels pre-fetching mode for `label()` step. Used only when `"+USE_MULTIQUERY.toStringWithoutRoot()+"` is `true`.<br>" +
                "Supported modes:<br>" +
                "- `%s` - Pre-fetch labels for all vertices in a batch.<br>" +
                "- `%s` - Skips vertex labels pre-fetching optimization.<br>",
            MultiQueryLabelStepStrategyMode.ALL.getConfigName(),
            MultiQueryLabelStepStrategyMode.NONE.getConfigName()),
        ConfigOption.Type.MASKABLE, MultiQueryLabelStepStrategyMode.ALL.getConfigName());

    public static final ConfigOption<String> DROP_STEP_BATCH_MODE = new ConfigOption<>(QUERY_BATCH_NS,"drop-step-mode",
        String.format("Batching mode for `drop()` step. Used only when `"+USE_MULTIQUERY.toStringWithoutRoot()+"` is `true`.<br>" +
                "Supported modes:<br>" +
                "- `%s` - Drops all vertices in a batch.<br>" +
                "- `%s` - Skips drop batching optimization.<br>",
            MultiQueryDropStepStrategyMode.ALL.getConfigName(),
            MultiQueryDropStepStrategyMode.NONE.getConfigName()),
        ConfigOption.Type.MASKABLE, MultiQueryDropStepStrategyMode.ALL.getConfigName());

    // ################ SCHEMA #######################
    // ################################################

    public static final ConfigNamespace SCHEMA_NS = new ConfigNamespace(ROOT_NS,"schema",
            "Schema related configuration options");

    public static final ConfigOption<String> AUTO_TYPE = new ConfigOption<>(SCHEMA_NS,"default",
            "Configures the DefaultSchemaMaker to be used by this graph."
            + " Either one of the following shorthands can be used: <br>"
            + " - `default` (a blueprints compatible schema maker with MULTI edge labels and SINGLE property keys),<br>"
            + " - `tp3` (same as default, but has LIST property keys),<br>"
            + " - `none` (automatic schema creation is disabled)<br>"
            + " - `ignore-prop` (same as none, but simply ignore unknown properties rather than throw exceptions)<br>"
            + " - or to the full package and classname of a custom/third-party implementing the"
            + " interface `org.janusgraph.core.schema.DefaultSchemaMaker`",
            ConfigOption.Type.MASKABLE, "default", new Predicate<String>() {
        @Override
        public boolean apply(@Nullable String s) {
            if (s==null) return false;
            if (PREREGISTERED_AUTO_TYPE.containsKey(s)) return true;
            try {
                Class<?> clazz = ClassUtils.getClass(s);
                return DefaultSchemaMaker.class.isAssignableFrom(clazz);
            } catch (ClassNotFoundException e) {
                return false;
            }
        }
    });

    private static final Map<String, DefaultSchemaMaker> PREREGISTERED_AUTO_TYPE = Collections.unmodifiableMap(
        new HashMap<String, DefaultSchemaMaker>(4){{
            put("none", DisableDefaultSchemaMaker.INSTANCE);
            put("ignore-prop", IgnorePropertySchemaMaker.INSTANCE);
            put("default", JanusGraphDefaultSchemaMaker.INSTANCE);
            put("tp3", Tp3DefaultSchemaMaker.INSTANCE);
        }}
    );

    public static final ConfigOption<Boolean> SCHEMA_MAKER_LOGGING = new ConfigOption<>(SCHEMA_NS, "logging",
            "Controls whether logging is enabled for schema makers. This only takes effect if you set `schema.default` " +
            "to `default` or `ignore-prop`. For `default` schema maker, warning messages will be logged before schema types " +
            "are created automatically. For `ignore-prop` schema maker, warning messages will be logged before unknown properties " +
            "are ignored.", ConfigOption.Type.MASKABLE, false);

    public static final ConfigOption<Boolean> SCHEMA_CONSTRAINTS = new ConfigOption<>(SCHEMA_NS, "constraints",
            "Configures the schema constraints to be used by this graph. If config 'schema.constraints' " +
            "is set to 'true' and 'schema.default' is set to 'none', then an 'IllegalArgumentException' is thrown for schema constraint violations. " +
            "If 'schema.constraints' is set to 'true' and 'schema.default' is not set 'none', schema constraints are automatically created "+
            "as described in the config option 'schema.default'. If 'schema.constraints' is set to 'false' which is the default, then no schema constraints are applied.",
            ConfigOption.Type.GLOBAL_OFFLINE, false);

    public static final ConfigNamespace SCHEMA_INIT = new ConfigNamespace(SCHEMA_NS,"init",
        "Configuration options for schema initialization on startup.");

    public static final ConfigOption<String> SCHEMA_INIT_STRATEGY = new ConfigOption<>(SCHEMA_INIT,"strategy",
        String.format("Specifies the strategy for schema initialization before starting JanusGraph. You must provide the full " +
                "class path of a class that implements the `%s` interface and has parameterless constructor.<br>" +
                "The following shortcuts are also available:<br>" +
                "- `%s` - Skips schema initialization.<br>" +
                "- `%s` - Schema initialization via provided JSON file or JSON string.<br>",
            SchemaInitStrategy.class.getSimpleName(),
            SchemaInitType.NONE.getConfigName(),
            SchemaInitType.JSON.getConfigName()),
        ConfigOption.Type.LOCAL, SchemaInitType.NONE.getConfigName());

    public static final ConfigOption<Boolean> SCHEMA_DROP_BEFORE_INIT = new ConfigOption<>(SCHEMA_INIT,"drop-before-startup",
        String.format("Drops the entire schema with graph data before JanusGraph schema initialization. " +
            "Note that the schema will be dropped regardless of the selected initialization strategy, " +
            "including when `%s` is set to `%s`.",
            SCHEMA_INIT_STRATEGY.toStringWithoutRoot(),
            SchemaInitType.NONE.getConfigName()),
        ConfigOption.Type.LOCAL, false);

    public static final ConfigNamespace SCHEMA_INIT_JSON = new ConfigNamespace(SCHEMA_INIT,"json",
        "Options for JSON schema initialization strategy.");

    public static final ConfigOption<String> SCHEMA_INIT_JSON_FILE = new ConfigOption<>(SCHEMA_INIT_JSON,"file",
        "File path to JSON formated schema definition.", ConfigOption.Type.LOCAL, String.class);

    public static final ConfigOption<String> SCHEMA_INIT_JSON_STR = new ConfigOption<>(SCHEMA_INIT_JSON,"string",
        "JSON formated schema definition string. This option takes precedence if both `file` and `string` are used.",
        ConfigOption.Type.LOCAL, String.class);

    public static final ConfigOption<Boolean> SCHEMA_INIT_JSON_SKIP_ELEMENTS = new ConfigOption<>(SCHEMA_INIT_JSON,"skip-elements",
        "Skip creation of VertexLabel, EdgeLabel, and PropertyKey.",
        ConfigOption.Type.LOCAL, false);

    public static final ConfigOption<Boolean> SCHEMA_INIT_JSON_SKIP_INDICES = new ConfigOption<>(SCHEMA_INIT_JSON,"skip-indices",
        "Skip creation of indices.",
        ConfigOption.Type.LOCAL, false);

    public static final ConfigOption<String> SCHEMA_INIT_JSON_INDICES_ACTIVATION_TYPE = new ConfigOption<>(SCHEMA_INIT_JSON,"indices-activation",
        String.format("Indices activation type:<br>" +
                "- `%s` - Reindex process will be triggered for any updated index. After this all updated indexes will be enabled.<br>" +
                "- `%s` - Reindex process will be triggered for any index which is not enabled (including previously created indices). After reindexing all indices will be enabled.<br>" +
                "- `%s` - Skip reindex process for any updated indexes.<br>" +
                "- `%s` - Force enable all updated indexes without running any reindex process (previous data may not be available for such indices).<br>" +
                "- `%s` - Force enable all indexes (including previously created indices) without running any reindex process (previous data may not be available for such indices).<br>",
            IndicesActivationType.REINDEX_AND_ENABLE_UPDATED_ONLY.getConfigName(),
            IndicesActivationType.REINDEX_AND_ENABLE_NON_ENABLED.getConfigName(),
            IndicesActivationType.SKIP_ACTIVATION.getConfigName(),
            IndicesActivationType.FORCE_ENABLE_UPDATED_ONLY.getConfigName(),
            IndicesActivationType.FORCE_ENABLE_NON_ENABLED.getConfigName()
        ),
        ConfigOption.Type.LOCAL, IndicesActivationType.REINDEX_AND_ENABLE_NON_ENABLED.getConfigName());

    public static final ConfigOption<Boolean> SCHEMA_INIT_JSON_FORCE_CLOSE_OTHER_INSTANCES = new ConfigOption<>(SCHEMA_INIT_JSON,"force-close-other-instances",
        String.format("Force closes other JanusGraph instances before schema initialization, regardless if they are active or not. " +
            "This is a dangerous operation. This option exists to help people initialize schema who struggle with zombie JanusGraph instances. " +
            "It's not recommended to be used unless you know what you are doing. Instead of this parameter, " +
            "it's recommended to check `%s` and `%s` options to not create zombie instances in the cluster.",
                UNIQUE_INSTANCE_ID.toStringWithoutRoot(),
                REPLACE_INSTANCE_IF_EXISTS.toStringWithoutRoot()
            ),
        ConfigOption.Type.LOCAL, false);

    public static final ConfigOption<Long> SCHEMA_INIT_JSON_AWAIT_INDEX_STATUS_TIMEOUT = new ConfigOption<>(SCHEMA_INIT_JSON,"await-index-status-timeout",
        "Timeout for awaiting index status operation defined in milliseconds. If the status await timeouts the exception will be thrown during schema initialization process.",
        ConfigOption.Type.LOCAL, TimeUnit.MINUTES.toMillis(3));

    // ################ CACHE #######################
    // ################################################

    public static final ConfigNamespace CACHE_NS = new ConfigNamespace(ROOT_NS,"cache","Configuration options that modify JanusGraph's caching behavior");

    public static final ConfigOption<Boolean> DB_CACHE = new ConfigOption<>(CACHE_NS,"db-cache",
            "Whether to enable JanusGraph's database-level cache, which is shared across all transactions. " +
            "Enabling this option speeds up traversals by holding hot graph elements in memory, " +
            "but also increases the likelihood of reading stale data.  Disabling it forces each " +
            "transaction to independently fetch graph elements from storage before reading/writing them.",
            ConfigOption.Type.MASKABLE, false);

    /**
     * The size of the database level cache.
     * If this value is between 0.0 (strictly bigger) and 1.0 (strictly smaller), then it is interpreted as a
     * percentage of the total heap space available to the JVM this JanusGraph instance is running in.
     * If this value is bigger than 1.0 it is interpreted as an absolute size in bytes.
     */
    public static final ConfigOption<Double> DB_CACHE_SIZE = new ConfigOption<>(CACHE_NS,"db-cache-size",
            "Size of JanusGraph's database level cache.  Values between 0 and 1 are interpreted as a percentage " +
            "of VM heap, while larger values are interpreted as an absolute size in bytes.",
            ConfigOption.Type.MASKABLE, 0.3);

    /**
     * How long the database level cache will keep keys expired while the mutations that triggered the expiration
     * are being persisted. This value should be larger than the time it takes for persisted mutations to become visible.
     * This setting only ever makes sense for distributed storage backends where writes may be accepted but are not
     * immediately readable.
     */
    public static final ConfigOption<Integer> DB_CACHE_CLEAN_WAIT = new ConfigOption<>(CACHE_NS,"db-cache-clean-wait",
            "How long, in milliseconds, database-level cache will keep entries after flushing them.  " +
            "This option is only useful on distributed storage backends that are capable of acknowledging writes " +
            "without necessarily making them immediately visible.",
            ConfigOption.Type.MASKABLE, 50);

    /**
     * The default expiration time for elements held in the database level cache. This is the time period before
     * JanusGraph will check against storage backend for a newer query answer.
     * Setting this value to 0 will cache elements forever (unless they get evicted due to space constraints). This only
     * makes sense when this is the only JanusGraph instance interacting with a storage backend.
     */
    public static final ConfigOption<Long> DB_CACHE_TIME = new ConfigOption<>(CACHE_NS,"db-cache-time",
            "Default expiration time, in milliseconds, for entries in the database-level cache. " +
            "Entries are evicted when they reach this age even if the cache has room to spare. " +
            "Set to 0 to disable expiration (cache entries live forever or until memory pressure " +
            "triggers eviction when set to 0).",
            ConfigOption.Type.MASKABLE, 10000L);

    /**
     * Configures the maximum number of recently-used vertices cached by a transaction. The smaller the cache size, the
     * less memory a transaction can consume at maximum. For many concurrent, long running transactions in memory constraint
     * environments, reducing the cache size can avoid OutOfMemory and GC limit exceeded exceptions.
     * Note, however, that all modifications in a transaction must always be kept in memory and hence this setting does not
     * have much impact on write intense transactions. Those must be split into smaller transactions in the case of memory errors.
     * <p>
     * The recently-used vertex cache can contain both dirty and clean vertices, that is, both vertices which have been
     * created or updated in the current transaction and vertices which have only been read in the current transaction.
     */
    public static final ConfigOption<Integer> TX_CACHE_SIZE = new ConfigOption<>(CACHE_NS,"tx-cache-size",
            "Maximum size of the transaction-level cache of recently-used vertices.",
            ConfigOption.Type.MASKABLE, 20000);

    /**
     * Configures the initial size of the dirty (modified) vertex map used by a transaction.  All vertices created or
     * updated by a transaction are held in that transaction's dirty vertex map until the transaction commits.
     * This option sets the initial size of the dirty map.  Unlike {@link #TX_CACHE_SIZE}, this is not a maximum.
     * The transaction will transparently allocate more space to store dirty vertices if this initial size hint
     * is exceeded.  Transactions that know how many vertices they are likely to modify a priori can avoid resize
     * costs associated with growing the dirty vertex data structure by setting this option.
     */
    public static final ConfigOption<Integer> TX_DIRTY_SIZE = new ConfigOption<>(CACHE_NS, "tx-dirty-size",
          "Initial size of the transaction-level cache of uncommitted dirty vertices. " +
          "This is a performance hint for write-heavy, performance-sensitive transactional workloads. " +
          "If set, it should roughly match the median vertices modified per transaction.",
          ConfigOption.Type.MASKABLE, Integer.class);

    /**
     * The default value of {@link #TX_DIRTY_SIZE} when batch loading is disabled.
     * This value is only considered if the user does not specify a value for
     * {@code #TX_DIRTY_CACHE_SIZE} explicitly in either the graph or transaction config.
     */
    private static final int TX_DIRTY_SIZE_DEFAULT_WITHOUT_BATCH = 32;

    /**
     * The default value of {@link #TX_DIRTY_SIZE} when batch loading is enabled.
     * This value is only considered if the user does not specify a value for
     * {@code #TX_DIRTY_CACHE_SIZE} explicitly in either the graph or transaction config.
     */
    private static final int TX_DIRTY_SIZE_DEFAULT_WITH_BATCH = 4096;


    // ################ STORAGE #######################
    // ################################################

    public static final ConfigNamespace STORAGE_NS = new ConfigNamespace(ROOT_NS,"storage","Configuration options for the storage backend.  Some options are applicable only for certain backends.");

    /**
     * Storage root directory for those storage backends that require local storage
     */
    public static final ConfigOption<String> STORAGE_ROOT = new ConfigOption<>(STORAGE_NS,"root",
            "Storage root directory for those storage backends that require local storage. " +
            "If you do not supply storage.directory and you do supply graph.graphname, then your data " +
            "will be stored in the directory equivalent to <STORAGE_ROOT>/<GRAPH_NAME>.",
            ConfigOption.Type.LOCAL, String.class);

    /**
     * Storage directory for those storage backends that require local storage
     */
    public static final ConfigOption<String> STORAGE_DIRECTORY = new ConfigOption<>(STORAGE_NS,"directory",
            "Storage directory for those storage backends that require local storage.",
            ConfigOption.Type.LOCAL, String.class);

    /**
     * Path to a configuration file for those storage backends that
     * require/support a separate config file
     */
    public static final ConfigOption<String> STORAGE_CONF_FILE = new ConfigOption<>(STORAGE_NS,"conf-file",
            "Path to a configuration file for those storage backends which require/support a single separate config file.",
            ConfigOption.Type.LOCAL, String.class);

    /**
     * Define the storage backed to use for persistence
     */
    public static final ConfigOption<String> STORAGE_BACKEND = new ConfigOption<>(STORAGE_NS,"backend",
            "The primary persistence provider used by JanusGraph.  This is required.  It should be set one of " +
            "JanusGraph's built-in shorthand names for its standard storage backends " +
            "(shorthands: " + String.join(", ", StandardStoreManager.getAllShorthands()) + ") " +
            "or to the full package and classname of a custom/third-party StoreManager implementation.",
            ConfigOption.Type.LOCAL, String.class);

    /**
     * Specifies whether this database is read-only, i.e. write operations are not supported
     */
    public static final ConfigOption<Boolean> STORAGE_READONLY = new ConfigOption<>(STORAGE_NS,"read-only",
            "Read-only database",
            ConfigOption.Type.LOCAL, false);

    /**
     * Enables batch loading which improves write performance but assumes that only one thread is interacting with
     * the graph
     */
    public static final ConfigOption<Boolean> STORAGE_BATCH = new ConfigOption<>(STORAGE_NS,"batch-loading",
            "Whether to enable batch loading into the storage backend",
            ConfigOption.Type.LOCAL, false);

    /**
     * Enables transactions on storage backends that support them
     */
    public static final ConfigOption<Boolean> STORAGE_TRANSACTIONAL = new ConfigOption<>(STORAGE_NS,"transactions",
            "Enables transactions on storage backends that support them",
            ConfigOption.Type.MASKABLE, true);

    public static final ConfigOption<Boolean> ASSIGN_TIMESTAMP = new ConfigOption<>(GRAPH_NS, "assign-timestamp",
            "Whether to use JanusGraph generated client-side timestamp in mutations if the backend supports it. " +
            "When enabled, JanusGraph assigns one timestamp to all insertions and another slightly earlier " +
            "timestamp to all deletions in the same batch. When this is disabled, mutation behavior depends on the backend. Some " +
            "might use server-side timestamp (e.g. HBase) while others might use client-side timestamp generated by driver (CQL).",
            ConfigOption.Type.LOCAL, Boolean.class, true);

    /**
     * Buffers graph mutations locally up to the specified number before persisting them against the storage backend.
     * Set to 0 to disable buffering. Buffering is disabled automatically if the storage backend does not support buffered mutations.
     */
    public static final ConfigOption<Integer> BUFFER_SIZE = new ConfigOption<>(STORAGE_NS,"buffer-size",
            "Size of the batch in which mutations are persisted",
            ConfigOption.Type.MASKABLE, 1024, ConfigOption.positiveInt());

    /**
     * Number of mutations in a transaction after which use parallel processing for transaction aggregations.
     * This might give a boost in transaction commit time.
     * Default value is 100.
     */
    public static final ConfigOption<Integer> NUM_MUTATIONS_PARALLEL_THRESHOLD = new ConfigOption<>(STORAGE_NS,"num-mutations-parallel-threshold",
        "This parameter determines the minimum number of mutations a transaction must have before parallel processing is applied during aggregation. " +
            "Leveraging parallel processing can enhance the commit times for transactions involving a large number of mutations. " +
            "However, it is advisable not to set the threshold too low (e.g., 0 or 1) due to the overhead associated with parallelism synchronization. " +
            "This overhead is more efficiently offset in the context of larger transactions.",
    ConfigOption.Type.MASKABLE, 100, ConfigOption.positiveInt());

    public static final ConfigOption<Duration> STORAGE_WRITE_WAITTIME = new ConfigOption<>(STORAGE_NS,"write-time",
            "Maximum time (in ms) to wait for a backend write operation to complete successfully. If a backend write operation " +
            "fails temporarily, JanusGraph will backoff exponentially and retry the operation until the wait time has been exhausted. ",
            ConfigOption.Type.MASKABLE, Duration.ofSeconds(100L));

    public static final ConfigOption<Duration> STORAGE_READ_WAITTIME = new ConfigOption<>(STORAGE_NS,"read-time",
            "Maximum time (in ms) to wait for a backend read operation to complete successfully. If a backend read operation " +
                    "fails temporarily, JanusGraph will backoff exponentially and retry the operation until the wait time has been exhausted. ",
            ConfigOption.Type.MASKABLE, Duration.ofSeconds(10L));

    /**
     * If enabled, JanusGraph attempts to parallelize storage operations against the storage backend using a fixed thread pool shared
     * across the entire JanusGraph graph database instance. Parallelization is only applicable to certain storage operations and
     * can be beneficial when the operation is I/O bound.
     */
    public static final ConfigOption<Boolean> PARALLEL_BACKEND_OPS = new ConfigOption<>(STORAGE_NS,"parallel-backend-ops",
            "Whether JanusGraph should attempt to parallelize storage operations",
            ConfigOption.Type.MASKABLE, true);

    public static final ConfigNamespace PARALLEL_BACKEND_EXECUTOR_SERVICE = new ConfigNamespace(
        STORAGE_NS,
        "parallel-backend-executor-service",
        "Configuration options for executor service which is used for parallel requests when `"+PARALLEL_BACKEND_OPS.toStringWithoutRoot()+"` is enabled.");

    public static final ConfigOption<Integer> PARALLEL_BACKEND_EXECUTOR_SERVICE_CORE_POOL_SIZE = new ConfigOption<>(
        PARALLEL_BACKEND_EXECUTOR_SERVICE,
        "core-pool-size",
        "Core pool size for executor service. May be ignored if custom executor service is used " +
            "(depending on the implementation of the executor service)."+
            "If not set or set to -1 the core pool size will be equal to number of processors multiplied by "
            +ExecutorServiceBuilder.THREAD_POOL_SIZE_SCALE_FACTOR+".",
        ConfigOption.Type.LOCAL,
        Integer.class);

    public static final ConfigOption<Integer> PARALLEL_BACKEND_EXECUTOR_SERVICE_MAX_POOL_SIZE = new ConfigOption<>(
        PARALLEL_BACKEND_EXECUTOR_SERVICE,
        "max-pool-size",
        "Maximum pool size for executor service. Ignored for `fixed` and `cached` executor services. " +
            "May be ignored if custom executor service is used (depending on the implementation of the executor service).",
        ConfigOption.Type.LOCAL,
        Integer.class,
        Integer.MAX_VALUE);

    public static final ConfigOption<Long> PARALLEL_BACKEND_EXECUTOR_SERVICE_KEEP_ALIVE_TIME = new ConfigOption<>(
        PARALLEL_BACKEND_EXECUTOR_SERVICE,
        "keep-alive-time",
        "Keep alive time in milliseconds for executor service. When the number of threads is greater than the `"+
            PARALLEL_BACKEND_EXECUTOR_SERVICE_CORE_POOL_SIZE.getName()+
            "`, this is the maximum time that excess idle threads will wait for new tasks before terminating. " +
            "Ignored for `fixed` executor service and may be ignored if custom executor service is used " +
            "(depending on the implementation of the executor service).",
        ConfigOption.Type.LOCAL,
        Long.class,
        60000L);

    public static final ConfigOption<String> PARALLEL_BACKEND_EXECUTOR_SERVICE_CLASS = new ConfigOption<>(
        PARALLEL_BACKEND_EXECUTOR_SERVICE,
        "class",
        "The implementation of `ExecutorService` to use. " +
            "The full name of the class which extends `"+ ExecutorService.class.getSimpleName()+"` which has either " +
            "a public constructor with `"+ ExecutorServiceConfiguration.class.getSimpleName()+"` argument (preferred constructor) or " +
            "a public parameterless constructor. Other accepted options are: `fixed` - fixed thread pool of size `"
            +PARALLEL_BACKEND_EXECUTOR_SERVICE_CORE_POOL_SIZE.getName()+"`; `cached` - cached thread pool;",
        ConfigOption.Type.LOCAL,
        String.class,
        ExecutorServiceBuilder.FIXED_THREAD_POOL_CLASS);

    public static final ConfigOption<Long> PARALLEL_BACKEND_EXECUTOR_SERVICE_MAX_SHUTDOWN_WAIT_TIME = new ConfigOption<>(
        PARALLEL_BACKEND_EXECUTOR_SERVICE,
        "max-shutdown-wait-time",
        "Max shutdown wait time in milliseconds for executor service threads to be finished during shutdown. " +
            "After this time threads will be interrupted (signalled with interrupt) without any additional wait time.",
        ConfigOption.Type.LOCAL,
        Long.class,
        60000L);

    public static final ConfigOption<String[]> STORAGE_HOSTS = new ConfigOption<>(STORAGE_NS,"hostname",
            "The hostname or comma-separated list of hostnames of storage backend servers.  " +
            "This is only applicable to some storage backends, such as cassandra and hbase.",
            ConfigOption.Type.LOCAL, new String[]{NetworkUtil.getLoopbackAddress()});

    /**
     * Configuration key for the port on which to connect to remote storage backend servers.
     */
    public static final ConfigOption<Integer> STORAGE_PORT = new ConfigOption<>(STORAGE_NS,"port",
            "The port on which to connect to storage backend servers. For HBase, it is the Zookeeper port.",
            ConfigOption.Type.LOCAL, Integer.class);

    /**
     * Username and password keys to be used to specify an access credential that may be needed to connect
     * with a secured storage backend.
     */
    public static final ConfigOption<String> AUTH_USERNAME = new ConfigOption<>(STORAGE_NS,"username",
            "Username to authenticate against backend",
            ConfigOption.Type.LOCAL, String.class);
    public static final ConfigOption<String> AUTH_PASSWORD = new ConfigOption<>(STORAGE_NS,"password",
            "Password to authenticate against backend",
            ConfigOption.Type.LOCAL, String.class);

    /**
     * Default timeout when connecting to a remote database instance
     * <p>
     */
    public static final ConfigOption<Duration> CONNECTION_TIMEOUT = new ConfigOption<>(STORAGE_NS,"connection-timeout",
            "Default timeout, in milliseconds, when connecting to a remote database instance",
            ConfigOption.Type.MASKABLE, Duration.ofMillis(10000L));

    /**
     * Time in milliseconds for backend manager to wait for the storage backends to
     * become available when JanusGraph is run in server mode. Should the backend manager
     * experience exceptions when attempting to access the storage backend it will retry
     * until this timeout is exceeded.
     * <p>
     * A wait time of 0 disables waiting.
     * <p>
     */
    public static final ConfigOption<Duration> SETUP_WAITTIME = new ConfigOption<>(STORAGE_NS,"setup-wait",
            "Time in milliseconds for backend manager to wait for the storage backends to become available when JanusGraph is run in server mode",
            ConfigOption.Type.MASKABLE, Duration.ofMillis(60000L));

    /**
     * Default number of results to pull over the wire when iterating over a distributed
     * storage backend.
     * This is batch size of results to pull when iterating a result set.
     */
    public static final ConfigOption<Integer> PAGE_SIZE = new ConfigOption<>(STORAGE_NS,"page-size",
            "JanusGraph break requests that may return many results from distributed storage backends " +
            "into a series of requests for small chunks/pages of results, where each chunk contains " +
            "up to this many elements.",
            ConfigOption.Type.MASKABLE, 100);

    public static final ConfigOption<Integer> KEYS_SIZE = new ConfigOption<>(STORAGE_NS,"keys-size",
        "The maximum amount of keys/partitions to retrieve from distributed storage system by JanusGraph in a single request.",
        ConfigOption.Type.MASKABLE, 100);

    public static final ConfigOption<Boolean> DROP_ON_CLEAR = new ConfigOption<>(STORAGE_NS, "drop-on-clear",
            "Whether to drop the graph database (true) or delete rows (false) when clearing storage. " +
            "Note that some backends always drop the graph database when clearing storage. Also note that indices are " +
            "always dropped when clearing storage.",
            ConfigOption.Type.MASKABLE, true);

    public static final ConfigNamespace LOCK_NS =
            new ConfigNamespace(STORAGE_NS, "lock", "Options for locking on eventually-consistent stores");

    /**
     * Number of times the system attempts to acquire a lock before giving up and throwing an exception.
     */
    public static final ConfigOption<Integer> LOCK_RETRY = new ConfigOption<>(LOCK_NS, "retries",
            "Number of times the system attempts to acquire a lock before giving up and throwing an exception",
            ConfigOption.Type.MASKABLE, 3);

    public static final ConfigOption<Duration> LOCK_WAIT = new ConfigOption<>(LOCK_NS, "wait-time",
            "Number of milliseconds the system waits for a lock application to be acknowledged by the storage backend. " +
            "Also, the time waited at the end of all lock applications before verifying that the applications were successful. " +
            "This value should be a small multiple of the average consistent write time. Although this value is maskable, it is " +
            "highly recommended to use the same value across JanusGraph instances in production environments.",
            ConfigOption.Type.MASKABLE, Duration.ofMillis(100L));

    /**
     * Number of milliseconds after which a lock is considered to have expired. Lock applications that were not released
     * are considered expired after this time and released.
     * This value should be larger than the maximum time a transaction can take in order to guarantee that no correctly
     * held applications are expired pre-maturely and as small as possible to avoid dead lock.
     */
    public static final ConfigOption<Duration> LOCK_EXPIRE = new ConfigOption<>(LOCK_NS, "expiry-time",
            "Number of milliseconds after which a lock is considered to have expired. " +
            "Lock applications that were not released are considered expired after this time and released. " +
            "This value should be larger than the maximum time a transaction can take in order to guarantee " +
            "that no correctly held applications are expired pre-maturely and as small as possible to avoid dead lock.",
            ConfigOption.Type.GLOBAL_OFFLINE, Duration.ofMillis(300 * 1000L));

    /**
     * Whether to attempt to delete expired locks from the storage backend. True
     * will attempt to delete expired locks in a background daemon thread. False
     * will never attempt to delete expired locks. This option is only
     * meaningful for the default lock backend.
     *
     * @see #LOCK_BACKEND
     */
    public static final ConfigOption<Boolean> LOCK_CLEAN_EXPIRED = new ConfigOption<>(LOCK_NS, "clean-expired",
            "Whether to delete expired locks from the storage backend",
            ConfigOption.Type.MASKABLE, false);

    /**
     * Locker type to use.  The supported types are in {@link org.janusgraph.diskstorage.Backend}.
     */
    public static final ConfigOption<String> LOCK_BACKEND = new ConfigOption<>(LOCK_NS, "backend",
            "Locker type to use",
            ConfigOption.Type.GLOBAL_OFFLINE, "consistentkey");

    /**
     * Configuration setting key for the local lock mediator prefix
     */
    public static final ConfigOption<String> LOCK_LOCAL_MEDIATOR_GROUP =
            new ConfigOption<>(LOCK_NS, "local-mediator-group",
            "This option determines the LocalLockMediator instance used for early detection of lock contention " +
            "between concurrent JanusGraph graph instances within the same process which are connected to the same " +
            "storage backend.  JanusGraph instances that have the same value for this variable will attempt to discover " +
            "lock contention among themselves in memory before proceeding with the general-case distributed locking " +
            "code.  JanusGraph generates an appropriate default value for this option at startup.  Overriding " +
            "the default is generally only useful in testing.", ConfigOption.Type.LOCAL, String.class);


    // ################ STORAGE - META #######################

    public static final ConfigNamespace STORE_META_NS = new ConfigNamespace(STORAGE_NS,"meta","Meta data to include in storage backend retrievals",true);

    public static final ConfigOption<Boolean> STORE_META_TIMESTAMPS = new ConfigOption<>(STORE_META_NS,"timestamps",
            "Whether to include timestamps in retrieved entries for storage backends that automatically annotated entries with timestamps. " +
            "If enabled, timestamp can be retrieved by `element.value(ImplicitKey.TIMESTAMP.name())` or equivalently, " +
            "`element.value(\"" + ImplicitKey.TIMESTAMP.name() + "\")`.",
            ConfigOption.Type.GLOBAL, false);

    public static final ConfigOption<Boolean> STORE_META_TTL = new ConfigOption<>(STORE_META_NS,"ttl",
            "Whether to include ttl in retrieved entries for storage backends that support storage and retrieval of cell level TTL. " +
            "If enabled, ttl can be retrieved by `element.value(ImplicitKey.TTL.name())` or equivalently, " +
            "`element.value(\"" + ImplicitKey.TTL.name() + "\")`.",
            ConfigOption.Type.GLOBAL, false);

    public static final ConfigOption<Boolean> STORE_META_VISIBILITY = new ConfigOption<>(STORE_META_NS,"visibility",
            "Whether to include visibility in retrieved entries for storage backends that support cell level visibility. " +
            "If enabled, visibility can be retrieved by `element.value(ImplicitKey.VISIBILITY.name())` or equivalently, " +
            "`element.value(\"" + ImplicitKey.VISIBILITY.name() + "\")`.",
            ConfigOption.Type.GLOBAL, true);


    // ################ CLUSTERING ###########################
    // ################################################

    public static final ConfigNamespace CLUSTER_NS = new ConfigNamespace(ROOT_NS,"cluster","Configuration options for multi-machine deployments");

    public static final ConfigOption<Integer> CLUSTER_MAX_PARTITIONS = new ConfigOption<>(CLUSTER_NS,"max-partitions",
            "The number of virtual partition blocks created in the partitioned graph. This should be larger than the maximum expected number of nodes" +
                    " in the JanusGraph graph cluster. Must be greater than 1 and a power of 2.",
            ConfigOption.Type.FIXED, 32, integer -> integer!=null && integer>1 && NumberUtil.isPowerOf2(integer));



    // ################ IDS ###########################
    // ################################################

    public static final ConfigNamespace IDS_NS = new ConfigNamespace(ROOT_NS,"ids","General configuration options for graph element IDs");

    /**
     * Size of the block to be acquired. Larger block sizes require fewer block applications but also leave a larger
     * fraction of the id pool occupied and potentially lost. For write heavy applications, larger block sizes should
     * be chosen.
     */
    public static final ConfigOption<Integer> IDS_BLOCK_SIZE = new ConfigOption<>(IDS_NS,"block-size",
            "Globally reserve graph element IDs in chunks of this size.  Setting this too low will make commits " +
            "frequently block on slow reservation requests.  Setting it too high will result in IDs wasted when a " +
            "graph instance shuts down with reserved but mostly-unused blocks.",
            ConfigOption.Type.GLOBAL_OFFLINE, 10000);

    /**
     * The name of the ID store. Currently this defaults to janusgraph_ids. You can override the ID store to
     * facilitate migration from JanusGraph's predecessor, Titan. Previously, this KCVStore was named titan_ids.
     */
    public static final ConfigOption<String> IDS_STORE_NAME = new ConfigOption<>(IDS_NS, "store-name",
        "The name of the ID KCVStore. IDS_STORE_NAME is meant to be used only for backward compatibility with Titan, " +
            "and should not be used explicitly in normal operations or in new graphs.",
        ConfigOption.Type.GLOBAL_OFFLINE, JanusGraphConstants.JANUSGRAPH_ID_STORE_NAME);


    /**
     * If flush ids is enabled, vertices and edges are assigned ids immediately upon creation. If not, then ids are only
     * assigned when the transaction is committed.
     */
    public static final ConfigOption<Boolean> IDS_FLUSH = new ConfigOption<>(IDS_NS,"flush",
            "When true, vertices and edges are assigned IDs immediately upon creation.  When false, " +
            "IDs are assigned only when the transaction commits.",
            ConfigOption.Type.MASKABLE, true);

    /**
     * The number of milliseconds that the JanusGraph id pool manager will wait before giving up on allocating a new block
     * of ids. Note, that failure to allocate a new id block will cause the entire database to fail, hence this value
     * should be set conservatively. Choose a high value if there is a lot of contention around id allocation.
     */
    public static final ConfigOption<Duration> IDS_RENEW_TIMEOUT = new ConfigOption<>(IDS_NS,"renew-timeout",
            "The number of milliseconds that the JanusGraph id pool manager will wait before giving up on allocating a new block of ids",
            ConfigOption.Type.MASKABLE, Duration.ofMillis(120000L));

    /**
     * Configures when the id pool manager will attempt to allocate a new id block. When all but the configured percentage
     * of the current block is consumed, a new block will be allocated. Larger values should be used if a lot of ids
     * are allocated in a short amount of time. Value must be in (0,1].
     */
    public static final ConfigOption<Double> IDS_RENEW_BUFFER_PERCENTAGE = new ConfigOption<>(IDS_NS,"renew-percentage",
            "When the most-recently-reserved ID block has only this percentage of its total IDs remaining " +
            "(expressed as a value between 0 and 1), JanusGraph asynchronously begins reserving another block. " +
            "This helps avoid transaction commits waiting on ID reservation even if the block size is relatively small.",
            ConfigOption.Type.MASKABLE, 0.3);

    // ################ IDAUTHORITY ###################
    // ################################################

    public static final ConfigNamespace IDAUTHORITY_NS = new ConfigNamespace(IDS_NS,"authority","Configuration options for graph element ID reservation/allocation");

    /**
     * The number of milliseconds the system waits for an id block application to be acknowledged by the storage backend.
     * Also, the time waited after the application before verifying that the application was successful.
     */
    public static final ConfigOption<Duration> IDAUTHORITY_WAIT = new ConfigOption<>(IDAUTHORITY_NS,"wait-time",
            "The number of milliseconds the system waits for an ID block reservation to be acknowledged by the storage backend",
            ConfigOption.Type.GLOBAL_OFFLINE, Duration.ofMillis(300L));

    /**
     * Sets the strategy used by {@link ConsistentKeyIDAuthority} to avoid
     * contention in ID block allocation between JanusGraph instances concurrently
     * sharing a single distributed storage backend.
     */
    // This is set to GLOBAL_OFFLINE as opposed to MASKABLE or GLOBAL to prevent mixing both global-randomized and local-manual modes within the same cluster
    public static final ConfigOption<ConflictAvoidanceMode> IDAUTHORITY_CONFLICT_AVOIDANCE = new ConfigOption<>(IDAUTHORITY_NS,"conflict-avoidance-mode",
            "This setting helps separate JanusGraph instances sharing a single graph storage backend avoid contention when reserving ID blocks, " +
            "increasing overall throughput.",
            ConfigOption.Type.GLOBAL_OFFLINE, ConflictAvoidanceMode.class, ConflictAvoidanceMode.NONE);

    /**
     * When JanusGraph allocates IDs with {@link org.janusgraph.diskstorage.idmanagement.ConflictAvoidanceMode#GLOBAL_AUTO}
     * configured, it picks a random unique ID marker and attempts to allocate IDs
     * from a partition using the marker. The ID markers function as
     * subpartitions with each ID partition. If the attempt fails because that
     * partition + unique id combination is already completely allocated, then
     * JanusGraph will generate a new random unique ID and try again. This controls
     * the maximum number of attempts before JanusGraph assumes the entire partition
     * is allocated and fails the request. It must be set to at least 1 and
     * should generally be set to 3 or more.
     * <p>
     * This setting has no effect when {@link #IDAUTHORITY_CONFLICT_AVOIDANCE} is not configured to
     * {@link org.janusgraph.diskstorage.idmanagement.ConflictAvoidanceMode#GLOBAL_AUTO}.
     */
    public static final ConfigOption<Integer> IDAUTHORITY_CAV_RETRIES = new ConfigOption<>(IDAUTHORITY_NS,"randomized-conflict-avoidance-retries",
            "Number of times the system attempts ID block reservations with random conflict avoidance tags before giving up and throwing an exception",
            ConfigOption.Type.MASKABLE, 5);

    /**
     * Configures the number of bits of JanusGraph assigned ids that are reserved for a unique id marker that
     * allows the id allocation to be scaled over multiple sub-clusters and to reduce race-conditions
     * when a lot of JanusGraph instances attempt to allocate ids at the same time (e.g. during parallel bulk loading)
     *
     * IMPORTANT: This should never ever, ever be modified from its initial value and ALL JanusGraph instances must use the
     * same value. Otherwise, data corruption will occur.
     *
     * This setting has no effect when {@link #IDAUTHORITY_CONFLICT_AVOIDANCE} is configured to
     * {@link org.janusgraph.diskstorage.idmanagement.ConflictAvoidanceMode#NONE}. However, note that while the
     * conflict avoidance mode can be changed, this setting cannot ever be changed and must therefore be considered a priori.
     */
    public static final ConfigOption<Integer> IDAUTHORITY_CAV_BITS = new ConfigOption<>(IDAUTHORITY_NS,"conflict-avoidance-tag-bits",
            "Configures the number of bits of JanusGraph-assigned element IDs that are reserved for the conflict avoidance tag",
            ConfigOption.Type.FIXED, 4 , uniqueIdBitWidth -> uniqueIdBitWidth>=0 && uniqueIdBitWidth<=16);

    /**
     * Unique id marker to be used by this JanusGraph instance when allocating ids. The unique id marker
     * must be non-negative and fit within the number of unique id bits configured.
     * By assigning different unique id markers to individual JanusGraph instances it can be assured
     * that those instances don't conflict with one another when attempting to allocate new id blocks.
     *
     * IMPORTANT: The configured unique id marker must fit within the configured unique id bit width.
     *
     * This setting has no effect when {@link #IDAUTHORITY_CONFLICT_AVOIDANCE} is configured to
     * {@link org.janusgraph.diskstorage.idmanagement.ConflictAvoidanceMode#NONE}.
     */
    public static final ConfigOption<Integer> IDAUTHORITY_CAV_TAG = new ConfigOption<>(IDAUTHORITY_NS,"conflict-avoidance-tag",
            "Conflict avoidance tag to be used by this JanusGraph instance when allocating IDs",
            ConfigOption.Type.LOCAL, 0);


    // ############## External Index ######################
    // ################################################

    public static final ConfigNamespace INDEX_NS = new ConfigNamespace(ROOT_NS,"index","Configuration options for the individual indexing backends",true);


    /**
     * Define the indexing backed to use for index support
     */
    public static final ConfigOption<String> INDEX_BACKEND = new ConfigOption<>(INDEX_NS,"backend",
            "The indexing backend used to extend and optimize JanusGraph's query functionality. " +
            "This setting is optional.  JanusGraph can use multiple heterogeneous index backends.  " +
            "Hence, this option can appear more than once, so long as the user-defined name between " +
            "\"" + INDEX_NS.getName() + "\" and \"backend\" is unique among appearances." +
            "Similar to the storage backend, this should be set to one of " +
            "JanusGraph's built-in shorthand names for its standard index backends " +
            "(shorthands: " + String.join(", ", StandardIndexProvider.getAllShorthands()) + ") " +
            "or to the full package and classname of a custom/third-party IndexProvider implementation.",
            ConfigOption.Type.GLOBAL_OFFLINE, "elasticsearch");

    public static final ConfigOption<String> INDEX_DIRECTORY = new ConfigOption<>(INDEX_NS,"directory",
            "Directory to store index data locally",
            ConfigOption.Type.MASKABLE, String.class);

    public static final ConfigOption<String> INDEX_NAME = new ConfigOption<>(INDEX_NS,"index-name",
            "Name of the index if required by the indexing backend",
            ConfigOption.Type.GLOBAL_OFFLINE, "janusgraph");

    public static final ConfigOption<String[]> INDEX_HOSTS = new ConfigOption<>(INDEX_NS,"hostname",
            "The hostname or comma-separated list of hostnames of index backend servers.  " +
            "This is only applicable to some index backends, such as elasticsearch and solr.",
            ConfigOption.Type.MASKABLE, new String[]{NetworkUtil.getLoopbackAddress()});

    public static final ConfigOption<Integer> INDEX_PORT = new ConfigOption<>(INDEX_NS,"port",
            "The port on which to connect to index backend servers",
            ConfigOption.Type.MASKABLE, Integer.class);

    public static final ConfigOption<String> INDEX_CONF_FILE = new ConfigOption<>(INDEX_NS,"conf-file",
            "Path to a configuration file for those indexing backends that require/support a separate config file",
            ConfigOption.Type.MASKABLE, String.class);

    public static final ConfigOption<Integer> INDEX_MAX_RESULT_SET_SIZE = new ConfigOption<>(INDEX_NS, "max-result-set-size",
            "Maximum number of results to return if no limit is specified. For index backends that support scrolling, it represents " +
                    "the number of results in each batch",
            ConfigOption.Type.MASKABLE, 50);

    public static final ConfigOption<Boolean> INDEX_NAME_MAPPING = new ConfigOption<>(INDEX_NS,"map-name",
            "Whether to use the name of the property key as the field name in the index. It must be ensured, that the " +
                    "indexed property key names are valid field names. Renaming the property key will NOT rename the field " +
                    "and its the developers responsibility to avoid field collisions.",
            ConfigOption.Type.GLOBAL, true);


    // ############## Logging System ######################
    // ################################################

    public static final ConfigNamespace LOG_NS = new ConfigNamespace(GraphDatabaseConfiguration.ROOT_NS,"log","Configuration options for JanusGraph's logging system",true);

    public static final String MANAGEMENT_LOG = "janusgraph";
    public static final String TRANSACTION_LOG = "tx";
    public static final String USER_LOG = "user";
    public static final String USER_LOG_PREFIX = "ulog_";

    public static final Duration TRANSACTION_LOG_DEFAULT_TTL = Duration.ofDays(7);

    public static final ConfigOption<String> LOG_BACKEND = new ConfigOption<>(LOG_NS,"backend",
            "Define the log backend to use. A reserved shortcut `default` can be used to use graph's storage backend to manage logs. " +
                "A custom log implementation can be specified by providing " +
                "full class path which implements " +
                "`org.janusgraph.diskstorage.log.LogManager` and accepts a single parameter " +
                "`org.janusgraph.diskstorage.configuration.Configuration` in the public constructor.",
            ConfigOption.Type.GLOBAL_OFFLINE, "default");

    public static final ConfigOption<Integer> LOG_NUM_BUCKETS = new ConfigOption<>(LOG_NS,"num-buckets",
            "The number of buckets to split log entries into for load balancing",
            ConfigOption.Type.GLOBAL_OFFLINE, 1, ConfigOption.positiveInt());

    public static final ConfigOption<Integer> LOG_SEND_BATCH_SIZE = new ConfigOption<>(LOG_NS,"send-batch-size",
            "Maximum number of log messages to batch up for sending for logging implementations that support batch sending",
            ConfigOption.Type.MASKABLE, 256, ConfigOption.positiveInt());

    public static final ConfigOption<Integer> LOG_READ_BATCH_SIZE = new ConfigOption<>(LOG_NS,"read-batch-size",
            "Maximum number of log messages to read at a time for logging implementations that read messages in batches",
            ConfigOption.Type.MASKABLE, 1024, ConfigOption.positiveInt());

    public static final ConfigOption<Duration> LOG_SEND_DELAY = new ConfigOption<>(LOG_NS,"send-delay",
            "Maximum time in ms that messages can be buffered locally before sending in batch",
            ConfigOption.Type.MASKABLE, Duration.ofMillis(1000L));

    public static final ConfigOption<Duration> LOG_READ_INTERVAL = new ConfigOption<>(LOG_NS,"read-interval",
            "Time in ms between message readings from the backend for this logging implementations that read message in batch",
            ConfigOption.Type.MASKABLE, Duration.ofMillis(5000L));

    public static final ConfigOption<Integer> LOG_READ_THREADS = new ConfigOption<>(LOG_NS,"read-threads",
            "Number of threads to be used in reading and processing log messages",
            ConfigOption.Type.MASKABLE, 1, ConfigOption.positiveInt());

    public static final ConfigOption<Duration> LOG_STORE_TTL = new ConfigOption<Duration>(LOG_NS,"ttl",
            "Sets a TTL on all log entries, meaning " +
                    "that all entries added to this log expire after the configured amount of time. Requires " +
                    "that the log implementation supports TTL.",
            ConfigOption.Type.GLOBAL, Duration.class, sd -> null != sd && !sd.isZero());

    // ############## Attributes ######################
    // ################################################

    public static final ConfigNamespace ATTRIBUTE_NS = new ConfigNamespace(ROOT_NS,"attributes","Configuration options for attribute handling");

    public static final ConfigNamespace CUSTOM_ATTRIBUTE_NS = new ConfigNamespace(ATTRIBUTE_NS,"custom","Custom attribute serialization and handling",true);

    public static final String ATTRIBUTE_PREFIX = "attribute";

    public static final ConfigOption<String> CUSTOM_ATTRIBUTE_CLASS = new ConfigOption<>(CUSTOM_ATTRIBUTE_NS,"attribute-class",
            "Class of the custom attribute to be registered",
            ConfigOption.Type.GLOBAL_OFFLINE, String.class);
    public static final ConfigOption<String> CUSTOM_SERIALIZER_CLASS = new ConfigOption<>(CUSTOM_ATTRIBUTE_NS,"serializer-class",
            "Class of the custom attribute serializer to be registered",
            ConfigOption.Type.GLOBAL_OFFLINE, String.class);

    // ################ Metrics #######################
    // ################################################

    /**
     * Configuration key prefix for Metrics.
     */
    public static final ConfigNamespace METRICS_NS = new ConfigNamespace(ROOT_NS,"metrics","Configuration options for metrics reporting");

    /**
     * Whether to enable basic timing and operation count monitoring on backend
     * methods using the {@code com.codahale.metrics} package.
     */
    public static final ConfigOption<Boolean> BASIC_METRICS = new ConfigOption<>(METRICS_NS,"enabled",
            "Whether to enable basic timing and operation count monitoring on backend",
            ConfigOption.Type.MASKABLE, false);

    /**
     * This is the prefix used outside of a graph database configuration, or for
     * operations where a system-internal transaction is necessary as an
     * implementation detail. It currently can't be modified, though there is no
     * substantial technical obstacle preventing it from being configured --
     * some kind of configuration object is in scope everywhere it is used, and
     * it could theoretically be stored in and read from that object.
     */
    public static final String METRICS_PREFIX_DEFAULT = "org.janusgraph";
    public static final String METRICS_SYSTEM_PREFIX_DEFAULT = METRICS_PREFIX_DEFAULT + "." + "sys";
    public static final String METRICS_SCHEMA_PREFIX_DEFAULT = METRICS_SYSTEM_PREFIX_DEFAULT + "." + "schema";

    /**
     * The default name prefix for Metrics reported by JanusGraph. All metric names
     * will begin with this string and a period. This value can be overridden on
     * a transaction-specific basis through
     * {@link StandardTransactionBuilder#groupName(String)}.
     * <p>
     * Default = {@literal #METRICS_PREFIX_DEFAULT}
     */
    public static final ConfigOption<String> METRICS_PREFIX = new ConfigOption<>(METRICS_NS,"prefix",
            "The default name prefix for Metrics reported by JanusGraph.",
            ConfigOption.Type.MASKABLE, METRICS_PREFIX_DEFAULT);

    /**
     * Whether to aggregate measurements for the edge store, vertex index, edge
     * index, and ID store.
     * <p>
     * If true, then metrics for each of these backends will use the same metric
     * name ("stores"). All of their measurements will be combined. This setting
     * measures the sum of JanusGraph's backend activity without distinguishing
     * between contributions of its various internal stores.
     * <p>
     * If false, then metrics for each of these backends will use a unique
     * metric name ("idStore", "edgeStore", "vertexIndex", and "edgeIndex").
     * This setting exposes the activity associated with each backend component,
     * but it also multiplies the number of measurements involved by four.
     * <p>
     * This option has no effect when {@link #BASIC_METRICS} is false.
     */
    public static final ConfigOption<Boolean> METRICS_MERGE_STORES = new ConfigOption<>(METRICS_NS,"merge-stores",
            "Whether to aggregate measurements for the edge store, vertex index, edge index, and ID store",
            ConfigOption.Type.MASKABLE, true);

    public static final ConfigNamespace METRICS_CONSOLE_NS = new ConfigNamespace(METRICS_NS,"console","Configuration options for metrics reporting to console");


    /**
     * Metrics console reporter interval in milliseconds. Leaving this
     * configuration key absent or null disables the console reporter.
     */
    public static final ConfigOption<Duration> METRICS_CONSOLE_INTERVAL = new ConfigOption<>(METRICS_CONSOLE_NS,"interval",
            "Time between Metrics reports printing to the console, in milliseconds",
            ConfigOption.Type.MASKABLE, Duration.class);

    public static final ConfigNamespace METRICS_CSV_NS = new ConfigNamespace(METRICS_NS,"csv","Configuration options for metrics reporting to CSV file");

    /**
     * Metrics CSV reporter interval in milliseconds. Leaving this configuration
     * key absent or null disables the CSV reporter.
     */
    public static final ConfigOption<Duration> METRICS_CSV_INTERVAL = new ConfigOption<>(METRICS_CSV_NS,"interval",
            "Time between dumps of CSV files containing Metrics data, in milliseconds",
            ConfigOption.Type.MASKABLE, Duration.class);

    /**
     * Metrics CSV output directory. It will be created if it doesn't already
     * exist. This option must be non-null if {@link #METRICS_CSV_INTERVAL} is
     * non-null. This option has no effect if {@code #METRICS_CSV_INTERVAL} is
     * null.
     */
    public static final ConfigOption<String> METRICS_CSV_DIR = new ConfigOption<>(METRICS_CSV_NS,"directory",
            "Metrics CSV output directory",
            ConfigOption.Type.MASKABLE, String.class);

    public static final ConfigNamespace METRICS_JMX_NS = new ConfigNamespace(METRICS_NS,"jmx","Configuration options for metrics reporting through JMX");

    /**
     * Whether to report Metrics through a JMX MBean.
     */
    public static final ConfigOption<Boolean> METRICS_JMX_ENABLED = new ConfigOption<>(METRICS_JMX_NS,"enabled",
            "Whether to report Metrics through a JMX MBean",
            ConfigOption.Type.MASKABLE, false);

    /**
     * The JMX domain in which to report Metrics. If null, then Metrics applies
     * its default value.
     */
    public static final ConfigOption<String> METRICS_JMX_DOMAIN = new ConfigOption<>(METRICS_JMX_NS,"domain",
            "The JMX domain in which to report Metrics",
            ConfigOption.Type.MASKABLE, String.class);

    /**
     * The JMX agentId through which to report Metrics. Calling
     * {@link MBeanServerFactory#findMBeanServer(String)} on this value must
     * return exactly one {@code MBeanServer} at runtime. If null, then Metrics
     * applies its default value.
     */
    public static final ConfigOption<String> METRICS_JMX_AGENTID = new ConfigOption<>(METRICS_JMX_NS,"agentid",
            "The JMX agentId used by Metrics",
            ConfigOption.Type.MASKABLE, String.class);

    public static final ConfigNamespace METRICS_SLF4J_NS = new ConfigNamespace(METRICS_NS,"slf4j","Configuration options for metrics reporting through slf4j");

    /**
     * Metrics Slf4j reporter interval in milliseconds. Leaving this
     * configuration key absent or null disables the Slf4j reporter.
     */
    public static final ConfigOption<Duration> METRICS_SLF4J_INTERVAL = new ConfigOption<>(METRICS_SLF4J_NS,"interval",
            "Time between slf4j logging reports of Metrics data, in milliseconds",
            ConfigOption.Type.MASKABLE, Duration.class);

    /**
     * The complete name of the Logger through which Metrics will report via
     * Slf4j. If non-null, then Metrics will be dumped on
     * {@link LoggerFactory#getLogger(String)} with the configured value as the
     * argument. If null, then Metrics will use its default Slf4j logger.
     */
    public static final ConfigOption<String> METRICS_SLF4J_LOGGER = new ConfigOption<>(METRICS_SLF4J_NS,"logger",
            "The complete name of the Logger through which Metrics will report via Slf4j",
            ConfigOption.Type.MASKABLE, String.class);

    /**
     * The configuration namespace within {@link #METRICS_NS} for
     * Graphite.
     */
    public static final ConfigNamespace METRICS_GRAPHITE_NS = new ConfigNamespace(METRICS_NS,"graphite","Configuration options for metrics reporting through Graphite");

    /**
     * The hostname to receive Graphite plaintext protocol metric data. Setting
     * this config key has no effect unless {@link #GRAPHITE_INTERVAL} is also
     * set.
     */
    public static final ConfigOption<String> GRAPHITE_HOST = new ConfigOption<>(METRICS_GRAPHITE_NS,"hostname",
            "The hostname to receive Graphite plaintext protocol metric data",
            ConfigOption.Type.MASKABLE, String.class);

    /**
     * The number of milliseconds to wait between sending Metrics data to the
     * host specified {@link #GRAPHITE_HOST}. This has no effect unless
     * {@link #GRAPHITE_HOST} is also set.
     */
    public static final ConfigOption<Duration> GRAPHITE_INTERVAL = new ConfigOption<>(METRICS_GRAPHITE_NS,"interval",
            "The number of milliseconds to wait between sending Metrics data",
            ConfigOption.Type.MASKABLE, Duration.class);

    /**
     * The port to which Graphite data are sent.
     * <p>
     */
    public static final ConfigOption<Integer> GRAPHITE_PORT = new ConfigOption<>(METRICS_GRAPHITE_NS,"port",
            "The port to which Graphite data are sent",
            ConfigOption.Type.MASKABLE, 2003);

    /**
     * A Graphite-specific prefix for reported metrics. If non-null, Metrics
     * prepends this and a "." to all metric names before reporting them to
     * Graphite.
     * <p>
     */
    public static final ConfigOption<String> GRAPHITE_PREFIX = new ConfigOption<>(METRICS_GRAPHITE_NS,"prefix",
            "A Graphite-specific prefix for reported metrics",
            ConfigOption.Type.MASKABLE, String.class);

    // ################### SCRIPT EVALUATION #######################
    // #############################################################

    public static final ConfigNamespace SCRIPT_ENGINE_NS = new ConfigNamespace(GRAPH_NS, "script-eval",
        "Configuration options for gremlin script engine.");

    public static final ConfigOption<Boolean> SCRIPT_EVAL_ENABLED = new ConfigOption<>(SCRIPT_ENGINE_NS, "enabled",
        "Whether to enable Gremlin script evaluation. If it is enabled, a gremlin script engine will be " +
            "instantiated together with the JanusGraph instance, with which one can use `eval` method to evaluate a gremlin " +
            "script in plain string format. This is usually only useful when JanusGraph is used as an embedded Java library.",
        ConfigOption.Type.MASKABLE, false);

    public static final ConfigOption<String> SCRIPT_EVAL_ENGINE = new ConfigOption<>(SCRIPT_ENGINE_NS, "engine",
        "Full class name of script engine that implements `GremlinScriptEngine` interface. Following shorthands " +
            "can be used: <br> " +
            "- `GremlinLangScriptEngine` (A script engine that only accepts standard gremlin queries. Anything else " +
            "including lambda function is not accepted. We recommend using this because it's generally safer, but it is " +
            "not guaranteed that it has no security problem.)" +
            "- `GremlinGroovyScriptEngine` (A script engine that accepts arbitrary groovy code. This can be dangerous " +
            "and you should use it at your own risk. See https://tinkerpop.apache.org/docs/current/reference/#script-execution " +
            "for potential security problems.)",
        ConfigOption.Type.MASKABLE, "GremlinLangScriptEngine", new Predicate<String>() {
        @Override
        public boolean apply(@Nullable String s) {
            if (s == null) return false;
            if (PREREGISTERED_SCRIPT_ENGINE.containsKey(s)) return true;
            try {
                Class<?> clazz = ClassUtils.getClass(s);
                return GremlinScriptEngine.class.isAssignableFrom(clazz);
            } catch (ClassNotFoundException e) {
                return false;
            }
        }
    });

    private static final Map<String, String> PREREGISTERED_SCRIPT_ENGINE = Collections.unmodifiableMap(
        new HashMap<String, String>(2) {{
            put("GremlinLangScriptEngine", GremlinLangScriptEngine.class.getName());
            put("GremlinGroovyScriptEngine", GremlinGroovyScriptEngine.class.getName());
        }}
    );

    public static final ConfigNamespace GREMLIN_NS = new ConfigNamespace(ROOT_NS,"gremlin",
            "Gremlin configuration options");

    public static final ConfigOption<String> GREMLIN_GRAPH = new ConfigOption<>(GREMLIN_NS, "graph",
            "The implementation of graph factory that will be used by gremlin server", ConfigOption.Type.LOCAL, "org.janusgraph.core.JanusGraphFactory");

    // ################ Begin Class Definition #######################
    // ###############################################################

    public static final String SYSTEM_PROPERTIES_STORE_NAME = "system_properties";
    public static final String SYSTEM_CONFIGURATION_IDENTIFIER = "configuration";
    public static final String USER_CONFIGURATION_IDENTIFIER = "userconfig";

    private static final Map<String, String> REGISTERED_INDEX_SELECTION_STRATEGIES = new HashMap() {{
        put(ThresholdBasedIndexSelectionStrategy.NAME, ThresholdBasedIndexSelectionStrategy.class.getName());
        put(BruteForceIndexSelectionStrategy.NAME, BruteForceIndexSelectionStrategy.class.getName());
        put(ApproximateIndexSelectionStrategy.NAME, ApproximateIndexSelectionStrategy.class.getName());
    }};

    private final Configuration configuration;
    private final ReadConfiguration configurationAtOpen;
    private String uniqueGraphId;
    private final ModifiableConfiguration localConfiguration;

    private boolean readOnly;
    private boolean evalScript;
    private GremlinScriptEngine scriptEngine;
    private boolean flushIDs;
    private boolean forceIndexUsage;
    private boolean batchLoading;
    private int txVertexCacheSize;
    private int txDirtyVertexSize;
    private DefaultSchemaMaker defaultSchemaMaker;
    private boolean hasDisabledSchemaConstraints;
    private Boolean propertyPrefetching;
    private boolean adjustQueryLimit;
    private int hardMaxLimit;
    private Boolean useMultiQuery;
    private boolean limitedBatch;
    private int limitedBatchSize;
    private MultiQueryStrategyRepeatStepMode repeatStepMode;
    private boolean optimizerBackendAccess;
    private IndexSelectionStrategy indexSelectionStrategy;
    private boolean allowVertexIdSetting;
    private boolean allowCustomVertexIdType;
    private boolean logTransactions;
    private String metricsPrefix;
    private String unknownIndexKeyName;
    private MultiQueryHasStepStrategyMode hasStepStrategyMode;
    private MultiQueryPropertiesStrategyMode propertiesStrategyMode;
    private MultiQueryLabelStepStrategyMode labelStepStrategyMode;
    private MultiQueryDropStepStrategyMode dropStepStrategyMode;
    private String schemaInitStrategy;
    private boolean dropSchemaBeforeInit;
    private String schemaInitJsonFile;
    private String schemaInitJsonString;
    private boolean schemaInitJsonSkipElements;
    private boolean schemaInitJsonSkipIndices;
    private IndicesActivationType schemaInitJsonIndicesActivationType;
    private boolean schemaInitJsonForceCloseOtherInstances;
    private long schemaInitJsonIndexStatusAwaitTimeout;

    private StoreFeatures storeFeatures = null;

    public GraphDatabaseConfiguration(ReadConfiguration configurationAtOpen, ModifiableConfiguration localConfiguration,
                                      String uniqueGraphId, Configuration configuration) {
        this.configurationAtOpen = configurationAtOpen;
        this.localConfiguration = localConfiguration;
        this.uniqueGraphId = uniqueGraphId;
        this.configuration = configuration;
        preLoadConfiguration();
    }

    public static ModifiableConfiguration buildGraphConfiguration() {
        return new ModifiableConfiguration(ROOT_NS,
            new CommonsConfiguration(ConfigurationUtil.createBaseConfiguration()),
            BasicConfiguration.Restriction.NONE);
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public boolean hasScriptEval() {
        return evalScript;
    }

    public GremlinScriptEngine getScriptEngine() {
        return scriptEngine;
    }

    public boolean hasFlushIDs() {
        return flushIDs;
    }

    public boolean hasForceIndexUsage() {
        return forceIndexUsage;
    }

    public int getTxVertexCacheSize() {
        return txVertexCacheSize;
    }

    public int getTxDirtyVertexSize() {
        return txDirtyVertexSize;
    }

    public boolean isBatchLoading() {
        return batchLoading;
    }

    public String getUniqueGraphId() {
        return uniqueGraphId;
    }

    public String getMetricsPrefix() {
        return metricsPrefix;
    }

    public DefaultSchemaMaker getDefaultSchemaMaker() {
        return defaultSchemaMaker;
    }

    public boolean hasDisabledSchemaConstraints() {
        return hasDisabledSchemaConstraints;
    }

    public boolean allowVertexIdSetting() {
        return allowVertexIdSetting;
    }

    public boolean allowCustomVertexIdType() {
        return allowCustomVertexIdType;
    }

    public Duration getMaxCommitTime() {
        return configuration.get(MAX_COMMIT_TIME);
    }

    public Duration getMaxWriteTime() {
        return configuration.get(STORAGE_WRITE_WAITTIME);
    }

    public boolean hasPropertyPrefetching() {
        if (propertyPrefetching == null) {
            return getStoreFeatures().isDistributed();
        } else {
            return propertyPrefetching;
        }
    }

    public boolean useMultiQuery() {
        return useMultiQuery;
    }

    public boolean limitedBatch() {
        return limitedBatch;
    }

    public int limitedBatchSize() {
        return limitedBatchSize;
    }

    public MultiQueryStrategyRepeatStepMode repeatStepMode() {
        return repeatStepMode;
    }

    public boolean optimizerBackendAccess() {
        return optimizerBackendAccess;
    }

    public IndexSelectionStrategy getIndexSelectionStrategy() {
        return indexSelectionStrategy;
    }

    public MultiQueryHasStepStrategyMode hasStepStrategyMode() {
        return hasStepStrategyMode;
    }

    public MultiQueryPropertiesStrategyMode propertiesStrategyMode() {
        return propertiesStrategyMode;
    }

    public MultiQueryLabelStepStrategyMode labelStepStrategyMode() {
        return labelStepStrategyMode;
    }

    public MultiQueryDropStepStrategyMode dropStepStrategyMode() {
        return dropStepStrategyMode;
    }

    public boolean adjustQueryLimit() {
        return adjustQueryLimit;
    }

    public int getHardMaxLimit() {
        return hardMaxLimit;
    }

    public String getUnknownIndexKeyName() {
        return unknownIndexKeyName;
    }

    public boolean hasLogTransactions() {
        return logTransactions;
    }

    public TimestampProvider getTimestampProvider() {
        return configuration.get(TIMESTAMP_PROVIDER);
    }

    public boolean isUpgradeAllowed(String name) {
        return configuration.get(ALLOW_UPGRADE) && JanusGraphConstants.UPGRADEABLE_FIXED.contains(name);
    }

    public VertexIDAssigner getIDAssigner(Backend backend) {
        return new VertexIDAssigner(configuration, backend.getIDAuthority(), backend.getStoreFeatures());
    }

    public String getBackendDescription() {
        String className = configuration.get(STORAGE_BACKEND);
        if (className.equalsIgnoreCase("berkeleyje")) {
            return className + ":" + configuration.get(STORAGE_DIRECTORY);
        } else {
            return className + ":" + Arrays.toString(configuration.get(STORAGE_HOSTS));
        }
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public Backend getBackend() {
        Backend backend = new Backend(configuration);
        backend.initialize(configuration);
        storeFeatures = backend.getStoreFeatures();
        return backend;
    }

    public String getGraphName() {
        return getConfigurationAtOpen().getString(GRAPH_NAME.toStringWithoutRoot());
    }

    public StoreFeatures getStoreFeatures() {
        Preconditions.checkArgument(storeFeatures != null, "Cannot retrieve store features before the storage backend has been initialized");
        return storeFeatures;
    }

    public Serializer getSerializer() {
        return getSerializer(configuration);
    }

    public static Serializer getSerializer(Configuration configuration) {
        Serializer serializer = new StandardSerializer();

        List<RegisteredAttributeClass<?>> registeredAttributeClasses =
            RegisteredAttributeClassesConverter.getInstance().convert(configuration);

        for (RegisteredAttributeClass<?> clazz : registeredAttributeClasses) {
            clazz.registerWith(serializer);
        }
        return serializer;
    }

    public SchemaCache getTypeCache(SchemaCache.StoreRetrieval retriever) {
        if (configuration.get(BASIC_METRICS)) return new MetricInstrumentedSchemaCache(retriever);
        else return new StandardSchemaCache(retriever);
    }

    public org.apache.commons.configuration2.Configuration getLocalConfiguration() {
        org.apache.commons.configuration2.Configuration config = ((CommonsConfiguration)localConfiguration.getConfiguration()).getCommonConfiguration();
        config.setProperty(Graph.GRAPH, JanusGraphFactory.class.getName());
        return config;
    }

    public org.apache.commons.configuration2.Configuration getConfigurationAtOpen() {
        return ReadConfigurationConverter.getInstance().convertToBaseConfiguration(configurationAtOpen);
    }

    public String getSchemaInitStrategy(){
        return schemaInitStrategy;
    }

    public boolean isDropSchemaBeforeInit(){
        return dropSchemaBeforeInit;
    }

    public String getSchemaInitJsonFile(){
        return schemaInitJsonFile;
    }

    public String getSchemaInitJsonString(){
        return schemaInitJsonString;
    }

    public boolean getSchemaInitJsonSkipElements(){
        return schemaInitJsonSkipElements;
    }

    public boolean getSchemaInitJsonSkipIndices(){
        return schemaInitJsonSkipIndices;
    }

    public IndicesActivationType getSchemaInitJsonIndicesActivationType(){
        return schemaInitJsonIndicesActivationType;
    }

    public boolean getSchemaInitJsonForceCloseOtherInstances(){
        return schemaInitJsonForceCloseOtherInstances;
    }

    public long getSchemaInitJsonIndexStatusAwaitTimeout(){
        return schemaInitJsonIndexStatusAwaitTimeout;
    }

    private void preLoadConfiguration() {
        readOnly = configuration.get(STORAGE_READONLY);
        evalScript = configuration.get(SCRIPT_EVAL_ENABLED);
        if (evalScript) {
            String scriptEngineName = configuration.get(SCRIPT_EVAL_ENGINE);
            if (PREREGISTERED_SCRIPT_ENGINE.containsKey(scriptEngineName)) {
                scriptEngineName = PREREGISTERED_SCRIPT_ENGINE.get(scriptEngineName);
            }
            scriptEngine = ConfigurationUtil.instantiate(scriptEngineName);
        }
        flushIDs = configuration.get(IDS_FLUSH);
        forceIndexUsage = configuration.get(FORCE_INDEX_USAGE);
        batchLoading = configuration.get(STORAGE_BATCH);
        String autoTypeMakerName = configuration.get(AUTO_TYPE);
        if (PREREGISTERED_AUTO_TYPE.containsKey(autoTypeMakerName))
            defaultSchemaMaker = PREREGISTERED_AUTO_TYPE.get(autoTypeMakerName);
        else defaultSchemaMaker = ConfigurationUtil.instantiate(autoTypeMakerName);
        //Disable auto-type making when batch-loading is enabled since that may overwrite types without warning
        if (batchLoading) defaultSchemaMaker = DisableDefaultSchemaMaker.INSTANCE;

        defaultSchemaMaker.enableLogging(configuration.get(SCHEMA_MAKER_LOGGING));

        hasDisabledSchemaConstraints = !configuration.get(SCHEMA_CONSTRAINTS);

        txVertexCacheSize = configuration.get(TX_CACHE_SIZE);
        //Check for explicit dirty vertex cache size first, then fall back on batch-loading-dependent default
        if (configuration.has(TX_DIRTY_SIZE)) {
            txDirtyVertexSize = configuration.get(TX_DIRTY_SIZE);
        } else {
            txDirtyVertexSize = batchLoading ?
                TX_DIRTY_SIZE_DEFAULT_WITH_BATCH :
                TX_DIRTY_SIZE_DEFAULT_WITHOUT_BATCH;
        }

        propertyPrefetching = configuration.get(PROPERTY_PREFETCHING);
        useMultiQuery = configuration.get(USE_MULTIQUERY);
        limitedBatch = configuration.get(LIMITED_BATCH);
        limitedBatchSize = configuration.get(LIMITED_BATCH_SIZE);
        repeatStepMode = selectExactConfig(REPEAT_STEP_BATCH_MODE, MultiQueryStrategyRepeatStepMode.values());
        hasStepStrategyMode = selectExactConfig(HAS_STEP_BATCH_MODE, MultiQueryHasStepStrategyMode.values());
        propertiesStrategyMode = selectExactConfig(PROPERTIES_BATCH_MODE, MultiQueryPropertiesStrategyMode.values());
        labelStepStrategyMode = selectExactConfig(LABEL_STEP_BATCH_MODE, MultiQueryLabelStepStrategyMode.values());
        dropStepStrategyMode = selectExactConfig(DROP_STEP_BATCH_MODE, MultiQueryDropStepStrategyMode.values());

        indexSelectionStrategy = Backend.getImplementationClass(configuration, configuration.get(INDEX_SELECT_STRATEGY),
            REGISTERED_INDEX_SELECTION_STRATEGIES);
        optimizerBackendAccess = configuration.get(OPTIMIZER_BACKEND_ACCESS);

        adjustQueryLimit = configuration.get(ADJUST_LIMIT);
        hardMaxLimit = configuration.get(HARD_MAX_LIMIT);

        allowVertexIdSetting = configuration.get(ALLOW_SETTING_VERTEX_ID);
        allowCustomVertexIdType = configuration.get(ALLOW_CUSTOM_VERTEX_ID_TYPES);
        if (allowCustomVertexIdType && !allowVertexIdSetting) {
            throw new JanusGraphConfigurationException(String.format("%s is enabled but %s is disabled",
                ALLOW_CUSTOM_VERTEX_ID_TYPES.getName(), ALLOW_SETTING_VERTEX_ID.getName()));
        }

        logTransactions = configuration.get(SYSTEM_LOG_TRANSACTIONS);

        unknownIndexKeyName = configuration.get(IGNORE_UNKNOWN_INDEX_FIELD) ? UNKNOWN_FIELD_NAME : null;

        schemaInitStrategy = configuration.get(SCHEMA_INIT_STRATEGY);
        dropSchemaBeforeInit = configuration.get(SCHEMA_DROP_BEFORE_INIT);
        schemaInitJsonFile = configuration.has(SCHEMA_INIT_JSON_FILE) ? configuration.get(SCHEMA_INIT_JSON_FILE) : null;
        schemaInitJsonString = configuration.has(SCHEMA_INIT_JSON_STR) ? configuration.get(SCHEMA_INIT_JSON_STR) : null;
        schemaInitJsonSkipElements = configuration.get(SCHEMA_INIT_JSON_SKIP_ELEMENTS);
        schemaInitJsonSkipIndices = configuration.get(SCHEMA_INIT_JSON_SKIP_INDICES);
        schemaInitJsonIndicesActivationType = selectExactConfig(SCHEMA_INIT_JSON_INDICES_ACTIVATION_TYPE, IndicesActivationType.values());
        schemaInitJsonForceCloseOtherInstances = configuration.get(SCHEMA_INIT_JSON_FORCE_CLOSE_OTHER_INSTANCES);
        schemaInitJsonIndexStatusAwaitTimeout = configuration.get(SCHEMA_INIT_JSON_AWAIT_INDEX_STATUS_TIMEOUT);

        configureMetrics();
    }

    private <T extends ConfigName> T selectExactConfig(ConfigOption<String> configOption, T ... configs){
        final String configName = configuration.get(configOption);
        T resultConfig = Arrays.stream(configs).filter(config -> config.getConfigName().equals(configName)).findAny().orElse(null);
        return Preconditions.checkNotNull(resultConfig, configName+" is not recognized as one of the supported values of `"+configOption.toStringWithoutRoot()+"`");
    }

    private void configureMetrics() {
        Preconditions.checkNotNull(configuration);

        metricsPrefix = configuration.get(METRICS_PREFIX);

        if (!configuration.get(BASIC_METRICS)) {
            metricsPrefix = null;
        } else {
            Preconditions.checkNotNull(metricsPrefix);
        }

        configureMetricsConsoleReporter();
        configureMetricsCsvReporter();
        configureMetricsJmxReporter();
        configureMetricsSlf4jReporter();
        configureMetricsGraphiteReporter();
    }

    private void configureMetricsConsoleReporter() {
        if (configuration.has(METRICS_CONSOLE_INTERVAL)) {
            MetricManager.INSTANCE.addConsoleReporter(configuration.get(METRICS_CONSOLE_INTERVAL));
        }
    }

    private void configureMetricsCsvReporter() {
        if (configuration.has(METRICS_CSV_DIR)) {
            MetricManager.INSTANCE.addCsvReporter(configuration.get(METRICS_CSV_INTERVAL), configuration.get(METRICS_CSV_DIR));
        }
    }

    private void configureMetricsJmxReporter() {
        if (configuration.get(METRICS_JMX_ENABLED)) {
            MetricManager.INSTANCE.addJmxReporter(configuration.get(METRICS_JMX_DOMAIN), configuration.get(METRICS_JMX_AGENTID));
        }
    }

    private void configureMetricsSlf4jReporter() {
        if (configuration.has(METRICS_SLF4J_INTERVAL)) {
            // null loggerName is allowed -- that means Metrics will use its internal default
            MetricManager.INSTANCE.addSlf4jReporter(configuration.get(METRICS_SLF4J_INTERVAL),
                configuration.has(METRICS_SLF4J_LOGGER) ? configuration.get(METRICS_SLF4J_LOGGER) : null);
        }
    }

    private void configureMetricsGraphiteReporter() {
        if (configuration.has(GRAPHITE_HOST)) {
            MetricManager.INSTANCE.addGraphiteReporter(configuration.get(GRAPHITE_HOST),
                configuration.get(GRAPHITE_PORT),
                configuration.get(GRAPHITE_PREFIX),
                configuration.get(GRAPHITE_INTERVAL));
        }
    }

}
