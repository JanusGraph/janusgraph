package com.thinkaurelius.titan.hadoop.config;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.thinkaurelius.titan.diskstorage.configuration.*;
import com.thinkaurelius.titan.graphdb.database.serialize.kryo.KryoSerializer;
import com.thinkaurelius.titan.hadoop.Tokens;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Element;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Preconditions;

import javax.annotation.Nullable;

public class TitanHadoopConfiguration {

    public static final ConfigNamespace ROOT_NS =
            new ConfigNamespace(null, "faunus", "Faunus configuration root");

    public static final ConfigNamespace TRUNK_NS =
            new ConfigNamespace(new ConfigNamespace(ROOT_NS, "titan", "titan-hadoop namespace"), "hadoop", "Titan-Hadoop configuration parent");

    public static final ConfigNamespace INPUT_NS =
            new ConfigNamespace(TRUNK_NS, "input", "Graph input format configuration");

    public static final ConfigOption<String> INPUT_FORMAT = new ConfigOption<String>(
            INPUT_NS, "format",
            "Package and classname of the input format class.  This must implement the Hadoop InputFormat interface.",
            ConfigOption.Type.LOCAL, "current");

    public static final ConfigOption<String> INPUT_LOCATION = new ConfigOption<String>(
            INPUT_NS, "location",
            "Path to the default input file",
            ConfigOption.Type.LOCAL, String.class);

    public static final ConfigNamespace INPUT_CONF_NS =
            new ConfigNamespace(INPUT_NS, "conf", "Settings for the input format class");

    public static final ConfigOption<String> INPUT_VERTEX_QUERY_FILTER = new ConfigOption<String> (
            INPUT_NS, "vertex-query-filter",
            "A Gremlin vertex-centric query which limits the relations read by Faunus for each vertex.  " +
            "This query string should assume the variable v represents a vertex (Faunus binds the v " +
            "variable automatically before evaluating this query string).",
            ConfigOption.Type.LOCAL, "v.query()");

    public static final ConfigOption<String> TITAN_INPUT_VERSION = new ConfigOption<String>(
            INPUT_NS, "db-version",
            "The version of the Titan database being read",
            ConfigOption.Type.LOCAL, "current");


    public static final ConfigOption<Direction> INPUT_EDGE_COPY_DIRECTION = new ConfigOption<Direction>(
            INPUT_NS, "edge-copy-direction",
            "The edge direction to read and mirror in the opposing direction. " +
            "OUT creates IN edges.  IN creates out EDGES.  BOTH is not supported. ",
            ConfigOption.Type.LOCAL, Direction.class, Direction.OUT);

    @Deprecated // use INPUT_EDGE_COPY_DIRECTION instead
    public static final ConfigOption<Direction> INPUT_EDGE_COPY_DIR = new ConfigOption<Direction>(
            INPUT_NS, "edge-copy-dir",
            "The edge direction to read and mirror in the opposing direction. " +
            "OUT creates IN edges.  IN creates out EDGES.  BOTH is not supported.",
            ConfigOption.Type.LOCAL, Direction.class, Direction.OUT,
            ConfigOption.disallowEmpty(Direction.class), INPUT_EDGE_COPY_DIRECTION);

    public static final ConfigNamespace OUTPUT_NS =
            new ConfigNamespace(TRUNK_NS, "output", "Graph output format configuration");

    public static final ConfigOption<String> OUTPUT_FORMAT = new ConfigOption<String>(
            OUTPUT_NS, "format",
            "Package and classname of the output format class.  This must implment the Hadoop OutputFormat interface.",
            ConfigOption.Type.LOCAL, String.class);

    public static final ConfigOption<Boolean> OUTPUT_INFER_SCHEMA = new ConfigOption<Boolean>(
            OUTPUT_NS, "infer-schema",
            "Whether to attempt to automatically create Titan property keys and labels before writing data",
            ConfigOption.Type.LOCAL, true);

    public static final ConfigNamespace JOBDIR_NS =
            new ConfigNamespace(TRUNK_NS, "jobdir", "Temporary SequenceFile configuration");

    public static final ConfigOption<String> JOBDIR_LOCATION = new ConfigOption<String>(
            JOBDIR_NS, "location",
            "An HDFS path used to store temporary SequenceFiles in between executions of MR jobs chained together by Titan-Hadoop",
            ConfigOption.Type.LOCAL, "jobs");

    public static final ConfigOption<Boolean> JOBDIR_OVERWRITE = new ConfigOption<Boolean>(
            JOBDIR_NS, "overwrite",
            "Whether to temporary SequenceFiles",
            ConfigOption.Type.LOCAL, true);

    public static final ConfigNamespace OUTPUT_CONF_NS =
            new ConfigNamespace(OUTPUT_NS, "conf", "Settings for the output format class");

    public static final ConfigNamespace PIPELINE_NS =
            new ConfigNamespace(TRUNK_NS, "pipeline", "MapReduce job cascading configuration");

    public static final ConfigOption<Boolean> PIPELINE_TRACK_STATE = new ConfigOption<Boolean>(
            PIPELINE_NS, "track-state",
            "Whether to keep intermediate state information",
            ConfigOption.Type.LOCAL, false);

    public static final ConfigOption<Boolean> PIPELINE_TRACK_PATHS = new ConfigOption<Boolean>(
            PIPELINE_NS, "track-paths",
            "Whether to keep intermediate traversal path information",
            ConfigOption.Type.LOCAL, false);

    public static final ConfigOption<Integer> PIPELINE_MAP_SPILL_OVER = new ConfigOption<Integer>(
            PIPELINE_NS, "map-spill-over",
            "The maximum number of map entries held in memory during computation; maps that grow past this entry limit will be partially written to disk",
            ConfigOption.Type.LOCAL, 500);

    public static final ConfigNamespace SIDE_EFFECT_NS =
            new ConfigNamespace(TRUNK_NS, "sideeffect", "Side-effect output format configuration");

    public static final ConfigOption<String> SIDE_EFFECT_FORMAT = new ConfigOption<String>(
            SIDE_EFFECT_NS, "format",
            "Package and classname of the output format to use for computation side effects.  This must implement the Hadoop OutputFormat interface.",
            ConfigOption.Type.LOCAL, String.class);

    public static final ConfigNamespace SERIALIZER_NS =
            new ConfigNamespace(TRUNK_NS, "serializer", "Serializer configuration");

    public static final ConfigOption<Integer> KRYO_MAX_OUTPUT_SIZE = new ConfigOption<Integer>(
            SERIALIZER_NS, "max-output-buffer-size",
            "The maximum size, in bytes, of any single object serialized by Kryo.  " +
            "Attempts to serialize objects with larger serialized representations will generate an " +
            "exception.  This should be set large enough to accommodate any single sane datum written " +
            "by Kryo, and serves as a last-resort sanity check to avoid erroneously serializing reference cycles.",
            ConfigOption.Type.LOCAL, Integer.class, KryoSerializer.DEFAULT_MAX_OUTPUT_SIZE);

    public static final ConfigNamespace INDEX_NS =
            new ConfigNamespace(TRUNK_NS, "reindex", "Index repair configuration");

    public static final ConfigOption<String> INDEX_NAME = new ConfigOption<String>(
            INDEX_NS, "name",
            "The name of a Titan index to build or repair.  The index must already be enabled or installed.",
            ConfigOption.Type.LOCAL, String.class);

    public static final ConfigOption<String> INDEX_TYPE = new ConfigOption<String>(
            INDEX_NS, "type",
            "The relationtype of a Titan index to build or repair.  The index must already be enabled or installed.",
            ConfigOption.Type.LOCAL, String.class, "", new Predicate<String>() {
                @Override
                public boolean apply(@Nullable String input) {
                    return null != input; // empty string acceptable
                }
            });

    // Hidden settings used by MapReduce jobs defined in the transform, sideeffect, and filter packages

    public static final ConfigNamespace TRANSFORM_NS =
            new ConfigNamespace(TRUNK_NS, "transform", "Automatically-set Titan-Hadoop internal options");

    // VerticesVerticesMapReduce

    public static final ConfigNamespace VERTICES_VERTICES_NS =
            new ConfigNamespace(TRANSFORM_NS, "vertices-vertices", "Automatically-set options used by VerticesVerticesMapReduce");

    public static final ConfigOption<Direction> VERTICES_VERTICES_DIRECTION = new ConfigOption<Direction>(
            VERTICES_VERTICES_NS, "direction",
            "TODO" /* TODO */, ConfigOption.Type.LOCAL, Direction.class).hide();

    public static final ConfigOption<String[]> VERTICES_VERTICES_LABELS = new ConfigOption<String[]>(
            VERTICES_VERTICES_NS, "labels",
            "TODO" /* TODO */, ConfigOption.Type.LOCAL, String[].class, new String[]{},
            Predicates.<String[]>alwaysTrue()).hide();

    // VerticesEdgesMapReduce

    public static final ConfigNamespace VERTICES_EDGES_NS =
            new ConfigNamespace(TRANSFORM_NS, "vertices-edges", "Automatically-set options used by VerticesVerticesMapReduce");

    public static final ConfigOption<Direction> VERTICES_EDGES_DIRECTION = new ConfigOption<Direction>(
            VERTICES_EDGES_NS, "direction",
            "TODO" /* TODO */, ConfigOption.Type.LOCAL, Direction.class).hide();

    public static final ConfigOption<String[]> VERTICES_EDGES_LABELS = new ConfigOption<String[]>(
            VERTICES_EDGES_NS, "labels",
            "TODO" /* TODO */, ConfigOption.Type.LOCAL, String[].class, new String[]{},
            Predicates.<String[]>alwaysTrue()).hide();
    // LinkMapReduce

    public static final ConfigNamespace LINK_NS =
            new ConfigNamespace(SIDE_EFFECT_NS, "link", "Automatically-set options used by LinkMapReduce");

    //public static final String DIRECTION = Tokens.makeNamespace(LinkMapReduce.class) + ".direction";
    public static final ConfigOption<Direction> LINK_DIRECTION = new ConfigOption<Direction>(
            LINK_NS, "direction",
            "TODO" /* TODO */, ConfigOption.Type.LOCAL, Direction.class).hide();

    //public static final String LABEL = Tokens.makeNamespace(LinkMapReduce.class) + ".label";
    public static final ConfigOption<String> LINK_LABEL = new ConfigOption<String>(
            LINK_NS, "label",
            "TODO" /* TODO */, ConfigOption.Type.LOCAL, String.class).hide();

    //public static final String STEP = Tokens.makeNamespace(LinkMapReduce.class) + ".step";
    public static final ConfigOption<Integer> LINK_STEP = new ConfigOption<Integer>(
            LINK_NS, "step",
            "TODO" /* TODO */, ConfigOption.Type.LOCAL, Integer.class, -1).hide();

    //public static final String MERGE_DUPLICATES = Tokens.makeNamespace(LinkMapReduce.class) + ".mergeDuplicates";
    public static final ConfigOption<Boolean> LINK_MERGE_DUPLICATES = new ConfigOption<Boolean>(
            LINK_NS, "merge-duplicates",
            "TODO" /* TODO */, ConfigOption.Type.LOCAL, false).hide();

    //public static final String MERGE_WEIGHT_KEY = Tokens.makeNamespace(LinkMapReduce.class) + ".mergeWeightKey";
    public static final ConfigOption<String> LINK_MERGE_WEIGHT_KEY = new ConfigOption<String>(
            LINK_NS, "merge-weight-key",
            "TODO" /* TODO */, ConfigOption.Type.LOCAL, "_").hide();

    // CommitVerticesMapReduce

    public static final ConfigNamespace COMMIT_VERTICES_NS =
            new ConfigNamespace(SIDE_EFFECT_NS, "commit-vertices", "Automatically-set options used by CommitVerticesMapReduce");

    //public static final String ACTION = Tokens.makeNamespace(CommitVerticesMapReduce.class) + ".action";
    public static final ConfigOption<Tokens.Action> COMMIT_VERTICES_ACTION = new ConfigOption<Tokens.Action>(
            COMMIT_VERTICES_NS, "action",
            "TODO" /* TODO */, ConfigOption.Type.LOCAL, Tokens.Action.class).hide();

    // BackFilterMapReduce

    public static final ConfigNamespace FILTER_NS =
            new ConfigNamespace(TRUNK_NS, "filter", "Automatically-set options used by the filter package");

    public static final ConfigNamespace BACK_FILTER_NS =
            new ConfigNamespace(FILTER_NS, "filter", "Automatically-set options used by BackFilterMapReduce");

    //public static final String CLASS = Tokens.makeNamespace(BackFilterMapReduce.class) + ".class";
    public static final ConfigOption<String> BACK_FILTER_CLASS = new ConfigOption<String>(
            BACK_FILTER_NS, "class",
            "TODO" /* TODO */, ConfigOption.Type.LOCAL, String.class, Element.class.getCanonicalName()).hide();

    //public static final String STEP = Tokens.makeNamespace(BackFilterMapReduce.class) + ".step";
    public static final ConfigOption<Integer> BACK_FILTER_STEP = new ConfigOption<Integer>(
            BACK_FILTER_NS, "step",
            "TODO" /* TODO */, ConfigOption.Type.LOCAL, Integer.class, -1).hide();


//    public static final ConfigNamespace JARCACHE_NS =
//            new ConfigNamespace(TRUNK_NS, "jarcache", "Jar staging and DistributedCache classpath settings");

}
