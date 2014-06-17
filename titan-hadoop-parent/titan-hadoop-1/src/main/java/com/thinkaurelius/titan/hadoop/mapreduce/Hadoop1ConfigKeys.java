package com.thinkaurelius.titan.hadoop.mapreduce;

public class Hadoop1ConfigKeys implements HadoopConfigKeys {

    private static final String MAPREDUCE_SPECULATIVE_MAPS = "mapred.map.tasks.speculative.execution";
    private static final String MAPREDUCE_SPECULATIVE_REDUCES = "mapred.reduce.tasks.speculative.execution";

    @Override
    public String getSpeculativeMapConfigKey() {
        return MAPREDUCE_SPECULATIVE_MAPS;
    }

    @Override
    public String getSpeculativeReduceConfigKey() {
        return MAPREDUCE_SPECULATIVE_REDUCES;
    }
}
