package com.thinkaurelius.titan.hadoop.mapreduce;

public class Hadoop2ConfigKeys implements HadoopConfigKeys {

    private static final String MAPREDUCE_SPECULATIVE_MAPS = "mapreduce.map.speculative";
    private static final String MAPREDUCE_SPECULATIVE_REDUCES = "mapreduce.reduce.speculative";

    @Override
    public String getSpeculativeMapConfigKey() {
        return MAPREDUCE_SPECULATIVE_MAPS;
    }

    @Override
    public String getSpeculativeReduceConfigKey() {
        return MAPREDUCE_SPECULATIVE_REDUCES;
    }
}
