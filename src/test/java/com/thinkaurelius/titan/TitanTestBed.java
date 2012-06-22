package com.thinkaurelius.titan;

import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.core.*;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.HierarchicalConfiguration;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class TitanTestBed {
    
    
    public static void main(String args[]) {

        int i = 5;
        Integer ii = i;
        Long ll = Long.valueOf(ii);
        
        Configuration conf = new BaseConfiguration();
        conf.setProperty("storage.backend","hbase");
        //conf.setProperty("storage.hostname","127.0.0.1");
//        conf.setProperty("storage.directory","/var/folders/1_/clrrcnxn27v6zvw25x0m89500000gn/T/titandemo");
        TitanGraph g = TitanFactory.open(conf);

        g.addEdge(null,g.addVertex(null),g.addVertex(null),"test");

        g.shutdown();
        
    }
    
    
    
}
