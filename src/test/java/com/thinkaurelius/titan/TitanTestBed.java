package com.thinkaurelius.titan;

import com.thinkaurelius.titan.core.*;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class TitanTestBed {
    
    
    public static void main(String args[]) {

        Configuration conf = new BaseConfiguration();
        conf.setProperty("storage.backend","hbase");
        //conf.setProperty("storage.hostname","127.0.0.1");
//        conf.setProperty("storage.directory","/var/folders/1_/clrrcnxn27v6zvw25x0m89500000gn/T/titandemo");
        TitanGraph g = TitanFactory.open(conf);

        TitanKey time = g.makeType().name("time").dataType(Integer.class).functional().makePropertyKey();
        TitanLabel battled = g.makeType().name("battled").signature(time).makeEdgeLabel();
        
        g.addEdge(null,g.addVertex(null),g.addVertex(null),"test");
        TitanType name = g.getType("name");
        TitanVertex v = (TitanVertex)g.addVertex(null);
        TitanVertex v2 = (TitanVertex)g.getVertices("name","jupiter").iterator().next();
        TypeGroup family = TypeGroup.of(2,"family");
        TitanLabel father = g.makeType().name("father").group(family).makeEdgeLabel();
        v.query().group(family).vertices();
        g.shutdown();
        
    }
    
    
    
}
