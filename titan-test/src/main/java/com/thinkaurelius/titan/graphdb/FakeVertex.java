package com.thinkaurelius.titan.graphdb;

import com.tinkerpop.frames.Property;

public interface FakeVertex {
    @Property("uid")
    public Long getUid();
    
    @Property("vp_0")
    public Integer getProp0();
    
    @Property("vp_1")
    public Integer getProp1();

    @Property("vp_2")
    public Integer getProp2();
}