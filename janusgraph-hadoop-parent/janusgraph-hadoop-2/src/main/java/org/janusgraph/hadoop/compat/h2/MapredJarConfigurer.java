// Copyright 2017 JanusGraph Authors
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

package org.janusgraph.hadoop.compat.h2;

import java.io.IOException;

import org.janusgraph.hadoop.config.job.JobClasspathConfigurer;
import org.apache.hadoop.mapreduce.Job;

public class MapredJarConfigurer implements JobClasspathConfigurer {

    private final String mapredJar;

    public MapredJarConfigurer(String mapredJar) {
        this.mapredJar = mapredJar;
    }

    @Override
    public void configure(Job job) throws IOException {
        job.setJar(mapredJar);
    }
}
