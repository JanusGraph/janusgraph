// Copyright 2021 JanusGraph Authors
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

package org.janusgraph.diskstorage.configuration;

import java.util.concurrent.ThreadFactory;

public class ExecutorServiceConfiguration {

    private String configurationClass;

    private Integer corePoolSize;

    private Integer maxPoolSize;

    private Long keepAliveTime;

    private ThreadFactory threadFactory;

    public ExecutorServiceConfiguration() {
    }

    public ExecutorServiceConfiguration(String configurationClass, Integer corePoolSize, Integer maxPoolSize,
                                        Long keepAliveTime, ThreadFactory threadFactory) {
        this.configurationClass = configurationClass;
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveTime = keepAliveTime;
        this.threadFactory = threadFactory;
    }

    public String getConfigurationClass() {
        return configurationClass;
    }

    public void setConfigurationClass(String configurationClass) {
        this.configurationClass = configurationClass;
    }

    public Integer getCorePoolSize() {
        return corePoolSize;
    }

    public void setCorePoolSize(Integer corePoolSize) {
        this.corePoolSize = corePoolSize;
    }

    public Integer getMaxPoolSize() {
        return maxPoolSize;
    }

    public void setMaxPoolSize(Integer maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    public Long getKeepAliveTime() {
        return keepAliveTime;
    }

    public void setKeepAliveTime(Long keepAliveTime) {
        this.keepAliveTime = keepAliveTime;
    }

    public ThreadFactory getThreadFactory() {
        return threadFactory;
    }

    public void setThreadFactory(ThreadFactory threadFactory) {
        this.threadFactory = threadFactory;
    }
}
