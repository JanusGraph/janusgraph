// Copyright 2021 JanusGraph Authors
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

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class CustomTestExecutorService extends ThreadPoolExecutor {

    public static volatile ExecutorServiceConfiguration executorServiceConfiguration;

    public CustomTestExecutorService(ExecutorServiceConfiguration executorServiceConfiguration) {
        super(executorServiceConfiguration.getCorePoolSize(), executorServiceConfiguration.getMaxPoolSize(),
            executorServiceConfiguration.getKeepAliveTime(), TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        CustomTestExecutorService.executorServiceConfiguration = executorServiceConfiguration;
    }

}
