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

package org.janusgraph.hadoop.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;

/**
 * This class is a hack to support HBase Digest-MD5 token authentication
 * for MapReduce jobs.  HBase's Digest-MD5 token authentication is used
 * when a MR job needs to talk to a HBase cluster secured by Kerberos.
 *
 * This class uses reflection to avoid linking against HBase classes that
 * might not be on the classpath at runtime (e.g. when running JanusGraph-HBase
 * on Cassandra).  Its methods log errors but otherwise serve as noops if
 * HBase is not available or if reflection fails for some other reason.
 */
public class HBaseAuthHelper {

    private static final Logger log =
            LoggerFactory.getLogger(HBaseAuthHelper.class);


    public static Configuration wrapConfiguration(Configuration inner) {
        final String className = "org.apache.hadoop.hbase.HBaseConfiguration";
        final String methodName = "create";
        final Class<?> methodArgType = Configuration.class;

        try {
            Class<?> clazz = HBaseAuthHelper.class.getClassLoader().loadClass(className);
            Method m = clazz.getMethod(methodName, methodArgType);
            return (Configuration)m.invoke(null, inner);
        } catch (Throwable t) {
            log.error("Failed to call {}.{}({})", className, methodName, methodArgType.getSimpleName(), t);
        }

        return inner;
    }

    public static void setHBaseAuthToken(Configuration configuration, Job job) throws IOException {
        // Get HBase authentication token (when configured)

        String hbaseAuthentication = configuration.get("hbase.security.authentication");


        if (null != hbaseAuthentication && hbaseAuthentication.equals("kerberos")) {
            String quorumCfgKey = "hbase.zookeeper.quorum";

            log.info("Obtaining HBase Auth Token from ZooKeeper quorum " + configuration.get(quorumCfgKey));

            final String className = "org.apache.hadoop.hbase.security.User";
            try {
                Class<?> clazz = HBaseAuthHelper.class.getClassLoader().loadClass(className);
                Method getCurrent = clazz.getMethod("getCurrent");
                Object user = getCurrent.invoke(null);
                Method obtainAuthTokenForJob = clazz.getMethod("obtainAuthTokenForJob", Configuration.class, Job.class);
                obtainAuthTokenForJob.invoke(user, configuration, job);
                log.info("Obtained HBase Auth Token from ZooKeeper quorum {} for job {}", configuration.get(quorumCfgKey), job.getJobName());
            } catch (Throwable t) {
                log.error("Failed to generate or store HBase auth token", t);
            }

        } else {
            log.info("Not obtaining HBase Auth Token for MapReduce job " + job.getJobName());
        }
    }
}
