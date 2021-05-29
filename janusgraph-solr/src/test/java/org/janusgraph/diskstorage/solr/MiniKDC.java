// Copyright 2018 JanusGraph Authors
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

package org.janusgraph.diskstorage.solr;

import com.google.common.base.Joiner;
import org.apache.kerby.kerberos.kerb.KrbException;
import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;
import org.apache.kerby.util.NetworkUtil;

import java.io.File;
import java.util.Properties;

public class MiniKDC {

    private static final boolean DEBUG_ENABLED = false;

    private static final String REALM = "TEST.COM";
    private static final String LOCALHOST = "127.0.0.1";

    private static final String NON_INSTANCE_PRINCIPAL_POSTFIX = "@" + REALM;
    private static final String INSTANCE_PRINCIPAL_POSTFIX = "/" + LOCALHOST + NON_INSTANCE_PRINCIPAL_POSTFIX;

    private static final String CLIENT_KEYTAB_FILE_NAME = "bob.user.keytab";
    private static final String CLIENT_PNAME = "bob";
    private static final String CLIENT_PRINCIPAL = CLIENT_PNAME + NON_INSTANCE_PRINCIPAL_POSTFIX;

    private static final String SOLR_KEYTAB_FILE_NAME = "solr.keytab";
    private static final String SOLR_PNAME = "solr";
    private static final String SOLR_PRINCIPAL = SOLR_PNAME + INSTANCE_PRINCIPAL_POSTFIX;

    private static final String SPNEGO_KEYTAB_FILE_NAME = "spnego.service.keytab";
    private static final String SPNEGO_PNAME = "HTTP";
    private static final String SPNEGO_PRINCIPAL = SPNEGO_PNAME + INSTANCE_PRINCIPAL_POSTFIX;

    private static final String ZK_PNAME = "zookeeper";
    private static final String ZK_KEYTAB_FILE_NAME = "zk.service.keytab";
    private static final String ZK_PRINCIPAL = ZK_PNAME + INSTANCE_PRINCIPAL_POSTFIX;

    final String authConfigName;

    private Properties systemProperties;

    final File testDir;
    final File zkServiceKeytabFile;
    final File solrServiceKeytabFile;
    final File clientKeytabFile;
    final File spnegoKeytabFile;

    private SimpleKdcServer kdcServer;

    public MiniKDC(String authConfigName)  throws Exception {
        this.authConfigName = authConfigName;

        testDir = createTestDir();

        clientKeytabFile = new File(testDir, CLIENT_KEYTAB_FILE_NAME);
        solrServiceKeytabFile = new File(testDir, SOLR_KEYTAB_FILE_NAME);
        spnegoKeytabFile = new File(testDir, SPNEGO_KEYTAB_FILE_NAME);
        zkServiceKeytabFile = new File(testDir, ZK_KEYTAB_FILE_NAME);

        initSystemProperties();
        setSystemProperties();
    }

    public void start(String listenAddress) throws Exception {
        setUpKdcServer(listenAddress);
        setUpPrincipals();
    }

    public void createClientKeyTab() throws KrbException {
        if(!clientKeytabFile.exists()) {
            kdcServer.exportPrincipal(CLIENT_PRINCIPAL, clientKeytabFile);
        }
    }

    public void deleteClientKeyTab() {
        clientKeytabFile.delete();
    }

    public void createClientPrincipal() throws KrbException {
        if(kdcServer.getKadmin().getPrincipal(CLIENT_PRINCIPAL) == null) {
            kdcServer.createPrincipal(CLIENT_PRINCIPAL);
        }
    }

    public void deleteClientPrincipal() throws KrbException {
        kdcServer.deletePrincipal(CLIENT_PRINCIPAL);
    }

    public void close() throws Exception {
        deletePrincipals();
        kdcServer.stop();
        deleteKeytabs();
        testDir.delete();
        unsetSystemProperties();
    }

    private void setUpKdcServer(String listenAddress) throws Exception {
        kdcServer = new SimpleKdcServer();
        kdcServer.setKdcRealm(REALM);
        kdcServer.setKdcHost(listenAddress);
        kdcServer.setWorkDir(testDir);
        kdcServer.setKdcTcpPort(NetworkUtil.getServerPort());

        if(DEBUG_ENABLED) {
            kdcServer.enableDebug();
        }

        kdcServer.init();
        kdcServer.start();
    }

    private File createTestDir() {
        String path = Joiner.on(File.separatorChar).join("target", "kerberos_test_data");
        File dir = new File(path);
        dir.mkdirs();
        return dir;
    }

    private void setUpPrincipals() throws KrbException {
        kdcServer.createPrincipal(CLIENT_PRINCIPAL);
        kdcServer.exportPrincipal(CLIENT_PRINCIPAL, clientKeytabFile);

        kdcServer.createPrincipal(SOLR_PRINCIPAL);
        kdcServer.exportPrincipal(SOLR_PRINCIPAL, solrServiceKeytabFile);

        kdcServer.createPrincipal(SPNEGO_PRINCIPAL);
        kdcServer.exportPrincipal(SPNEGO_PRINCIPAL, spnegoKeytabFile);

        kdcServer.createPrincipals(ZK_PRINCIPAL);
        kdcServer.exportPrincipal(ZK_PRINCIPAL, zkServiceKeytabFile);
    }

    private void deletePrincipals() throws KrbException {
        kdcServer.getKadmin().deleteBuiltinPrincipals();
        kdcServer.deletePrincipal(CLIENT_PRINCIPAL);
        kdcServer.deletePrincipal(SOLR_PRINCIPAL);
        kdcServer.deletePrincipal(SPNEGO_PRINCIPAL);
        kdcServer.deletePrincipals(ZK_PRINCIPAL);
    }

    private void deleteKeytabs() {
        clientKeytabFile.delete();
        solrServiceKeytabFile.delete();
        spnegoKeytabFile.delete();
        zkServiceKeytabFile.delete();
    }

    private void initSystemProperties() {
        systemProperties = new Properties();
        systemProperties.setProperty("java.security.auth.login.config", authConfigName);
        systemProperties.setProperty("solr.kerberos.principal", SPNEGO_PRINCIPAL);
        systemProperties.setProperty("solr.kerberos.keytab", spnegoKeytabFile.getAbsolutePath());
        systemProperties.setProperty("solr.authentication.httpclient.configurer", "org.apache.solr.client.solrj.impl.Krb5HttpClientConfigurer");
        systemProperties.setProperty("solr.kerberos.cookie.domain", LOCALHOST);

        if(DEBUG_ENABLED) {
            systemProperties.setProperty("sun.security.krb5.debug", "true");
            systemProperties.setProperty("solr.jaas.debug", "true");
        }
    }

    private void setSystemProperties() {
        for(String key : systemProperties.stringPropertyNames()) {
            System.setProperty(key, systemProperties.getProperty(key));
        }
    }

    private void unsetSystemProperties() {
        for(String key : systemProperties.stringPropertyNames()) {
            System.clearProperty(key);
        }
    }
}
