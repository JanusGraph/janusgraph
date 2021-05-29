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

package org.janusgraph.diskstorage.es.rest.util;

import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SSLConfigurationCallbackTest {

    @Mock
    private SSLContextBuilder sslContextBuilderMock;

    @Mock
    private SSLContext sslContextMock;

    @Mock
    private HttpAsyncClientBuilder httpAsyncClientBuilderMock;

    @BeforeEach
    public void setUp() throws Exception {
        when(httpAsyncClientBuilderMock.setSSLContext(any(SSLContext.class))).thenReturn(httpAsyncClientBuilderMock);

        doReturn(sslContextMock).when(sslContextBuilderMock).build();
    }

    @Test
    public void testSSLContextInitTrustStoreOnly() throws Exception {

        final String trustStoreFile = "/a/b/c/truststore.jks";
        final String trustStorePassword = "trustStorePassword";

        final SSLConfigurationCallback cb = SSLConfigurationCallback.Builder.
                createCustom(sslContextBuilderMock).
                withTrustStore(trustStoreFile, trustStorePassword).
                build();

        cb.customizeHttpClient(httpAsyncClientBuilderMock);

        verify(sslContextBuilderMock).loadTrustMaterial(eq(new File(trustStoreFile)),
                eq(trustStorePassword.toCharArray()), same(null));
        verify(httpAsyncClientBuilderMock).setSSLContext(sslContextMock);
        verify(sslContextBuilderMock).build();

        verifyNoMoreInteractions(sslContextMock, sslContextBuilderMock);
    }

    @Test
    public void testSSLContextInitKeyStoreOnly() throws Exception {

        final String keyStoreFile = "/a/b/c/keystore.jks";
        final String keyStorePassword = "keyStorePassword";
        final String keyPassword = "keyPassword";

        final SSLConfigurationCallback cb = SSLConfigurationCallback.Builder.
                createCustom(sslContextBuilderMock).
                withKeyStore(keyStoreFile, keyStorePassword, keyPassword).
                build();

        cb.customizeHttpClient(httpAsyncClientBuilderMock);

        verify(sslContextBuilderMock).loadKeyMaterial(eq(new File(keyStoreFile)),
                eq(keyStorePassword.toCharArray()), eq(keyPassword.toCharArray()));
        verify(httpAsyncClientBuilderMock).setSSLContext(sslContextMock);
        verify(sslContextBuilderMock).loadTrustMaterial((TrustStrategy)null);
        verify(sslContextBuilderMock).build();

        verifyNoMoreInteractions(sslContextMock, sslContextBuilderMock);
    }

    @Test
    public void testDisableHostNameVerificationDefaultOff() throws Exception {
        final SSLConfigurationCallback cb = SSLConfigurationCallback.Builder.
                createCustom(sslContextBuilderMock).
                build();
        cb.customizeHttpClient(httpAsyncClientBuilderMock);
        // assuming that the callback does not modify the default host name verifier if not requested
        verify(sslContextBuilderMock).loadTrustMaterial((TrustStrategy)null);
        verify(sslContextBuilderMock).build();
        verify(httpAsyncClientBuilderMock).setSSLContext(sslContextMock);
        verifyNoMoreInteractions(sslContextMock, sslContextBuilderMock, httpAsyncClientBuilderMock);
    }

    @Test
    public void testDisableHostNameVerification() throws Exception {
        final SSLConfigurationCallback cb = SSLConfigurationCallback.Builder.
                createCustom(sslContextBuilderMock).
                disableHostNameVerification().
                build();
        cb.customizeHttpClient(httpAsyncClientBuilderMock);
        final ArgumentCaptor<HostnameVerifier> hostnameVerifierCaptor = ArgumentCaptor.forClass(HostnameVerifier.class);
        verify(httpAsyncClientBuilderMock).setSSLHostnameVerifier(hostnameVerifierCaptor.capture());
        verify(sslContextBuilderMock).loadTrustMaterial((TrustStrategy)null);
        verify(sslContextBuilderMock).build();
        verify(httpAsyncClientBuilderMock).setSSLContext(sslContextMock);
        verifyNoMoreInteractions(sslContextMock, sslContextBuilderMock, httpAsyncClientBuilderMock);

        assertEquals(1, hostnameVerifierCaptor.getAllValues().size());
        final HostnameVerifier verifier = hostnameVerifierCaptor.getValue();
        // this assertion is implementation-specific but should be good enough
        // given the simplicity of the class under test
        assertTrue(verifier instanceof NoopHostnameVerifier);
    }

    @Test
    public void testAllowSelfSignedCertsDefaultOff() throws Exception {
        final SSLConfigurationCallback cb = SSLConfigurationCallback.Builder.
                createCustom(sslContextBuilderMock).
                build();
        cb.customizeHttpClient(httpAsyncClientBuilderMock);

        verify(sslContextBuilderMock).loadTrustMaterial((TrustStrategy)null);
        verify(sslContextBuilderMock).build();
        verify(httpAsyncClientBuilderMock).setSSLContext(sslContextMock);
        verifyNoMoreInteractions(sslContextMock, sslContextBuilderMock, httpAsyncClientBuilderMock);
    }

    @Test
    public void testAllowSelfSignedCerts() throws Exception {
        final SSLConfigurationCallback cb = SSLConfigurationCallback.Builder.
                createCustom(sslContextBuilderMock).
                allowSelfSignedCertificates().
                build();
        cb.customizeHttpClient(httpAsyncClientBuilderMock);
        final ArgumentCaptor<TrustStrategy> trustStrategyCaptor = ArgumentCaptor.forClass(TrustStrategy.class);
        verify(sslContextBuilderMock).loadTrustMaterial(trustStrategyCaptor.capture());
        verify(sslContextBuilderMock).build();
        verify(httpAsyncClientBuilderMock).setSSLContext(sslContextMock);
        verifyNoMoreInteractions(sslContextMock, sslContextBuilderMock, httpAsyncClientBuilderMock);

        assertEquals(1, trustStrategyCaptor.getAllValues().size());
        final TrustStrategy trustStrategy = trustStrategyCaptor.getValue();
        // this assertion is implementation-specific but should be good enough
        // given the simplicity of the class under test
        assertTrue(trustStrategy instanceof TrustSelfSignedStrategy);
    }
}
