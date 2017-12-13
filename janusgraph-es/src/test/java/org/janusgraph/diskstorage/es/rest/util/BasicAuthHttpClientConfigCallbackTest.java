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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({HttpAsyncClientBuilder.class})
public class BasicAuthHttpClientConfigCallbackTest {

    private static final String HTTP_USER = "testuser";
    private static final String HTTP_PASSWORD = "testpass";
    private static final String HTTP_REALM = "testrealm";

    private final HttpAsyncClientBuilder httpAsyncClientBuilderMock = PowerMockito.mock(HttpAsyncClientBuilder.class);

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(httpAsyncClientBuilderMock.setDefaultCredentialsProvider(anyObject())).thenReturn(httpAsyncClientBuilderMock);
    }

    private CredentialsProvider basicAuthTestBase(final String realm) {
        final BasicAuthHttpClientConfigCallback cb = new BasicAuthHttpClientConfigCallback(realm,
            BasicAuthHttpClientConfigCallbackTest.HTTP_USER, BasicAuthHttpClientConfigCallbackTest.HTTP_PASSWORD);

        cb.customizeHttpClient(httpAsyncClientBuilderMock);

        final ArgumentCaptor<BasicCredentialsProvider> cpCaptor = ArgumentCaptor.forClass(BasicCredentialsProvider.class);

        verify(httpAsyncClientBuilderMock).setDefaultCredentialsProvider(cpCaptor.capture());

        final CredentialsProvider cp = cpCaptor.getValue();
        assertNotNull(cp);

        return cp;
    }

    @Test
    public void testSetDefaultCredentialsProviderNoRealm() throws Exception {

        final CredentialsProvider cp = basicAuthTestBase("");

        // expected: will match any host and any realm
        final Credentials credentialsForRealm1 = cp.getCredentials(new AuthScope("dummyhost1", 1234, "dummyrealm1"));
        assertEquals(HTTP_USER, credentialsForRealm1.getUserPrincipal().getName());
        assertEquals(HTTP_PASSWORD, credentialsForRealm1.getPassword());
    }

    @Test
    public void testSetDefaultCredentialsProviderWithRealm() throws Exception {

        final CredentialsProvider cp = basicAuthTestBase(HTTP_REALM);

        // expected: will match any host in that specific realm
        final Credentials credentialsForRealm1 = cp.getCredentials(new AuthScope("dummyhost1", 1234, HTTP_REALM));
        assertEquals(HTTP_USER, credentialsForRealm1.getUserPrincipal().getName());
        assertEquals(HTTP_PASSWORD, credentialsForRealm1.getPassword());

        // ...but not in any other realms
        final Credentials credentialsForRealm3 = cp.getCredentials(new AuthScope("dummyhost1", 1234, "Not_" + HTTP_REALM));
        assertNull(credentialsForRealm3);
    }
}
