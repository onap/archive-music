/*******************************************************************************
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2019 AT&T Intellectual Property
 * ===================================================================
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  
 * ============LICENSE_END=============================================
 * ====================================================================
 *******************************************************************************/
package org.onap.music.main;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import java.util.Properties;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.onap.music.rest.RestMusicVersionAPI;

public class PropertiesLoaderTest {
    PropertiesLoader propertiesLoader;
    
    @Before
    public void setup() {
        propertiesLoader = new PropertiesLoader();
    }
    
    @Test
    public void testLoadProperties() {
        Properties properties = Mockito.mock(Properties.class);
        Mockito.when(properties.getProperty("cassandra.host")).thenReturn("127.0.0.1");
        Mockito.when(properties.getProperty("cassandra.port")).thenReturn("8007");
        Mockito.when(properties.getProperty("cassandra.user")).thenReturn("user");
        Mockito.when(properties.getProperty("cassandra.password")).thenReturn("password");
        Mockito.when(properties.getProperty("music.properties")).thenReturn("property");
        Mockito.when(properties.getProperty("debug")).thenReturn("true");
        Mockito.when(properties.getProperty("version")).thenReturn("x.x.x");
        Mockito.when(properties.getProperty("build")).thenReturn("y.y");
        Mockito.when(properties.getProperty("lock.lease.period")).thenReturn("5000");
        Mockito.when(properties.getProperty("cadi")).thenReturn("true");
        Mockito.when(properties.getProperty("keyspace.active")).thenReturn("true");
        Mockito.when(properties.getProperty("retry.count")).thenReturn("20");
        Mockito.when(properties.getProperty("transId.header.prefix")).thenReturn("transId");
        Mockito.when(properties.getProperty("conversation.header.prefix")).thenReturn("conversation");
        Mockito.when(properties.getProperty("clientId.header.prefix")).thenReturn("clientId");
        Mockito.when(properties.getProperty("messageId.header.prefix")).thenReturn("messageId");
        Mockito.when(properties.getProperty("transId.header.required")).thenReturn("true");
        Mockito.when(properties.getProperty("conversation.header.required")).thenReturn("true");
        Mockito.when(properties.getProperty("clientId.header.required")).thenReturn("true");
        Mockito.when(properties.getProperty("messageId.header.required")).thenReturn("true");
        Mockito.when(properties.getProperty("music.aaf.ns")).thenReturn("ns");
        Mockito.when(properties.getProperty("cipher.enc.key")).thenReturn("key");
        CorePropertiesLoader.loadProperties(properties);
        assertEquals("127.0.0.1", MusicUtil.getMyCassaHost());
        assertEquals(8007, MusicUtil.getCassandraPort());
        assertEquals("user", MusicUtil.getCassName());
        assertEquals("password", MusicUtil.getCassPwd());
        assertEquals("property", MusicUtil.getMusicPropertiesFilePath());
        assertEquals(true, MusicUtil.isDebug());
        assertEquals("x.x.x", MusicUtil.getVersion());
        assertEquals("y.y", MusicUtil.getBuild());
        assertEquals(5000L, MusicUtil.getDefaultLockLeasePeriod());
        assertEquals(true, MusicUtil.getIsCadi());
        assertEquals(true, MusicUtil.isKeyspaceActive());
        assertEquals(20, MusicUtil.getRetryCount());
        assertEquals("transId-", MusicUtil.getTransIdPrefix());
        assertEquals("conversation-", MusicUtil.getConversationIdPrefix());
        assertEquals("clientId-", MusicUtil.getClientIdPrefix());
        assertEquals("messageId-", MusicUtil.getMessageIdPrefix());
        assertEquals(true, MusicUtil.getTransIdRequired());
        assertEquals(true, MusicUtil.getConversationIdRequired());
        assertEquals(true, MusicUtil.getClientIdRequired());
        assertEquals(true, MusicUtil.getMessageIdRequired());
        assertEquals("ns", MusicUtil.getMusicAafNs());
        assertEquals("key", MusicUtil.getCipherEncKey());
        
        Mockito.when(properties.getProperty("cassandra.connecttimeoutms")).thenReturn("1000");
        Mockito.when(properties.getProperty("cassandra.readtimeoutms")).thenReturn("1000");
        Mockito.when(properties.getProperty("cassandra.connectTimeOutMS")).thenReturn("1000");
        Mockito.when(properties.getProperty("cassandra.readTimeOutMS")).thenReturn("1000");
        PropertiesLoader.loadProperties(properties);
        assertEquals("127.0.0.1", MusicUtil.getMyCassaHost());
        assertEquals(8007, MusicUtil.getCassandraPort());
        assertEquals("user", MusicUtil.getCassName());
        assertEquals("password", MusicUtil.getCassPwd());
        assertEquals(1000, MusicUtil.getCassandraConnectTimeOutMS());
        assertEquals(1000, MusicUtil.getCassandraReadTimeOutMS());
        assertEquals("property", MusicUtil.getMusicPropertiesFilePath());
        assertEquals(true, MusicUtil.isDebug());
        assertEquals("x.x.x", MusicUtil.getVersion());
        assertEquals("y.y", MusicUtil.getBuild());
        assertEquals(5000L, MusicUtil.getDefaultLockLeasePeriod());
        assertEquals(true, MusicUtil.getIsCadi());
        assertEquals(true, MusicUtil.isKeyspaceActive());
        assertEquals(20, MusicUtil.getRetryCount());
        assertEquals("transId-", MusicUtil.getTransIdPrefix());
        assertEquals("conversation-", MusicUtil.getConversationIdPrefix());
        assertEquals("clientId-", MusicUtil.getClientIdPrefix());
        assertEquals("messageId-", MusicUtil.getMessageIdPrefix());
        assertEquals(true, MusicUtil.getTransIdRequired());
        assertEquals(true, MusicUtil.getConversationIdRequired());
        assertEquals(true, MusicUtil.getClientIdRequired());
        assertEquals(true, MusicUtil.getMessageIdRequired());
        assertEquals("ns", MusicUtil.getMusicAafNs());
        assertEquals("key", MusicUtil.getCipherEncKey());
        
        propertiesLoader.setProperties();
        propertiesLoader.loadProperties();
        assertEquals("127.0.0.1", MusicUtil.getMyCassaHost());
        assertEquals(8007, MusicUtil.getCassandraPort());
        assertEquals("user", MusicUtil.getCassName());
        assertEquals("password", MusicUtil.getCassPwd());
        assertEquals(1000, MusicUtil.getCassandraConnectTimeOutMS());
        assertEquals(1000, MusicUtil.getCassandraReadTimeOutMS());
        assertEquals("property", MusicUtil.getMusicPropertiesFilePath());
        assertEquals(true, MusicUtil.isDebug());
        assertEquals("x.x.x", MusicUtil.getVersion());
        assertEquals("y.y", MusicUtil.getBuild());
        assertEquals(5000L, MusicUtil.getDefaultLockLeasePeriod());
        assertEquals(true, MusicUtil.getIsCadi());
        assertEquals(true, MusicUtil.isKeyspaceActive());
        assertEquals(20, MusicUtil.getRetryCount());
        assertEquals("transId-", MusicUtil.getTransIdPrefix());
        assertEquals("conversation-", MusicUtil.getConversationIdPrefix());
        assertEquals("clientId-", MusicUtil.getClientIdPrefix());
        assertEquals("messageId-", MusicUtil.getMessageIdPrefix());
        assertEquals(true, MusicUtil.getTransIdRequired());
        assertEquals(true, MusicUtil.getConversationIdRequired());
        assertEquals(true, MusicUtil.getClientIdRequired());
        assertEquals(true, MusicUtil.getMessageIdRequired());
        assertEquals("ns", MusicUtil.getMusicAafNs());
        assertEquals("key", MusicUtil.getCipherEncKey());
    }
}
