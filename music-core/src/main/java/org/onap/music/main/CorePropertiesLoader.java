/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
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
 */

package org.onap.music.main;

import java.util.Properties;

import org.onap.music.eelf.logging.EELFLoggerDelegate;

public class CorePropertiesLoader {

    public static void loadProperties(Properties properties) {
        if (properties.getProperty("cassandra.host")!=null) {
            MusicUtil.setMyCassaHost(properties.getProperty("cassandra.host"));
        }

        if (properties.getProperty("cassandra.port")!=null) {
            MusicUtil.setCassandraPort(Integer.parseInt(properties.getProperty("cassandra.port")));
        }

        if (properties.getProperty("cassandra.user")!=null) {
            MusicUtil.setCassName(properties.getProperty("cassandra.user"));
        }

        if (properties.getProperty("cassandra.password")!=null) {
            MusicUtil.setCassPwd(properties.getProperty("cassandra.password"));
        }
        
        if (properties.getProperty("music.properties")!=null) {
            MusicUtil.setMusicPropertiesFilePath(properties.getProperty("music.properties"));
        }

        if (properties.getProperty("debug")!=null) {
            MusicUtil.setDebug(Boolean.parseBoolean(properties.getProperty("debug")));
        }

        if (properties.getProperty("version")!=null) {
            MusicUtil.setVersion(properties.getProperty("version"));
        }

        if (properties.getProperty("build")!=null) {
            MusicUtil.setBuild(properties.getProperty("build"));
        }

        if (properties.getProperty("lock.lease.period")!=null) {
            MusicUtil.setDefaultLockLeasePeriod(Long.parseLong(properties.getProperty("lock.lease.period")));
        }

        if (properties.getProperty("cadi")!=null) {
            MusicUtil.setIsCadi(Boolean.parseBoolean(properties.getProperty("cadi")));
        }

        if (properties.getProperty("keyspace.active")!=null) {
            MusicUtil.setKeyspaceActive(Boolean.parseBoolean(properties.getProperty("keyspace.active")));
        }

        if (properties.getProperty("retry.count")!=null) {
            MusicUtil.setRetryCount(Integer.parseInt(properties.getProperty("retry.count")));
        }

        if (properties.getProperty("transId.header.prefix")!=null) {
            MusicUtil.setTransIdPrefix(properties.getProperty("transId.header.prefix"));
        }

        if (properties.getProperty("conversation.header.prefix")!=null) {
            MusicUtil.setConversationIdPrefix(properties.getProperty("conversation.header.prefix"));
        }

        if (properties.getProperty("clientId.header.prefix")!=null) {
            MusicUtil.setClientIdPrefix(properties.getProperty("clientId.header.prefix"));
        }

        if (properties.getProperty("messageId.header.prefix")!=null) {
            MusicUtil.setMessageIdPrefix(properties.getProperty("messageId.header.prefix"));
        }

        if (properties.getProperty("transId.header.required")!=null) {
            MusicUtil.setTransIdRequired(Boolean.parseBoolean(properties.getProperty("transId.header.required")));
        }

        if (properties.getProperty("conversation.header.required")!=null) {
            MusicUtil.setConversationIdRequired(Boolean.parseBoolean(properties.getProperty("conversation.header.required")));
        }

        if (properties.getProperty("clientId.header.required")!=null) {
            MusicUtil.setClientIdRequired(Boolean.parseBoolean(properties.getProperty("clientId.header.required")));
        }

        if (properties.getProperty("messageId.header.required")!=null) {
            MusicUtil.setMessageIdRequired(Boolean.parseBoolean(properties.getProperty("messageId.header.required")));
        }

        if (properties.getProperty("music.aaf.ns")!=null) {
            MusicUtil.setMusicAafNs(properties.getProperty("music.aaf.ns"));
        }

        if (properties.getProperty("cipher.enc.key")!=null) {
            MusicUtil.setCipherEncKey(properties.getProperty("cipher.enc.key"));
        }

    }
    
}
