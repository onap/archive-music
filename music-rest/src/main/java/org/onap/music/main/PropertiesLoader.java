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

import java.util.HashSet;
import java.util.Properties;

import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.stereotype.Component;

@PropertySource(value = {"file:/opt/app/music/etc/music.properties", "classpath:/project.properties"})
//"file:/opt/app/music/etc/key.properties"
@Component
public class PropertiesLoader implements InitializingBean {

    @Value("${cassandra.host}")
    public String cassandraHost;

    @Value("${debug}")
    public String debug;

    @Value("${version}")
    public String version;

    @Value("${build}")
    public String build;

    @Value("${music.properties}")
    public String musicProperties;

    @Value("${lock.lease.period}")
    public String lockLeasePeriod;

    @Value("${cassandra.user}")
    public String cassandraUser;

    @Value("${cassandra.password}")
    public String cassandraPassword;

    @Value("${cassandra.port}")
    public String cassandraPort;

    @Value("${cassandra.connecttimeoutms}")
    public String cassandraConnectTimeOutMS;

    @Value("${cassandra.readtimeoutms}")
    public String cassandraReadTimeOutMS;

    @Value("${cadi}")
    public String isCadi;

    @Value("${keyspace.active}")
    public String isKeyspaceActive;

    @Value("${retry.count}")
    public String rertryCount;
    
    @Value("${lock.daemon.sleeptime.ms}")
    public String lockDaemonSleeptimems;

    @Value("${keyspaces.for.lock.cleanup}")
    public String keyspacesForLockCleanup;

    @Value("${create.lock.wait.period.ms}")
    private long createLockWaitPeriod;

    @Value("${create.lock.wait.increment.ms}")
    private int createLockWaitIncrement;

    @Value("${transId.header.prefix}")
    private String transIdPrefix;

    @Value("${conversation.header.prefix}")
    private String conversationIdPrefix;

    @Value("${clientId.header.prefix}")
    private String clientIdPrefix;

    @Value("${messageId.header.prefix}")
    private String messageIdPrefix;    

    @Value("${transId.header.required}")
    private Boolean transIdRequired;

    @Value("${conversation.header.required}")
    private Boolean conversationIdRequired;

    @Value("${clientId.header.required}")
    private Boolean clientIdRequired;

    @Value("${messageId.header.required}")
    private Boolean messageIdRequired;

    @Value("${music.aaf.ns}")
    private String musicAafNs;

    @Value("${cipher.enc.key}")
    private String cipherEncKey;

    @SuppressWarnings("unused")
    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(PropertiesLoader.class);

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertyConfigInDev() {

        PropertySourcesPlaceholderConfigurer pspc = new PropertySourcesPlaceholderConfigurer();
        pspc.setIgnoreResourceNotFound(true);
        pspc.setIgnoreUnresolvablePlaceholders(true);
        return pspc;
    }

    /**
     * .
     */
    public void loadProperties() {
        if(cipherEncKey != null) {
            MusicUtil.setCipherEncKey(cipherEncKey);
        }
        if (musicAafNs != null) {
            MusicUtil.setMusicAafNs(musicAafNs);
        }
        if (cassandraPort != null && !cassandraPort.equals("${cassandra.port}")) {
            MusicUtil.setCassandraPort(Integer.parseInt(cassandraPort));
        }
        if (cassandraUser != null && !cassandraUser.equals("${cassandra.user}")) {
            MusicUtil.setCassName(cassandraUser);
        }
        if (cassandraPassword != null && !cassandraPassword.equals("${cassandra.password}")) {
            MusicUtil.setCassPwd(cassandraPassword);
        }
        if (cassandraConnectTimeOutMS != null && !cassandraConnectTimeOutMS.equals("${cassandra.connecttimeoutms}")) {
            MusicUtil.setCassandraConnectTimeOutMS(Integer.parseInt(cassandraConnectTimeOutMS));
        }
        if (cassandraReadTimeOutMS != null && !cassandraReadTimeOutMS.equals("${cassandra.readtimeoutms}")) {
            MusicUtil.setCassandraReadTimeOutMS(Integer.parseInt(cassandraReadTimeOutMS));
        }
        if (debug != null && !debug.equals("${debug}")) {
            MusicUtil.setDebug(Boolean.parseBoolean(debug));
        }
        if (lockLeasePeriod != null && !lockLeasePeriod.equals("${lock.lease.period}")) {
            MusicUtil.setDefaultLockLeasePeriod(Long.parseLong(lockLeasePeriod));
        }
        if (musicProperties != null && !musicProperties.equals("${music.properties}")) {
            MusicUtil.setMusicPropertiesFilePath(musicProperties);
        }
        if (cassandraHost != null && !cassandraHost.equals("${cassandra.host}")) {
            MusicUtil.setMyCassaHost(cassandraHost);
        }
        if (version != null && !version.equals("${version}")) {
            MusicUtil.setVersion(version);
        }
        if (build != null && !version.equals("${build}")) {
            MusicUtil.setBuild(build);
        }
        if (isCadi != null && !isCadi.equals("${cadi}")) {
            MusicUtil.setIsCadi(Boolean.parseBoolean(isCadi));
        }
        if (rertryCount != null && !rertryCount.equals("${retry.count}")) {
            MusicUtil.setRetryCount(Integer.parseInt(rertryCount));
        }
        if (isKeyspaceActive != null && !isKeyspaceActive.equals("${keyspace.active}")) {
            MusicUtil.setKeyspaceActive(Boolean.parseBoolean(isKeyspaceActive));
        }
        if (lockDaemonSleeptimems != null && !lockDaemonSleeptimems.equals("${lock.daemon.sleeptime.ms}")) {
            MusicUtil.setLockDaemonSleepTimeMs(Long.parseLong(lockDaemonSleeptimems));
        }
        if (keyspacesForLockCleanup !=null && !keyspacesForLockCleanup.equals("${keyspaces.for.lock.cleanup}")) {
            HashSet<String> keyspaces = new HashSet<>();
            for (String keyspace: keyspacesForLockCleanup.split(",")) {
                keyspaces.add(keyspace);
            }
            MusicUtil.setKeyspacesToCleanLocks(keyspaces);
        }
        if(transIdPrefix!=null) {
            MusicUtil.setTransIdPrefix(transIdPrefix);
        }

        if(conversationIdPrefix!=null) {
            MusicUtil.setConversationIdPrefix(conversationIdPrefix);
        }

        if(clientIdPrefix!=null) {
            MusicUtil.setClientIdPrefix(clientIdPrefix);
        }

        if(messageIdPrefix!=null) {
            MusicUtil.setMessageIdPrefix(messageIdPrefix);
        }

        if(transIdRequired!=null) {
            MusicUtil.setTransIdRequired(transIdRequired);
        }

        if(conversationIdRequired!=null) {
            MusicUtil.setConversationIdRequired(conversationIdRequired);
        }

        if(clientIdRequired!=null) {
            MusicUtil.setClientIdRequired(clientIdRequired);
        }

        if(messageIdRequired!=null) {
            MusicUtil.setMessageIdRequired(messageIdRequired);
        }
        
        if(createLockWaitPeriod!=0) {
            MusicUtil.setCreateLockWaitPeriod(createLockWaitPeriod);
        }

        if(createLockWaitIncrement!=0) {
            MusicUtil.setCreateLockWaitIncrement(createLockWaitIncrement);
        }
    }

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

        if(properties.getProperty("cassandra.connectTimeOutMS")!=null) {
            MusicUtil.setCassandraConnectTimeOutMS(Integer.parseInt(properties.getProperty("cassandra.connecttimeoutms")));
        }

        if(properties.getProperty("cassandra.readTimeOutMS")!=null) {
            MusicUtil.setCassandraReadTimeOutMS(Integer.parseInt(properties.getProperty("cassandra.readtimeoutms")));
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

    @Override
    public void afterPropertiesSet() throws Exception {
        // TODO Auto-generated method stub

    }
    
    /* For unit testing purpose only*/
    protected void setProperties() {
        cassandraHost = "127.0.0.1";
        debug = "true";
        version = "x.x.x";
        build = "y.y";
        musicProperties = "property";
        lockLeasePeriod = "5000";
        cassandraUser = "user";
        cassandraPassword = "password";
        cassandraPort = "8007";
        cassandraConnectTimeOutMS = "1000";
        cassandraReadTimeOutMS = "1000";
        isCadi = "true";
        isKeyspaceActive = "true";
        rertryCount = "20";
        transIdPrefix = "transId-";
        conversationIdPrefix = "conversation-";
        clientIdPrefix = "clientId-";
        messageIdPrefix = "messageId-";    
        transIdRequired = true;
        conversationIdRequired = true;
        clientIdRequired = true;
        messageIdRequired = true;
        musicAafNs = "ns";
        cipherEncKey = "key";
    }

}
