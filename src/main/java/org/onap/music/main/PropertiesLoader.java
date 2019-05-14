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

import java.util.ArrayList;
import java.util.Arrays;

import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.stereotype.Component;

@PropertySource(value = {"file:/opt/app/music/etc/music.properties", "classpath:/project.properties"})
@Component
public class PropertiesLoader implements InitializingBean {

    @Value("${cassandra.host}")
    public String cassandraHost;
    
    @Value("${music.ip}")
    public String musicIp;        
    
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
    
    @Value("${aaf.endpoint.url}")
    public String aafEndpointUrl;
    
    @Value("${admin.username}")
    public String adminUsername;
    
    @Value("${admin.password}")
    public String adminPassword;
    
    @Value("${cassandra.port}")
    public String cassandraPort;
    
    @Value("${aaf.admin.url}")
    public String aafAdminUrl;
    
    @Value("${music.namespace}")
    public String musicNamespace;
    
    @Value("${admin.aaf.role}")
    public String adminAafRole;
    
    @Value("${notify.interval}")
    public String notifyInterval;
    
    @Value("${notify.timeout}")
    public String notifyTimeout;

    @Value("${cadi}")
    public String isCadi;
    
    @Value("${keyspace.active}")
    public String isKeyspaceActive;

    @Value("${retry.count}")
    public String rertryCount;
    
    @Value("${transId.header.prefix}")
    private String transIdPrefix;

    @Value("${conversation.header.prefix}")
    private String conversationIdPrefix;

    @Value("${clientId.header.prefix}")
    private String clientIdPrefix;

    @Value("${messageId.header.prefix}")
    private String messageIdPrefix;    
    
    @Value("${transId.header.required}")
    private String transIdRequired;

    @Value("${conversation.header.required}")
    private String conversationIdRequired;

    @Value("${clientId.header.required}")
    private String clientIdRequired;

    @Value("${messageId.header.required}")
    private String messageIdRequired;
    
    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(PropertiesLoader.class);
    
    @Bean
    public static PropertySourcesPlaceholderConfigurer propertyConfigInDev() {
        //return new PropertySourcesPlaceholderConfigurer();
        PropertySourcesPlaceholderConfigurer pspc = new PropertySourcesPlaceholderConfigurer();
        pspc.setIgnoreResourceNotFound(true);
        pspc.setIgnoreUnresolvablePlaceholders(true);
        return pspc;
    }
    
    /**
     * .
     */
    public void loadProperties() {
        if (aafAdminUrl != null && !aafAdminUrl.equals("${aaf.admin.url}")) {
            MusicUtil.setAafAdminUrl(aafAdminUrl);
        }
        if (aafEndpointUrl != null && !aafEndpointUrl.equals("${aaf.endpoint.url}")) {
            MusicUtil.setAafEndpointUrl(aafEndpointUrl);
        }
        if (adminAafRole != null && !adminAafRole.equals("${admin.aaf.role}")) {
            MusicUtil.setAdminAafRole(adminAafRole);
        }
        if (adminPassword != null && !adminPassword.equals("${admin.password}")) {
            MusicUtil.setAdminPass(adminPassword);
        }
        if (adminUsername != null && !adminUsername.equals("${admin.username}")) {
            MusicUtil.setAdminId(adminUsername);
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
        if (debug != null && !debug.equals("${debug}")) {
            MusicUtil.setDebug(Boolean.parseBoolean(debug));
        }
        if (lockLeasePeriod != null && !lockLeasePeriod.equals("${lock.lease.period}")) {
            MusicUtil.setDefaultLockLeasePeriod(Long.parseLong(lockLeasePeriod));
        }
        if (musicIp != null && !musicIp.equals("${music.ip}")) {
            MusicUtil.setDefaultMusicIp(musicIp);
        }
        if (musicNamespace != null && !musicNamespace.equals("${music.namespace}")) {
            MusicUtil.setMusicNamespace(musicNamespace);
        }
        if (musicProperties != null && !musicProperties.equals("${music.properties}")) {
            MusicUtil.setMusicPropertiesFilePath(musicProperties);
        }
        if (cassandraHost != null && !cassandraHost.equals("${cassandra.host}")) {
            MusicUtil.setMyCassaHost(cassandraHost);
        }
        if (notifyInterval != null && !notifyInterval.equals("${notify.interval}")) {
            MusicUtil.setNotifyInterval(Integer.parseInt(notifyInterval));
        }
        if (notifyTimeout != null && !notifyTimeout.equals("${notify.timeout}")) {
            MusicUtil.setNotifyTimeOut(Integer.parseInt(notifyTimeout));
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
    }
    
    
    @Override
    public void afterPropertiesSet() throws Exception {
        // TODO Auto-generated method stub
        
    }
        
}
