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

    @Value("${zookeeper.host}")
    private String zookeeperHost;
    
    @Value("${cassandra.host}")
    public String cassandraHost;
    
    @Value("${music.ip}")
    public String musicIp;        
    
    @Value("${debug}")
    public String debug;
    
    @Value("${version}")
    public String version;
    
    @Value("${music.rest.ip}")
    public String musicRestIp;
    
    @Value("${music.properties}")
    public String musicProperties;
    
    @Value("${lock.lease.period}")
    public String lockLeasePeriod;
    
    @Value("${public.ip}")
    public String publicIp;
    
    @Value("${my.id}")
    public String myId;
    
    @Value("${all.ids}")
    public String allIds;
    
    @Value("${all.public.ips}")
    public String allPublicIps;
    
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
    
    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(PropertiesLoader.class);
    
    @Bean
    public static PropertySourcesPlaceholderConfigurer propertyConfigInDev() {
        //return new PropertySourcesPlaceholderConfigurer();
        PropertySourcesPlaceholderConfigurer pspc = new PropertySourcesPlaceholderConfigurer();
        pspc.setIgnoreResourceNotFound(true);
        pspc.setIgnoreUnresolvablePlaceholders(true);
        return pspc;
    }
    
    public void loadProperties () {
        if(aafAdminUrl != null && !aafAdminUrl.equals("${aaf.admin.url}"))
            MusicUtil.setAafAdminUrl(aafAdminUrl);
        if(aafEndpointUrl != null && !aafEndpointUrl.equals("${aaf.endpoint.url}"))
            MusicUtil.setAafEndpointUrl(aafEndpointUrl);
        if(adminAafRole != null && !adminAafRole.equals("${admin.aaf.role}"))
            MusicUtil.setAdminAafRole(adminAafRole);
        //MusicUtil.setAdminId(adminId);
        if(adminPassword != null && !adminPassword.equals("${admin.password}"))
            MusicUtil.setAdminPass(adminPassword);
        if(adminUsername != null && !adminUsername.equals("${admin.username}"))
            MusicUtil.setAdminId(adminUsername);
        if(allIds != null && !allIds.equals("${all.ids}")) {
            String[] ids = allIds.split(":");
            MusicUtil.setAllIds(new ArrayList<String>(Arrays.asList(ids)));
        }
        if(allPublicIps != null && !allPublicIps.equals("${all.public.ips}")) {
            String[] ips = allPublicIps.split(":");
            if (ips.length == 1) {
                // Future use
            } else if (ips.length > 1) {
                MusicUtil.setAllPublicIps(
                                new ArrayList<String>(Arrays.asList(ips)));
            }
        }
        if(cassandraPort != null && !cassandraPort.equals("${cassandra.port}"))
            MusicUtil.setCassandraPort(Integer.parseInt(cassandraPort));
        if(cassandraUser != null && !cassandraUser.equals("${cassandra.user}"))
            MusicUtil.setCassName(cassandraUser);
        if(cassandraPassword != null && !cassandraPassword.equals("${cassandra.password}"))
            MusicUtil.setCassPwd(cassandraPassword);
        if(debug != null && !debug.equals("${debug}"))
            MusicUtil.setDebug(Boolean.parseBoolean(debug));
        if(lockLeasePeriod != null && !lockLeasePeriod.equals("${lock.lease.period}"))
            MusicUtil.setDefaultLockLeasePeriod(Long.parseLong(lockLeasePeriod));
        if(musicIp != null && !musicIp.equals("${music.ip}"))
            MusicUtil.setDefaultMusicIp(musicIp);
        if(musicNamespace != null && !musicNamespace.equals("${music.namespace}"))
            MusicUtil.setMusicNamespace(musicNamespace);
        if(musicProperties != null && !musicProperties.equals("${music.properties}"))
            MusicUtil.setMusicPropertiesFilePath(musicProperties);
        if(musicRestIp != null && !musicRestIp.equals("${music.rest.ip}"))
            MusicUtil.setMusicRestIp(musicRestIp);
        if(cassandraHost != null && !cassandraHost.equals("${cassandra.host}")) 
            MusicUtil.setMyCassaHost(cassandraHost);
        logger.info("#### Cassandra Host: " + MusicUtil.getMyCassaHost());
        if(myId != null && !myId.equals("${my.id}")) 
            MusicUtil.setMyId(Integer.parseInt(myId));
        if(zookeeperHost != null && !zookeeperHost.equals("${zookeeper.host}"))
            MusicUtil.setMyZkHost(zookeeperHost);
        if(notifyInterval != null && !notifyInterval.equals("${notify.interval}")) 
            MusicUtil.setNotifyInterval(Integer.parseInt(notifyInterval));
        if(notifyTimeout != null && !notifyTimeout.equals("${notify.timeout}"))
            MusicUtil.setNotifyTimeOut(Integer.parseInt(notifyTimeout));
        if(allPublicIps != null && !allPublicIps.equals("${public.ip}"))
            MusicUtil.setPublicIp(allPublicIps);
        if(version != null && !version.equals("${version}"))
            MusicUtil.setVersion(version);
        if(isCadi != null && !isCadi.equals("${cadi}"))
            MusicUtil.setIsCadi(Boolean.parseBoolean(isCadi));
    }
    
    
    @Override
    public void afterPropertiesSet() throws Exception {
        // TODO Auto-generated method stub
        
    }
        
}
