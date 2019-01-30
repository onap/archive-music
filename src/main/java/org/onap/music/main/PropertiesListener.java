/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 *  Modifications Copyright (C) 2018 IBM.
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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;

import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicServiceException;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class PropertiesListener { // implements ServletContextListener {
    private Properties prop;
    private static final String MUSIC_PROPERTIES="music.properties";
/*    private Properties prop;

>>>>>>> c8db07f77a945bc22046ef50d773c3c3608b014a
    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(PropertiesListener.class);

    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        prop = new Properties();
        Properties projectProp = new Properties();
        URL resource = getClass().getResource("/");
        String musicPropertiesFilePath = resource.getPath().replace("WEB-INF/classes/","WEB-INF/classes/project.properties");

        // Open the file
        try {
            InputStream musicProps = null;
            projectProp.load(new FileInputStream(musicPropertiesFilePath));
            if (projectProp.containsKey(MUSIC_PROPERTIES)) {
                musicProps = new FileInputStream(projectProp.getProperty(MUSIC_PROPERTIES));
            } else {
                musicProps = new FileInputStream(MusicUtil.getMusicPropertiesFilePath());
            }
            prop.load(musicProps);
            musicProps.close();
            prop.putAll(projectProp);
            String[] propKeys = MusicUtil.getPropkeys();
            for (int k = 0; k < propKeys.length; k++) {
                String key = propKeys[k];
                if (prop.containsKey(key) && prop.get(key) != null) {
                    logger.info(key + " : " + prop.getProperty(key));
                    switch (key) {
                        case "zookeeper.host":
                            MusicUtil.setMyZkHost(prop.getProperty(key));
                            break;
                        case "cassandra.host":
                            MusicUtil.setMyCassaHost(prop.getProperty(key));
                            break;
                        case "music.ip":
                            MusicUtil.setDefaultMusicIp(prop.getProperty(key));
                            break;
                        case "debug":
                            MusicUtil.setDebug(Boolean
                                            .getBoolean(prop.getProperty(key).toLowerCase()));
                            break;
                        case "version":
                            MusicUtil.setVersion(prop.getProperty(key));
                            break;
                        case "music.rest.ip":
                            MusicUtil.setMusicRestIp(prop.getProperty(key));
                            break;
                        case MUSIC_PROPERTIES:
                            MusicUtil.setMusicPropertiesFilePath(prop.getProperty(key));
                            break;
                        case "lock.lease.period":
                            MusicUtil.setDefaultLockLeasePeriod(
                                            Long.parseLong(prop.getProperty(key)));
                            break;
                        case "my.id":
                            MusicUtil.setMyId(Integer.parseInt(prop.getProperty(key)));
                            break;
                        case "all.ids":
                            String[] ids = prop.getProperty(key).split(":");
                            MusicUtil.setAllIds(new ArrayList<String>(Arrays.asList(ids)));
                            break;
                        case "public.ip":
                            MusicUtil.setPublicIp(prop.getProperty(key));
                            break;
                        case "all.public.ips":
                            String[] ips = prop.getProperty(key).split(":");
                            if (ips.length == 1) {
                                // Future use
                            } else if (ips.length > 1) {
                                MusicUtil.setAllPublicIps(
                                                new ArrayList<String>(Arrays.asList(ips)));
                            }
                            break;
                        case "cassandra.user":
                            MusicUtil.setCassName(prop.getProperty(key));
                            break;
                        case "cassandra.password":
                            MusicUtil.setCassPwd(prop.getProperty(key));
                            break;
                        case "aaf.endpoint.url":
                            MusicUtil.setAafEndpointUrl(prop.getProperty(key));
                            break;
                        case "admin.username":
                            MusicUtil.setAdminId(prop.getProperty(key));
                            break;
                        case "admin.password":
                            MusicUtil.setAdminPass(prop.getProperty(key));
                            break;
                        case "cassandra.port":
                            MusicUtil.setCassandraPort(Integer.parseInt(prop.getProperty(key)));
                            break;
                        case "aaf.admin.url":
                            MusicUtil.setAafAdminUrl(prop.getProperty(key));
                            break;
                        case "music.namespace":
                            MusicUtil.setMusicNamespace(prop.getProperty(key));
                            break;
                        case "admin.aaf.role":
                            MusicUtil.setAdminAafRole(prop.getProperty(key));
                            break; 
                        case "notify.interval":
                            MusicUtil.setNotifyInterval(Integer.parseInt(prop.getProperty(key)));
                            break;
                        case "notify.timeout":
                            MusicUtil.setNotifyTimeOut(Integer.parseInt(prop.getProperty(key)));
                            break;
                        case "lock.using":
                            MusicUtil.setLockUsing(prop.getProperty(key));
                            break; 
                        case "cacheobject.maxlife":
                            MusicUtil.setCacheObjectMaxLife(Integer.parseInt(prop.getProperty(key)));
                            CachingUtil.setCacheEternalProps();
                            break;
                        default:
                            logger.error(EELFLoggerDelegate.errorLogger,
                                            "No case found for " + key);
                    }
                }
            }
        } catch (IOException e) {
            logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), AppMessages.IOERROR  ,ErrorSeverity.CRITICAL, ErrorTypes.CONNECTIONERROR);
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage());
        }

        logger.info(EELFLoggerDelegate.applicationLogger,
                        "Starting MUSIC " + MusicUtil.getVersion() + " on node with id "
                                        + MusicUtil.getMyId() + " and public ip "
                                        + MusicUtil.getPublicIp() + "...");
        logger.info(EELFLoggerDelegate.applicationLogger,
                        "List of all MUSIC ids:" + MusicUtil.getAllIds().toString());
        logger.info(EELFLoggerDelegate.applicationLogger,
                        "List of all MUSIC public ips:" + MusicUtil.getAllPublicIps().toString());
        
        scheduleCronJobForZKCleanup();
    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        prop = null;
    }
    
    
    private ScheduledExecutorService scheduler;
    public void scheduleCronJobForZKCleanup() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(new CachingUtil(), 0, 24, TimeUnit.HOURS);
        PreparedQueryObject pQuery = new PreparedQueryObject();
        String consistency = MusicUtil.EVENTUAL;
        pQuery.appendQueryString("CREATE TABLE IF NOT EXISTS admin.locks ( lock_id text PRIMARY KEY, ctime text)");
        try {
            ResultType result = MusicCore.nonKeyRelatedPut(pQuery, consistency);
        } catch (MusicServiceException e1) {
            logger.error(EELFLoggerDelegate.errorLogger, e1.getMessage(),ErrorSeverity.ERROR);
        }

      //Zookeeper cleanup
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                deleteLocksFromDB();
            }
        } , 0, 24, TimeUnit.HOURS);
    }


    public void deleteLocksFromDB() {
        PreparedQueryObject pQuery = new PreparedQueryObject();
        pQuery.appendQueryString(
                        "select * from admin.locks");
            try {
                ResultSet rs = MusicCore.get(pQuery);
                Iterator<Row> it = rs.iterator();
                StringBuilder deleteKeys = new StringBuilder();
                Boolean expiredKeys = false;
                while (it.hasNext()) {
                    Row row = (Row) it.next();
                    String id = row.getString("lock_id");
                    long ctime = Long.parseLong(row.getString("ctime"));
                    if(System.currentTimeMillis() >= ctime + 24 * 60 * 60 * 1000) {
                        expiredKeys = true;
                        String new_id = id.substring(1);
                        try {
                            MusicCore.deleteLock(new_id);
                        } catch (MusicLockingException e) {
                            logger.info(EELFLoggerDelegate.applicationLogger,
                                     e.getMessage());
                        }
                        deleteKeys.append("'").append(id).append("'").append(",");
                    }
                }
                if(expiredKeys) {
                    deleteKeys.deleteCharAt(deleteKeys.length()-1);
                    CachingUtil.deleteKeysFromDB(deleteKeys.toString());
               }
            } catch (MusicServiceException e) {
                logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),ErrorSeverity.ERROR);
            }
    }
*/
}
