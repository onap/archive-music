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

package org.onap.music.eelf.healthcheck;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;


import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.mindrot.jbcrypt.BCrypt;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.datastore.jsonobjects.JsonOnboard;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.lockingservice.MusicLockingService;
import org.onap.music.main.CachingUtil;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;




/**
 * @author inam
 *
 */
public class MusicHealthCheck {
    
    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicUtil.class);
    
    private String cassandrHost;
    private String zookeeperHost;
    
    
    
    
    
    
    
    
    public String getCassandraStatus() {
        logger.info(EELFLoggerDelegate.applicationLogger,"Getting Status for Cassandra");
        if(this.getAdminKeySpace()) {
            return "ACTIVE";
        }else {
            logger.info(EELFLoggerDelegate.applicationLogger,"Cassandra Service is not responding");
            return "INACTIVE";
        }
   }
    
    
    private Boolean getAdminKeySpace() {
        
        String appName = "";
        
        PreparedQueryObject pQuery = new PreparedQueryObject();
        pQuery.appendQueryString(
                        "select * from admin.keyspace_master");
        //pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), appName));
        try {
            ResultSet rs = MusicCore.get(pQuery);
        
            if(rs != null) {
                return Boolean.TRUE;
            }else {
                return Boolean.FALSE;
            }
        } catch (Exception e) {
            logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(),AppMessages.CASSANDRACONNECTIVITY, ErrorTypes.CONNECTIONERROR, ErrorSeverity.CRITICAL);
        }
        
        return Boolean.FALSE;
        
       
    }
    
    public String getZookeeperStatus() {
        
        
        String host = MusicUtil.getMyZkHost();
        logger.info(EELFLoggerDelegate.applicationLogger,"Getting Status for Zookeeper Host: "+host);
        try {
            MusicLockingService lockingService = MusicCore.getLockingServiceHandle();
            //additionally need to call the ZK to create,aquire and delete lock
        } catch (MusicLockingException e) {
            logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(),AppMessages.LOCKINGERROR, ErrorTypes.CONNECTIONERROR, ErrorSeverity.CRITICAL);
            return "INACTIVE";
        }
    
        logger.info(EELFLoggerDelegate.applicationLogger,"Zookeeper is Active and Running");
        return "ACTIVE";
        
            //return "Zookeeper is not responding";
        
    }




    public String getCassandrHost() {
        return cassandrHost;
    }




    public void setCassandrHost(String cassandrHost) {
        this.cassandrHost = cassandrHost;
    }




    public String getZookeeperHost() {
        return zookeeperHost;
    }




    public void setZookeeperHost(String zookeeperHost) {
        this.zookeeperHost = zookeeperHost;
    }
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    

}
