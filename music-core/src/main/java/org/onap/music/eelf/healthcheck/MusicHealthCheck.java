/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 * =================================================================== 
 *  Modification Copyright (c) 2020 IBM Intellectual Property
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

import java.util.UUID;

import org.onap.music.datastore.MusicDataStoreHandle;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.main.MusicCore;

import com.datastax.driver.core.ConsistencyLevel;

/**
 * @author inam
 *
 */
public class MusicHealthCheck {

    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicUtil.class);

    private String cassandrHost;

    public String getCassandraStatus(String consistency) {
        logger.info(EELFLoggerDelegate.applicationLogger, "Getting Status for Cassandra");
        
        boolean result = false;
        UUID randomUUID = UUID.randomUUID();
        try {
            result = getAdminKeySpace(consistency, randomUUID);
        } catch( Exception e) {
            if(e.getMessage().toLowerCase().contains("unconfigured table healthcheck")) {
                logger.error("Error", e);
                logger.debug("Creating table....");
                try {
                    boolean ksresult = createKeyspace();
                    if(ksresult) {
                        result = getAdminKeySpace(consistency, randomUUID);
                    }
                } catch (MusicServiceException e1) {
                    logger.error(EELFLoggerDelegate.errorLogger, e1.getMessage(), AppMessages.UNKNOWNERROR, ErrorSeverity.ERROR, ErrorTypes.UNKNOWN, e1);
                } catch (MusicQueryException e1) {
                    logger.error(EELFLoggerDelegate.errorLogger, e1.getMessage(), AppMessages.UNKNOWNERROR, ErrorSeverity.ERROR, ErrorTypes.UNKNOWN,e1);
                }
            } else {
                logger.error("Error", e);
                return "One or more nodes are down or not responding.";
            }
        }
        try {
			cleanHealthCheckId(randomUUID);
		} catch (MusicServiceException | MusicQueryException e) {
			logger.error("Error while cleaning healthcheck record id...", e);
		}
        if (result) {
            return "ACTIVE";
        } else {
            logger.info(EELFLoggerDelegate.applicationLogger, "Cassandra Service is not responding");
            return "INACTIVE";
        }
    }

    private Boolean getAdminKeySpace(String consistency, UUID randomUUID) throws MusicServiceException,MusicQueryException {
        PreparedQueryObject pQuery = new PreparedQueryObject();
        pQuery.appendQueryString("insert into admin.healthcheck (id) values (?)");
        pQuery.addValue(randomUUID);
        ResultType rs = null;
        rs = MusicCore.nonKeyRelatedPut(pQuery, consistency);
        logger.info(rs.toString());
        return null != rs;
        
    }

	private void cleanHealthCheckId(UUID randomUUID) throws MusicServiceException, MusicQueryException {
		String cleanQuery = "delete  from admin.healthcheck where id = ?";
        PreparedQueryObject deleteQueryObject = new PreparedQueryObject();
        deleteQueryObject.appendQueryString(cleanQuery);
        deleteQueryObject.addValue(randomUUID);
        MusicDataStoreHandle.getDSHandle().executePut(deleteQueryObject, "eventual");  
        logger.info(EELFLoggerDelegate.applicationLogger, "Cassandra healthcheck responded and cleaned up.");
	}
    
    
    
    private boolean createKeyspace() throws MusicServiceException,MusicQueryException {
        PreparedQueryObject pQuery = new PreparedQueryObject();
        pQuery.appendQueryString("CREATE TABLE admin.healthcheck (id uuid PRIMARY KEY)");
        ResultType rs = null ;
        rs = MusicCore.nonKeyRelatedPut(pQuery, ConsistencyLevel.ONE.toString());
        return rs != null && rs.getResult().toLowerCase().contains("success");
    }

    /*For unit testing purpose only*/
    public ResultType nonKeyRelatedPut(PreparedQueryObject pQuery, String consistency) throws MusicServiceException, MusicQueryException {
        return MusicCore.nonKeyRelatedPut(pQuery, consistency);
    }

    /*For unit testing purpose only*/
    public void executeEventualPut(PreparedQueryObject deleteQueryObject) throws MusicServiceException, MusicQueryException {
        MusicDataStoreHandle.getDSHandle().executePut(deleteQueryObject, "eventual");
    }
    public String getCassandrHost() {
        return cassandrHost;
    }

    public void setCassandrHost(String cassandrHost) {
        this.cassandrHost = cassandrHost;
    }

}