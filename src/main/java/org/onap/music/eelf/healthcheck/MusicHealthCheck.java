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

import java.util.UUID;

import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.lockingservice.MusicLockingService;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;

import com.datastax.driver.core.ConsistencyLevel;

/**
 * @author inam
 *
 */
public class MusicHealthCheck {

	private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicUtil.class);

	private String cassandrHost;
	private String zookeeperHost;
	private String error = "Error";

	public String getCassandraStatus(String consistency) {
		logger.info(EELFLoggerDelegate.applicationLogger, "Getting Status for Cassandra");
		
		boolean result = false;
		try {
			result = getAdminKeySpace(consistency);
		} catch(Exception e) {
			if(e.getMessage().toLowerCase().contains("unconfigured table healthcheck")) {
				logger.error(error, e);
				logger.debug("Creating table....");
				boolean ksresult = createKeyspace();
				if(ksresult)
					try {
						result = getAdminKeySpace(consistency);
					} catch (MusicServiceException e1) {
						logger.error(error, e1);
					}
			} else {
				logger.error(error, e);
				return "One or more nodes are down or not responding.";
			}
		}
		if (result) {
			return "ACTIVE";
		} else {
			logger.info(EELFLoggerDelegate.applicationLogger, "Cassandra Service is not responding");
			return "INACTIVE";
		}
	}

	private Boolean getAdminKeySpace(String consistency) throws MusicServiceException {


		PreparedQueryObject pQuery = new PreparedQueryObject();
		pQuery.appendQueryString("insert into admin.healthcheck (id) values (?)");
		pQuery.addValue(UUID.randomUUID());
			ResultType rs = MusicCore.nonKeyRelatedPut(pQuery, consistency);
			System.out.println(rs);
			if (rs != null) {
				return Boolean.TRUE;
			} else {
				return Boolean.FALSE;
			}


	}
	
	private boolean createKeyspace() {
		PreparedQueryObject pQuery = new PreparedQueryObject();
		pQuery.appendQueryString("CREATE TABLE admin.healthcheck (id uuid PRIMARY KEY)");
		ResultType rs = null ;
		try {
			rs = MusicCore.nonKeyRelatedPut(pQuery, ConsistencyLevel.ONE.toString());
		} catch (MusicServiceException e) {
			logger.error(error, e);
		}
		if(rs != null && rs.getResult().toLowerCase().contains("success"))
			return true;
		else
			return false;
	}

	public String getZookeeperStatus() {

		String host = MusicUtil.getMyZkHost();
		logger.info(EELFLoggerDelegate.applicationLogger, "Getting Status for Zookeeper Host: " + host);
		try {
			MusicLockingService lockingService = MusicCore.getLockingServiceHandle();
			// additionally need to call the ZK to create,aquire and delete lock
		} catch (MusicLockingException e) {
			logger.error("Locking Error has occured", e);
			logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.LOCKINGERROR,
					ErrorTypes.CONNECTIONERROR, ErrorSeverity.CRITICAL);
			return "INACTIVE";
		}

		logger.info(EELFLoggerDelegate.applicationLogger, "Zookeeper is Active and Running");
		return "ACTIVE";

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
