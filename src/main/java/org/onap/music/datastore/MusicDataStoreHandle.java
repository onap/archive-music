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
package org.onap.music.datastore;

import java.util.HashMap;
import java.util.Map;

import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicUtil;
import org.onap.music.service.impl.MusicCassaCore;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.TableMetadata;

public class MusicDataStoreHandle {
	
	 public static MusicDataStore mDstoreHandle = null;
	 private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicDataStoreHandle.class);

    /**
     *
     * @param remoteAddress
     * @return
     */
    public static MusicDataStore getDSHandle(String remoteAddress) {
        logger.info(EELFLoggerDelegate.applicationLogger,"Acquiring data store handle");
        long start = System.currentTimeMillis();
        if (mDstoreHandle == null) {
            try {
                MusicUtil.loadProperties();
            } catch (Exception e) {
                logger.error(EELFLoggerDelegate.errorLogger, "No properties file defined. Falling back to default.");
            }
            mDstoreHandle = new MusicDataStore(remoteAddress);
        }
        long end = System.currentTimeMillis();
        logger.info(EELFLoggerDelegate.applicationLogger,"Time taken to acquire data store handle:" + (end - start) + " ms");
        return mDstoreHandle;
    }

    /**
     * 
     * @return
     * @throws MusicServiceException 
     */
    public static MusicDataStore getDSHandle() throws MusicServiceException {
		
        logger.info(EELFLoggerDelegate.applicationLogger,"Acquiring data store handle");
        long start = System.currentTimeMillis();
        if (mDstoreHandle == null) {
        	try {
    			MusicUtil.loadProperties();
    		} catch (Exception e) {
    			logger.error(EELFLoggerDelegate.errorLogger, "No properties file defined. Falling back to default.");
    		}
            // Quick Fix - Best to put this into every call to getDSHandle?
            if (MusicUtil.getMyCassaHost().equals("localhost")) {
                mDstoreHandle = new MusicDataStore();
            } else {
                mDstoreHandle = new MusicDataStore(MusicUtil.getMyCassaHost());
            }
        }
        if(mDstoreHandle.getSession() == null) {
        	String message = "Connection to Cassandra has not been enstablished."
        			+ " Please check connection properites and reboot.";
        	logger.info(EELFLoggerDelegate.applicationLogger, message);
            throw new MusicServiceException(message);
        }
        long end = System.currentTimeMillis();
        logger.info(EELFLoggerDelegate.applicationLogger,"Time taken to acquire data store handle:" + (end - start) + " ms");
        return mDstoreHandle;
    }
    
    /**
     * 
     * @param keyspace
     * @param tablename
     * @return
     * @throws MusicServiceException 
     */
    public static TableMetadata returnColumnMetadata(String keyspace, String tablename) throws MusicServiceException {
        return MusicDataStoreHandle.getDSHandle().returnColumnMetadata(keyspace, tablename);
    }
    
    /**
     * 
     * @param results
     * @return
     * @throws MusicServiceException 
     */
    public static Map<String, HashMap<String, Object>> marshallResults(ResultSet results) throws MusicServiceException {
        return MusicDataStoreHandle.getDSHandle().marshalData(results);
    }

}
