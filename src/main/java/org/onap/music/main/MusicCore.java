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


import java.util.List;

import com.datastax.driver.core.*;
import org.onap.music.datastore.*;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.lockingservice.cassandra.CassaLockStore;
import org.onap.music.lockingservice.cassandra.MusicLockState;
import org.onap.music.service.MusicCoreService;
import org.onap.music.service.impl.MusicCassaCore;


/**
 * This class .....
 * 
 *
 */
public class MusicCore {

    public static CassaLockStore mLockHandle = null;
    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicCore.class);
    private static boolean unitTestRun=true;
    
	private static MusicCoreService _musicCore = null;

	public static MusicCoreService getSingletonInstance() {
		if (_musicCore == null) {
			String address = MusicUtil.getMyCassaHost();
			Cluster cluster;
			try {
				cluster = CassandraClusterBuilder.connectSmart(MusicUtil.getMyCassaHost());
			} catch (MusicServiceException e) {
				logger.error(EELFLoggerDelegate.errorLogger, "Can not connect to Cassandra cluster: " + address);
				return null;
			}
			Metadata metadata = cluster.getMetadata();
			logger.info(EELFLoggerDelegate.applicationLogger, "Connected to Cassandra cluster "
					+ metadata.getClusterName() + " at " + address);
			Session session = cluster.connect();

			_musicCore = new MusicCassaCore(cluster, session);
		}

		return _musicCore;
	}

	public static ReturnType acquireLock(String fullyQualifiedKey, String lockReference) throws MusicLockingException, MusicQueryException, MusicServiceException {
		return getSingletonInstance().acquireLock(fullyQualifiedKey, lockReference);
	}
	
	public static ReturnType acquireLockWithLease(String key, String lockReference, long leasePeriod) throws MusicLockingException, MusicQueryException, MusicServiceException {
		return getSingletonInstance().acquireLockWithLease(key, lockReference, leasePeriod);
	}
	
	public static String createLockReference(String fullyQualifiedKey) {
		return getSingletonInstance().createLockReference(fullyQualifiedKey);
	}
	
	public static String createLockReference(String fullyQualifiedKey, boolean isWriteLock) {
	    return getSingletonInstance().createLockReference(fullyQualifiedKey, isWriteLock);
	}
	
	public static MusicLockState  forciblyReleaseLock(String fullyQualifiedKey, String lockReference) throws MusicLockingException, MusicServiceException, MusicQueryException{
		return getSingletonInstance().forciblyReleaseLock(fullyQualifiedKey, lockReference);
	}
	
	public static ResultType createTable(String keyspace, String table, PreparedQueryObject tableQueryObject, String consistency) throws MusicServiceException {
		return getSingletonInstance().createTable(keyspace, table, tableQueryObject, consistency);
	}
	
	public static ResultSet quorumGet(PreparedQueryObject query) {
		return getSingletonInstance().quorumGet(query);
	}
	
	public static String whoseTurnIsIt(String fullyQualifiedKey) {
		return getSingletonInstance().whoseTurnIsIt(fullyQualifiedKey);
	}
	
	public static MusicLockState destroyLockRef(String fullyQualifiedKey, String lockReference) {
		return getSingletonInstance().destroyLockRef(fullyQualifiedKey, lockReference);
	}
	
	public static  MusicLockState  voluntaryReleaseLock(String fullyQualifiedKey, String lockReference) throws MusicLockingException {
		return getSingletonInstance().voluntaryReleaseLock(fullyQualifiedKey, lockReference);
	}
	
	public static ReturnType eventualPut(PreparedQueryObject queryObject) {
		return getSingletonInstance().eventualPut(queryObject);
	}
	
	public static ReturnType criticalPut(String keyspace, String table, String primaryKeyValue,
            PreparedQueryObject queryObject, String lockReference, Condition conditionInfo) {
		return getSingletonInstance().criticalPut(keyspace, table, primaryKeyValue, queryObject, lockReference, conditionInfo);
	}
	
	public static ResultType nonKeyRelatedPut(PreparedQueryObject queryObject, String consistency) throws MusicServiceException {
		return getSingletonInstance().nonKeyRelatedPut(queryObject, consistency);
	}
	
	public static ResultSet get(PreparedQueryObject queryObject) throws MusicServiceException{
		return getSingletonInstance().get(queryObject);
	}
	
	public static ResultSet criticalGet(String keyspace, String table, String primaryKeyValue,
            PreparedQueryObject queryObject, String lockReference) throws MusicServiceException{
		return getSingletonInstance().criticalGet(keyspace, table, primaryKeyValue, queryObject,lockReference);
	}
	
	public static ReturnType atomicPut(String keyspaceName, String tableName, String primaryKey,
            PreparedQueryObject queryObject, Condition conditionInfo) throws MusicLockingException, MusicQueryException, MusicServiceException 
	{
		return getSingletonInstance().atomicPut(keyspaceName, tableName, primaryKey, queryObject, conditionInfo);
	}
	
    public static ResultSet atomicGet(String keyspaceName, String tableName, String primaryKey,
            PreparedQueryObject queryObject) throws MusicServiceException, MusicLockingException, MusicQueryException {
    	return getSingletonInstance().atomicGet(keyspaceName, tableName, primaryKey, queryObject);
    }
    
    public static List<String> getLockQueue(String fullyQualifiedKey)
			throws MusicServiceException, MusicQueryException, MusicLockingException{
    	return getSingletonInstance().getLockQueue(fullyQualifiedKey);
    }
    
	public static long getLockQueueSize(String fullyQualifiedKey)
			throws MusicServiceException, MusicQueryException, MusicLockingException {
		return getSingletonInstance().getLockQueueSize(fullyQualifiedKey);
	}

	public static MusicDataStore getInstanceDSHandle() {
		MusicCoreService singletonInstance = getSingletonInstance();
		if (singletonInstance instanceof MusicCassaCore)
			return ((MusicCassaCore) singletonInstance).getDataStoreHandle();
		return null;
	}
}
