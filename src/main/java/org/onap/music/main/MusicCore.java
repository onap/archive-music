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


import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.onap.music.datastore.Condition;
import org.onap.music.datastore.MusicDataStore;
import org.onap.music.datastore.MusicDataStoreHandle;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.lockingservice.cassandra.CassaLockStore;
import org.onap.music.lockingservice.cassandra.MusicLockState;
import org.onap.music.lockingservice.cassandra.CassaLockStore.LockObject;
import org.onap.music.service.MusicCoreService;
import org.onap.music.service.impl.MusicCassaCore;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;


/**
 * This class .....
 * 
 *
 */
public class MusicCore {

    public static CassaLockStore mLockHandle = null;
    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicCore.class);
    private static boolean unitTestRun=true;
    
private static MusicCoreService musicCore = MusicCassaCore.getInstance();
	
	
	public static ReturnType acquireLock(String fullyQualifiedKey, String lockReference) throws MusicLockingException, MusicQueryException, MusicServiceException {
		return musicCore.acquireLock(fullyQualifiedKey, lockReference);
	}
	
	public static ReturnType acquireLockWithLease(String key, String lockReference, long leasePeriod) throws MusicLockingException, MusicQueryException, MusicServiceException {
		return musicCore.acquireLockWithLease(key, lockReference, leasePeriod);
	}
	
	public static String createLockReference(String fullyQualifiedKey) {
		return musicCore.createLockReference(fullyQualifiedKey);
	}
	
	public static MusicLockState  forciblyReleaseLock(String fullyQualifiedKey, String lockReference) throws MusicLockingException, MusicServiceException, MusicQueryException{
		return musicCore.forciblyReleaseLock(fullyQualifiedKey, lockReference);
	}
	
	public static ResultType createTable(String keyspace, String table, PreparedQueryObject tableQueryObject, String consistency) throws MusicServiceException {
		return musicCore.createTable(keyspace, table, tableQueryObject, consistency);
	}
	
	public static ResultSet quorumGet(PreparedQueryObject query) {
		return musicCore.quorumGet(query);		
	}
	
	public static String whoseTurnIsIt(String fullyQualifiedKey) {
		return musicCore.whoseTurnIsIt(fullyQualifiedKey);
	}
	
	public static MusicLockState destroyLockRef(String fullyQualifiedKey, String lockReference) {
		return musicCore.destroyLockRef(fullyQualifiedKey, lockReference);
	}
	
	public static  MusicLockState  voluntaryReleaseLock(String fullyQualifiedKey, String lockReference) throws MusicLockingException {
		return musicCore.voluntaryReleaseLock(fullyQualifiedKey, lockReference);
	}
	
	public static ReturnType eventualPut(PreparedQueryObject queryObject) {
		return musicCore.eventualPut(queryObject);
	}
	
	public static ReturnType criticalPut(String keyspace, String table, String primaryKeyValue,
            PreparedQueryObject queryObject, String lockReference, Condition conditionInfo) {
		return musicCore.criticalPut(keyspace, table, primaryKeyValue, queryObject, lockReference, conditionInfo);
	}
	
	public static ResultType nonKeyRelatedPut(PreparedQueryObject queryObject, String consistency) throws MusicServiceException {
		return musicCore.nonKeyRelatedPut(queryObject, consistency);
	}
	
	public static ResultSet get(PreparedQueryObject queryObject) throws MusicServiceException{
		return musicCore.get(queryObject);
	}
	
	public static ResultSet criticalGet(String keyspace, String table, String primaryKeyValue,
            PreparedQueryObject queryObject, String lockReference) throws MusicServiceException{
		return musicCore.criticalGet(keyspace, table, primaryKeyValue, queryObject,lockReference);
	}
	
	public static ReturnType atomicPut(String keyspaceName, String tableName, String primaryKey,
            PreparedQueryObject queryObject, Condition conditionInfo) throws MusicLockingException, MusicQueryException, MusicServiceException 
	{
		return musicCore.atomicPut(keyspaceName, tableName, primaryKey, queryObject, conditionInfo);
	}
	
    public static ResultSet atomicGet(String keyspaceName, String tableName, String primaryKey,
            PreparedQueryObject queryObject) throws MusicServiceException, MusicLockingException, MusicQueryException {
    	return musicCore.atomicGet(keyspaceName, tableName, primaryKey, queryObject);
    }
    
    public static List<String> getLockQueue(String fullyQualifiedKey)
			throws MusicServiceException, MusicQueryException, MusicLockingException{
    	return musicCore.getLockQueue(fullyQualifiedKey);
    }
    
	public static long getLockQueueSize(String fullyQualifiedKey)
			throws MusicServiceException, MusicQueryException, MusicLockingException {
		return musicCore.getLockQueueSize(fullyQualifiedKey);
	}

}
