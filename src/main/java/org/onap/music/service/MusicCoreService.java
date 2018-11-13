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
package org.onap.music.service;

import org.onap.music.datastore.Condition;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.lockingservice.cassandra.MusicLockState;
import org.onap.music.main.ResultType;
import org.onap.music.main.ReturnType;
import org.onap.music.lockingservice.cassandra.*;

import com.datastax.driver.core.ResultSet;



/**
 * @author srupane
 *
 */
public interface MusicCoreService {
	
	// Core Music Database Methods
	

	public ReturnType eventualPut(PreparedQueryObject queryObject);

	public ReturnType criticalPut(String keyspaceName, String tableName, String primaryKey,
			PreparedQueryObject queryObject, String lockId, Condition conditionInfo);

	public ResultType nonKeyRelatedPut(PreparedQueryObject queryObject, String consistency)
			throws MusicServiceException;

	public ResultSet get(PreparedQueryObject queryObject) throws MusicServiceException;

	public ResultSet atomicGet(String keyspaceName, String tableName, String primaryKey,
			PreparedQueryObject queryObject) throws MusicServiceException, MusicLockingException, MusicQueryException;

	public ReturnType atomicPutWithDeleteLock(String keyspaceName, String tableName, String primaryKey,
			PreparedQueryObject queryObject, Condition conditionInfo) throws MusicLockingException;

	public ReturnType atomicPut(String keyspaceName, String tableName, String primaryKey,
			PreparedQueryObject queryObject, Condition conditionInfo)
			throws MusicLockingException, MusicQueryException, MusicServiceException;

	public ResultSet criticalGet(String keyspaceName, String tableName, String primaryKey,
			PreparedQueryObject queryObject, String lockId) throws MusicServiceException;

	// Core Music Locking Service Methods

	public String createLockReference(String fullyQualifiedKey); // lock name

	public ReturnType acquireLockWithLease(String key, String lockReference, long leasePeriod)
			throws MusicLockingException, MusicQueryException, MusicServiceException; // key,lock id,time

	public ReturnType acquireLock(String key, String lockReference)
			throws MusicLockingException, MusicQueryException, MusicServiceException; // key,lock id

	public ResultType createTable(String keyspace, String table, PreparedQueryObject tableQueryObject,
			String consistency) throws MusicServiceException;

	public ResultSet quorumGet(PreparedQueryObject query);

	public String whoseTurnIsIt(String fullyQualifiedKey);// lock name

	public MusicLockState destroyLockRef(String fullyQualifiedKey, String lockReference); // lock name, lock id

	public MusicLockState voluntaryReleaseLock(String fullyQualifiedKey, String lockReference)
			throws MusicLockingException;// lock name,lock id

	public void deleteLock(String lockName) throws MusicLockingException;

}
