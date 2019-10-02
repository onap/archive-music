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
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;

import org.onap.music.datastore.Condition;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.datastore.jsonobjects.JsonDelete;
import org.onap.music.datastore.jsonobjects.JsonIndex;
import org.onap.music.datastore.jsonobjects.JsonInsert;
import org.onap.music.datastore.jsonobjects.JsonKeySpace;
import org.onap.music.datastore.jsonobjects.JsonSelect;
import org.onap.music.datastore.jsonobjects.JsonTable;
import org.onap.music.datastore.jsonobjects.JsonUpdate;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.lockingservice.cassandra.CassaLockStore;
import org.onap.music.lockingservice.cassandra.LockType;
import org.onap.music.lockingservice.cassandra.MusicLockState;
import org.onap.music.service.MusicCoreService;

import com.datastax.driver.core.ResultSet;

public class MusicCore {

    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicCore.class);
    private static MusicCoreService musicCore = MusicUtil.getMusicCoreService();
    private static CassaLockStore mLockHandle;
    
    public static CassaLockStore getmLockHandle() {
        return mLockHandle;
    }

    public static void setmLockHandle(CassaLockStore mLockHandleIn) {
        mLockHandle = mLockHandleIn;
    }

    /**
     * Acquire lock
     * 
     * @param fullyQualifiedKey DO NOT RELY ON THIS KEY WORKING. INCLUDE THE KEY IN THE LOCKID.
     * @param lockId - the full lock id (key + lockRef)
     * @return
     * @throws MusicLockingException
     * @throws MusicQueryException
     * @throws MusicServiceException
     */
    public static ReturnType acquireLock(String fullyQualifiedKey, String lockId)
            throws MusicLockingException, MusicQueryException, MusicServiceException {
        return musicCore.acquireLock(fullyQualifiedKey, lockId);
    }

    public static ReturnType acquireLockWithLease(String key, String lockId, long leasePeriod)
            throws MusicLockingException, MusicQueryException, MusicServiceException {
        return musicCore.acquireLockWithLease(key, lockId, leasePeriod);
    }

    public static String createLockReferenceAtomic(String fullyQualifiedKey) throws MusicLockingException {
        return musicCore.createLockReferenceAtomic(fullyQualifiedKey);
    }

    public static String createLockReferenceAtomic(String fullyQualifiedKey, LockType locktype) throws MusicLockingException {
        return musicCore.createLockReferenceAtomic(fullyQualifiedKey, locktype);
    }

    public static String createLockReference(String fullyQualifiedKey, LockType locktype, String owner) throws MusicLockingException {
        return musicCore.createLockReference(fullyQualifiedKey, locktype, owner);
    }

    public static ResultType createTable(String keyspace, String table, PreparedQueryObject tableQueryObject,
            String consistency) throws MusicServiceException {
        return musicCore.createTable(keyspace, table, tableQueryObject, consistency);
    }

    public static ResultSet quorumGet(PreparedQueryObject query) {
        return musicCore.quorumGet(query);
    }

    /**
     * Gets the top of queue for fullyQualifiedKey
     * 
     * @param fullyQualifiedKey
     * @return
     */
    public static String whoseTurnIsIt(String fullyQualifiedKey) {
        return musicCore.whoseTurnIsIt(fullyQualifiedKey);
    }

    /**
     * Gets the current lockholder(s) for fullyQualifiedKey
     * 
     * @param fullyQualifiedKey
     * @return
     */
    public static List<String> getCurrentLockHolders(String fullyQualifiedKey) {
        return musicCore.getCurrentLockHolders(fullyQualifiedKey);
    }
    
    public static ReturnType promoteLock(String lockIdToPromote) throws MusicLockingException {
        return musicCore.promoteLock(lockIdToPromote);
    }

    public static void destroyLockRef(String lockId) throws MusicLockingException {
        musicCore.destroyLockRef(lockId);
    }

    public static ReturnType eventualPut(PreparedQueryObject queryObject) {
        return musicCore.eventualPut(queryObject);
    }

    public static ReturnType eventualPut_nb(PreparedQueryObject queryObject, String keyspace, String tablename,
            String primaryKey) {
        return musicCore.eventualPut_nb(queryObject, keyspace, tablename, primaryKey);
    }

    public static ReturnType criticalPut(String keyspace, String table, String primaryKeyValue,
            PreparedQueryObject queryObject, String lockReference, Condition conditionInfo) {
        return musicCore.criticalPut(keyspace, table, primaryKeyValue, queryObject, lockReference, conditionInfo);
    }

    public static ResultType nonKeyRelatedPut(PreparedQueryObject queryObject, String consistency)
            throws MusicServiceException,MusicQueryException {
        return musicCore.nonKeyRelatedPut(queryObject, consistency);
    }

    public static ResultSet get(PreparedQueryObject queryObject) throws MusicServiceException {
        return musicCore.get(queryObject);
    }

    public static ResultSet criticalGet(String keyspace, String table, String primaryKeyValue,
            PreparedQueryObject queryObject, String lockReference) throws MusicServiceException {
        return musicCore.criticalGet(keyspace, table, primaryKeyValue, queryObject, lockReference);
    }

    public static ReturnType atomicPut(String keyspaceName, String tableName, String primaryKey,
            PreparedQueryObject queryObject, Condition conditionInfo)
            throws MusicLockingException, MusicQueryException, MusicServiceException {
        return musicCore.atomicPut(keyspaceName, tableName, primaryKey, queryObject, conditionInfo);
    }

    public static ResultSet atomicGet(String keyspaceName, String tableName, String primaryKey,
            PreparedQueryObject queryObject) throws MusicServiceException, MusicLockingException, MusicQueryException {
        return musicCore.atomicGet(keyspaceName, tableName, primaryKey, queryObject);
    }

    public static List<String> getLockQueue(String fullyQualifiedKey)
            throws MusicServiceException, MusicQueryException, MusicLockingException {
        return musicCore.getLockQueue(fullyQualifiedKey);
    }

    public static long getLockQueueSize(String fullyQualifiedKey)
            throws MusicServiceException, MusicQueryException, MusicLockingException {
        return musicCore.getLockQueueSize(fullyQualifiedKey);
    }

    public static void deleteLock(String lockName) throws MusicLockingException {
        musicCore.deleteLock(lockName);
    }

    public static ReturnType atomicPutWithDeleteLock(String keyspaceName, String tableName, String primaryKey,
            PreparedQueryObject queryObject, Condition conditionInfo) throws MusicLockingException {
        return musicCore.atomicPutWithDeleteLock(keyspaceName, tableName, primaryKey, queryObject, conditionInfo);
    }

    public static ResultSet atomicGetWithDeleteLock(String keyspaceName, String tableName, String primaryKey,
            PreparedQueryObject queryObject) throws MusicServiceException, MusicLockingException {
        return musicCore.atomicGetWithDeleteLock(keyspaceName, tableName, primaryKey, queryObject);
    }

    public static Map<String, Object> validateLock(String lockName) {
        return musicCore.validateLock(lockName);
    }

    public static MusicLockState releaseLock(String lockId, boolean voluntaryRelease) throws MusicLockingException {
        return musicCore.releaseLock(lockId, voluntaryRelease);
    }
    
    public static List<String> releaseAllLocksForOwner(String ownerId, String keyspace, String table) throws MusicLockingException, MusicServiceException, MusicQueryException {
        return musicCore.releaseAllLocksForOwner(ownerId, keyspace, table);
	}

    //Added changes for orm implementation.
    
    public static ResultType createKeyspace(JsonKeySpace jsonKeySpaceObject, String consistencyInfo) 
            throws MusicServiceException, MusicQueryException {
        return musicCore.createKeyspace(jsonKeySpaceObject,consistencyInfo);
    }
    
    public static ResultType dropKeyspace(JsonKeySpace josnKeyspaceObject, String consistencyInfo)
            throws MusicServiceException,MusicQueryException {
        return musicCore.dropKeyspace(josnKeyspaceObject, consistencyInfo);
    }
    
    public static ResultType createTable(JsonTable jsonTableObject,String consistencyInfo) 
            throws MusicServiceException,MusicQueryException {
        return musicCore.createTable(jsonTableObject, consistencyInfo);
    }
    
    public static ResultType dropTable(JsonTable jsonTableObject, String consistencyInfo) 
            throws MusicServiceException, MusicQueryException {
        return musicCore.dropTable(jsonTableObject, consistencyInfo);
    }
    
    public static ResultType createIndex(JsonIndex jsonIndexObject, String consistencyInfo) 
            throws MusicServiceException,MusicQueryException {
        return musicCore.createIndex(jsonIndexObject, consistencyInfo);
    }
    
    public static ResultSet select(JsonSelect jsonSelect, MultivaluedMap<String, String> rowParams) 
            throws MusicServiceException, MusicQueryException{
        return musicCore.select(jsonSelect, rowParams);
    }
    
    public static ResultSet selectCritical(JsonInsert jsonInsertObj, MultivaluedMap<String, String> rowParams) 
            throws MusicLockingException, MusicQueryException, MusicServiceException{
        return musicCore.selectCritical(jsonInsertObj, rowParams);
    }
    
    
    public static ReturnType insertIntoTable(JsonInsert jsonInsert) throws MusicLockingException, MusicQueryException, MusicServiceException{
        return musicCore.insertIntoTable(jsonInsert);
    }
    
    public static ReturnType updateTable(JsonUpdate jsonUpdateObj,MultivaluedMap<String, String> rowParams) 
            throws MusicLockingException, MusicQueryException, MusicServiceException{
        return musicCore.updateTable(jsonUpdateObj, rowParams);
    }
    
    public static ReturnType deleteFromTable(JsonDelete jsonDeleteObj,MultivaluedMap<String, String> rowParams) 
            throws MusicLockingException, MusicQueryException, MusicServiceException{
        return musicCore.deleteFromTable(jsonDeleteObj,rowParams);
    }

}
