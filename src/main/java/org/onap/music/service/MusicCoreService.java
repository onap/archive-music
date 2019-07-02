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
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.lockingservice.cassandra.LockType;
import org.onap.music.lockingservice.cassandra.MusicLockState;
import org.onap.music.main.ResultType;
import org.onap.music.main.ReturnType;

import com.datastax.driver.core.ResultSet;

public interface MusicCoreService {

    
    // Core Music Database Methods
    

    public ReturnType eventualPut(PreparedQueryObject queryObject);
    
    public  ReturnType eventualPut_nb(PreparedQueryObject queryObject,String keyspace,String tablename,String primaryKey);

    public ReturnType criticalPut(String keyspaceName, String tableName, String primaryKey,
        PreparedQueryObject queryObject, String lockId, Condition conditionInfo);

    public ResultType nonKeyRelatedPut(PreparedQueryObject queryObject, String consistency)
        throws MusicServiceException,MusicQueryException;

    public ResultSet get(PreparedQueryObject queryObject) throws MusicServiceException;

    public ResultSet atomicGet(String keyspaceName, String tableName, String primaryKey,
        PreparedQueryObject queryObject) throws MusicServiceException, MusicLockingException, MusicQueryException;

    public ReturnType atomicPutWithDeleteLock(String keyspaceName, String tableName, String primaryKey,
        PreparedQueryObject queryObject, Condition conditionInfo) throws MusicLockingException;
    
    public  ResultSet atomicGetWithDeleteLock(String keyspaceName, String tableName, String primaryKey,
        PreparedQueryObject queryObject) throws MusicServiceException, MusicLockingException;

    public ReturnType atomicPut(String keyspaceName, String tableName, String primaryKey,
        PreparedQueryObject queryObject, Condition conditionInfo)
        throws MusicLockingException, MusicQueryException, MusicServiceException;

    public ResultSet criticalGet(String keyspaceName, String tableName, String primaryKey,
        PreparedQueryObject queryObject, String lockId) throws MusicServiceException;

    // Core Music Locking Service Methods

    /**
     * Create a lock ref in the music lock store.
     * Default is write as this is the safest semantically
     * 
     * @param fullyQualifiedKey the key to create a lock on
     * @see {@link #creatLockReference(String, LockType)}
     */
    public String createLockReference(String fullyQualifiedKey) throws MusicLockingException; // lock name

    /**
     * Create a lock ref in the music lock store
     * @param fullyQualifiedKey the key to create a lock on
     * @param locktype the type of lock create, see {@link LockType}
     */
    public String createLockReference(String fullyQualifiedKey, LockType locktype) throws MusicLockingException;
    
    public ReturnType acquireLockWithLease(String key, String lockReference, long leasePeriod)
        throws MusicLockingException, MusicQueryException, MusicServiceException; // key,lock id,time

    public ReturnType acquireLock(String key, String lockReference)
        throws MusicLockingException, MusicQueryException, MusicServiceException; // key,lock id

    public ResultType createTable(String keyspace, String table, PreparedQueryObject tableQueryObject,
        String consistency) throws MusicServiceException;

    public ResultSet quorumGet(PreparedQueryObject query);

    /**
     * Gets top of queue for fullyQualifiedKey
     * @param fullyQualifiedKey
     * @return
     */
    public String whoseTurnIsIt(String fullyQualifiedKey);// lock name
    
    /**
     * Gets the current lockholder(s) for lockName
     * @param lockName
     * @return
     */
    public List<String> getCurrentLockHolders(String fullyQualifiedKey);

    public void destroyLockRef(String lockId) throws MusicLockingException;
    
    //public MusicLockState destroyLockRef(String fullyQualifiedKey, String lockReference); // lock name, lock id

    //public MusicLockState voluntaryReleaseLock(String fullyQualifiedKey, String lockReference)
    //        throws MusicLockingException;// lock name,lock id

    public void deleteLock(String lockName) throws MusicLockingException;
    
    //public MusicLockState  forciblyReleaseLock(String fullyQualifiedKey, String lockReference) throws MusicLockingException, MusicServiceException, MusicQueryException;

    public List<String> getLockQueue(String fullyQualifiedKey)
        throws MusicServiceException, MusicQueryException, MusicLockingException;
    
    public long getLockQueueSize(String fullyQualifiedKey)
        throws MusicServiceException, MusicQueryException, MusicLockingException;

    public Map<String, Object> validateLock(String lockName);

    public MusicLockState releaseLock(String lockId, boolean voluntaryRelease) throws MusicLockingException;
    
    
    //Methods added for orm
    

    public ResultType createTable(JsonTable jsonTableObject, String consistencyInfo) throws MusicServiceException,MusicQueryException;
    
    public ResultType dropTable(JsonTable jsonTableObject, String consistencyInfo) 
            throws MusicServiceException,MusicQueryException;
    
    public ResultType createKeyspace(JsonKeySpace jsonKeySpaceObject,String consistencyInfo) throws MusicServiceException,MusicQueryException;
    
    public ResultType dropKeyspace(JsonKeySpace jsonKeySpaceObject, String consistencyInfo) 
            throws MusicServiceException,MusicQueryException;
    
    public ResultType createIndex(JsonIndex jsonIndexObject, String consistencyInfo) throws MusicServiceException,MusicQueryException;
    
    public ResultSet select(JsonSelect jsonSelect, MultivaluedMap<String, String> rowParams) throws MusicServiceException, MusicQueryException;
    
    public ResultSet selectCritical(JsonInsert jsonInsertObj, MultivaluedMap<String, String> rowParams) 
            throws MusicLockingException, MusicQueryException, MusicServiceException;
    
    public ReturnType insertIntoTable(JsonInsert jsonInsert) throws MusicLockingException, MusicQueryException, MusicServiceException;
    
    public ReturnType updateTable(JsonUpdate jsonUpdateObj,MultivaluedMap<String, String> rowParams) 
            throws MusicLockingException, MusicQueryException, MusicServiceException;
    
    public ReturnType deleteFromTable(JsonDelete jsonDeleteObj,MultivaluedMap<String, String> rowParams) 
            throws MusicLockingException, MusicQueryException, MusicServiceException;

}
