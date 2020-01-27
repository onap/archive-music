/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 *  Modifications Copyright (c) 2018 IBM. 
 * ===================================================================
 *  Modifications Copyright (c) 2019 Samsung
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

package org.onap.music.service.impl;

import java.io.StringWriter;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import javax.ws.rs.core.MultivaluedMap;

import org.onap.music.datastore.Condition;
import org.onap.music.datastore.MusicDataStore;
import org.onap.music.datastore.MusicDataStoreHandle;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.datastore.jsonobjects.JsonDelete;
import org.onap.music.datastore.jsonobjects.JsonIndex;
import org.onap.music.datastore.jsonobjects.JsonInsert;
import org.onap.music.datastore.jsonobjects.JsonKeySpace;
import org.onap.music.datastore.jsonobjects.JsonSelect;
import org.onap.music.datastore.jsonobjects.JsonTable;
import org.onap.music.datastore.jsonobjects.JsonUpdate;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.exceptions.MusicDeadlockException;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.lockingservice.cassandra.CassaLockStore;
import org.onap.music.lockingservice.cassandra.CassaLockStore.LockObject;
import org.onap.music.lockingservice.cassandra.LockType;
import org.onap.music.lockingservice.cassandra.MusicLockState;
import org.onap.music.lockingservice.cassandra.MusicLockState.LockStatus;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.main.ReturnType;
import org.onap.music.service.MusicCoreService;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;

public class MusicCassaCore implements MusicCoreService {

    private static CassaLockStore mLockHandle = null;
    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicCassaCore.class);
    private static MusicCassaCore musicCassaCoreInstance = null;
    private static Set<String> set = Collections.synchronizedSet(new HashSet<String>());

    private MusicCassaCore() {
        // not going to happen
    }
    
    public static CassaLockStore getmLockHandle() {
        return mLockHandle;
    }

    //for unit testing purposes
    static void setmLockHandle(CassaLockStore mLockHandle) {
        MusicCassaCore.mLockHandle = mLockHandle;
    }
    
    public static MusicCassaCore getInstance() {

        if(musicCassaCoreInstance == null) {
            musicCassaCoreInstance = new MusicCassaCore();
        }
        return musicCassaCoreInstance;
    }

    public static CassaLockStore getLockingServiceHandle() throws MusicLockingException {
        logger.info(EELFLoggerDelegate.applicationLogger,"Acquiring lock store handle");
        long start = System.currentTimeMillis();

        if (mLockHandle == null) {
            try {
                mLockHandle = new CassaLockStore(MusicDataStoreHandle.getDSHandle());
            } catch (Exception e) {
                logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), AppMessages.LOCKHANDLE,ErrorSeverity.CRITICAL, ErrorTypes.LOCKINGERROR);
                throw new MusicLockingException("Failed to aquire Locl store handle " + e);
            }
        }
        long end = System.currentTimeMillis();
        logger.info(EELFLoggerDelegate.applicationLogger,"Time taken to acquire lock store handle:" + (end - start) + " ms");
        return mLockHandle;
    }

    public String createLockReferenceAtomic(String fullyQualifiedKey) throws MusicLockingException {
        return createLockReferenceAtomic(fullyQualifiedKey, LockType.WRITE);
    }
    public String createLockReference(String fullyQualifiedKey, String owner) throws MusicLockingException {
        return createLockReference(fullyQualifiedKey, LockType.WRITE, owner);
    }

    /**
     * This will be called for Atomic calls
     * 
     */
    public String createLockReferenceAtomic(String fullyQualifiedKey, LockType locktype) throws MusicLockingException {
        String[] splitString = fullyQualifiedKey.split("\\.");
        if (splitString.length < 3) {
            throw new MusicLockingException("Missing or incorrect lock details. Check table or key name.");
        }
        String keyspace = splitString[0];
        String table = splitString[1];
        String lockName = splitString[2];

        logger.info(EELFLoggerDelegate.applicationLogger,"Creating lock reference for lock name:" + lockName);
        long start = 0L;
        long end = 0L;
        String lockReference = null;
        LockObject peek = null;

        /** Lets check for an existing lock. 
         * This will allow us to limit the amount of requests going forward.
         */
        start = System.currentTimeMillis();
        try {
            peek = getLockingServiceHandle().peekLockQueue(keyspace, table, lockName);
        } catch (MusicServiceException | MusicQueryException e) {
            //logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(),e);
            throw new MusicLockingException("Error getting lockholder info for key [" + lockName +"]:" + e.getMessage());
        }
        
        if(peek!=null && (peek.getLocktype()!=null && peek.getLocktype().equals(LockType.WRITE)) && peek.getAcquireTime()!=null && peek.getLockRef()!=null) {
            long currentTime = System.currentTimeMillis();
            if((currentTime-Long.parseLong(peek.getAcquireTime()))<MusicUtil.getDefaultLockLeasePeriod()){
                //logger.info(EELFLoggerDelegate.applicationLogger,"Lock holder exists and lease not expired. Please try again for key="+lockName);
                throw new MusicLockingException("Unable to create lock reference for key [" + lockName + "]. Please try again.");
            }
        }
        end = System.currentTimeMillis();
        logger.info(EELFLoggerDelegate.applicationLogger,"Time taken to check for lock reference for key [" + lockName + "]:" + (end - start) + " ms");

        start = System.currentTimeMillis();
        /* We are Creating a Thread safe set and adding the key to the set. 
        * if a key exists then it wil be passed over and not go to the lock creation. 
        * If a key doesn't exist then it will set the value in the set and continue to create a lock. 
        *
        * This will ensure that no 2 threads using the same key will be able to try to create a lock
        * This wil in turn squash the amout of LWT Chatter in Cassandra an reduce the amount of
        * WriteTimeoutExceptions being experiences on single keys.
        */
        if ( set.add(fullyQualifiedKey)) {
            try {
                lockReference = "" + getLockingServiceHandle().genLockRefandEnQueue(keyspace, table, lockName, locktype,null);
                set.remove(fullyQualifiedKey);
            } catch (MusicLockingException | MusicServiceException | MusicQueryException e) {
                set.remove(fullyQualifiedKey);
                throw new MusicLockingException(e.getMessage());
            } catch (Exception e) {
                set.remove(fullyQualifiedKey);
                e.printStackTrace();
                logger.error(EELFLoggerDelegate.applicationLogger,"Exception in creatLockEnforced:"+ e.getMessage(),e);
                throw new MusicLockingException("Unable to create lock reference for key [" + lockName + "]. " + e.getMessage());
            }
        } else {
            throw new MusicLockingException("Unable to create lock reference for key [" + lockName + "]. Please try again.");
        }
        end = System.currentTimeMillis();
        logger.info(EELFLoggerDelegate.debugLogger,"### Set = " + set);
        logger.info(EELFLoggerDelegate.applicationLogger,"Time taken to create lock reference  for key [" + lockName + "]:" + (end - start) + " ms");
        return lockReference;

        //return createLockReference(fullyQualifiedKey, locktype, null);
    }

    public String createLockReference(String fullyQualifiedKey, LockType locktype, String owner) throws MusicLockingException {
        String[] splitString = fullyQualifiedKey.split("\\.");
        if (splitString.length < 3) {
            throw new MusicLockingException("Missing or incorrect lock details. Check table or key name.");
        }
        String keyspace = splitString[0];
        String table = splitString[1];
        String lockName = splitString[2];

        logger.info(EELFLoggerDelegate.applicationLogger,"Creating lock reference for lock name:" + lockName);
        long start = 0L;
        long end = 0L;
        String lockReference = null;

        /* Check for a Deadlock */
        try {
            boolean deadlock = getLockingServiceHandle().checkForDeadlock(keyspace, table, lockName, locktype, owner, false);
            if (deadlock) {
                MusicDeadlockException e = new MusicDeadlockException("Deadlock detected when " + owner + " tried to create lock on " + keyspace + "." + table + "." + lockName);
                e.setValues(owner, keyspace, table, lockName);
                throw e;
            }
        } catch (MusicDeadlockException e) {
            //just threw this, no need to wrap it
            throw e;
        } catch (MusicServiceException | MusicQueryException e) {
            logger.error(EELFLoggerDelegate.applicationLogger, e);
            throw new MusicLockingException("Unable to check for deadlock. " + e.getMessage(), e);
        }
        end = System.currentTimeMillis();
        logger.info(EELFLoggerDelegate.applicationLogger,"Time taken to check for deadlock for key [" + lockName + "]:" + (end - start) + " ms");

        start = System.currentTimeMillis();
        try {
            lockReference = "" + getLockingServiceHandle().genLockRefandEnQueue(keyspace, table, lockName, locktype, owner);
        } catch (MusicLockingException | MusicServiceException | MusicQueryException e) {
            logger.info(EELFLoggerDelegate.applicationLogger,e.getMessage(),e);
            throw new MusicLockingException("Unable to create lock reference for key [" + lockName + "]. Please try again: " + e.getMessage());
        } catch (Exception e) {
            logger.error(EELFLoggerDelegate.applicationLogger,e.getMessage(),e);
            throw new MusicLockingException("Unable to create lock reference. " + e.getMessage(), e);
        }
        end = System.currentTimeMillis();
        logger.info(EELFLoggerDelegate.applicationLogger,"Time taken to create lock reference  for key [" + lockName + "]:" + (end - start) + " ms");
        return lockReference;
    }
    
    public ReturnType promoteLock(String lockId) throws MusicLockingException {
        String[] splitString = lockId.split("\\.");
        String keyspace = splitString[0].substring(1);//remove '$'
        String table = splitString[1];
        String primaryKeyValue = splitString[2].substring(0, splitString[2].lastIndexOf("$"));
        String lockRef = lockId.substring(lockId.lastIndexOf("$")+1); //lockRef is "$" to end
        
        logger.info(EELFLoggerDelegate.applicationLogger,"Attempting to promote lock " + lockId);

        try {
            return getLockingServiceHandle().promoteLock(keyspace, table, primaryKeyValue, lockRef);
        } catch (MusicServiceException e) {
            throw new MusicLockingException("Unable to promote lock. ", e);
        } catch (MusicQueryException e) {
            throw new MusicLockingException("Unable to promote lock. ", e);
        }
        
    }


    public ReturnType acquireLockWithLease(String fullyQualifiedKey, String lockReference, long leasePeriod)
            throws MusicLockingException, MusicQueryException, MusicServiceException  {
        evictExpiredLockHolder(fullyQualifiedKey,leasePeriod);
        return acquireLock(fullyQualifiedKey, lockReference);
    }

    private void evictExpiredLockHolder(String fullyQualifiedKey, long leasePeriod)
            throws MusicLockingException, MusicQueryException, MusicServiceException {
        String[] splitString = fullyQualifiedKey.split("\\.");
        String keyspace = splitString[0];
        String table = splitString[1];
        String primaryKeyValue = splitString[2];

        LockObject currentLockHolderObject = getLockingServiceHandle().peekLockQueue(keyspace, table, primaryKeyValue);

        if (!currentLockHolderObject.getIsLockOwner()) { // no lock holder
            return;
        }
        /*
         * Release the lock of the previous holder if it has expired. if the update to the acquire time has
         * not reached due to network delays, simply use the create time as the reference
         */
        long referenceTime = Math.max(Long.parseLong(currentLockHolderObject.getAcquireTime()),
                Long.parseLong(currentLockHolderObject.getCreateTime()));
        if ((System.currentTimeMillis() - referenceTime) > leasePeriod) {
            forciblyReleaseLock(fullyQualifiedKey, currentLockHolderObject.getLockRef() + "");
            logger.info(EELFLoggerDelegate.applicationLogger, currentLockHolderObject.getLockRef() + " forcibly released");
        }
    }

    public ReturnType acquireLock(String fullyQualifiedKey, String lockId)
            throws MusicLockingException, MusicQueryException, MusicServiceException {
        String[] splitString = lockId.split("\\.");
        String keyspace = splitString[0].substring(1);//remove '$'
        String table = splitString[1];
        String primaryKeyValue = splitString[2].substring(0, splitString[2].lastIndexOf("$"));
        String localFullyQualifiedKey = lockId.substring(1, lockId.lastIndexOf("$"));
        String lockRef = lockId.substring(lockId.lastIndexOf("$")+1); //lockRef is "$" to end

        LockObject lockInfo = getLockingServiceHandle().getLockInfo(keyspace, table, primaryKeyValue, lockRef);

        if (!lockInfo.getIsLockOwner()) {
            return new ReturnType(ResultType.FAILURE, lockId + " is not a lock holder");//not top of the lock store q
        }
        
        if (getLockingServiceHandle().checkForDeadlock(keyspace, table, primaryKeyValue, lockInfo.getLocktype(), lockInfo.getOwner(), true)) {
            MusicDeadlockException e = new MusicDeadlockException("Deadlock detected when " + lockInfo.getOwner()  + " tried to create lock on " + keyspace + "." + table + "." + primaryKeyValue);
            e.setValues(lockInfo.getOwner(), keyspace, table, primaryKeyValue);
            throw e;
        }

        //check to see if the value of the key has to be synced in case there was a forceful release
        String syncTable = keyspace+".unsyncedKeys_"+table;
        String query = "select * from "+syncTable+" where key='"+localFullyQualifiedKey+"';";
        PreparedQueryObject readQueryObject = new PreparedQueryObject();
        readQueryObject.appendQueryString(query);
        ResultSet results = MusicDataStoreHandle.getDSHandle().executeQuorumConsistencyGet(readQueryObject);
        if (!results.all().isEmpty()) {
            logger.info("In acquire lock: Since there was a forcible release, need to sync quorum!");
            try {
                syncQuorum(keyspace, table, primaryKeyValue);
            } catch (Exception e) {
                StringWriter sw = new StringWriter();
                    logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), "[ERR506E] Failed to aquire lock ",
                        ErrorSeverity.CRITICAL, ErrorTypes.LOCKINGERROR, e);
                String exceptionAsString = sw.toString();
                return new ReturnType(ResultType.FAILURE, "Exception thrown while syncing key:\n" + exceptionAsString);
            }
            String cleanQuery = "delete from " + syncTable + " where key='"+localFullyQualifiedKey+"';";
            PreparedQueryObject deleteQueryObject = new PreparedQueryObject();
            deleteQueryObject.appendQueryString(cleanQuery);
            MusicDataStoreHandle.getDSHandle().executePut(deleteQueryObject, "critical");
        }

        getLockingServiceHandle().updateLockAcquireTime(keyspace, table, primaryKeyValue, lockRef);

        return new ReturnType(ResultType.SUCCESS, lockRef+" is the lock holder for the key");
    }



    /**
     *
     * @param tableQueryObject
     * @param consistency
     * @return Boolean Indicates success or failure
     * @throws MusicServiceException
     *
     */
    public ResultType createTable(String keyspace, String table, PreparedQueryObject tableQueryObject,
            String consistency) throws MusicServiceException {
        boolean result = false;

        try {
            // create shadow locking table
            result = getLockingServiceHandle().createLockQueue(keyspace, table);
            if (result == false) {
                return ResultType.FAILURE;
            }
            result = false;

            // create table to track unsynced_keys
            table = "unsyncedKeys_" + table;

            String tabQuery =
                    "CREATE TABLE IF NOT EXISTS " + keyspace + "." + table + " ( key text,PRIMARY KEY (key) );";
            PreparedQueryObject queryObject = new PreparedQueryObject();

            queryObject.appendQueryString(tabQuery);
            result = MusicDataStoreHandle.getDSHandle().executePut(queryObject, "eventual");
            if (result == false) {
                return ResultType.FAILURE;
            }
            result = false;

            // create actual table
            result = MusicDataStoreHandle.getDSHandle().executePut(tableQueryObject, consistency);
        } catch (MusicQueryException | MusicServiceException | MusicLockingException ex) {
            logger.error(EELFLoggerDelegate.errorLogger, ex.getMessage(), AppMessages.UNKNOWNERROR, ErrorSeverity.WARN,
                    ErrorTypes.MUSICSERVICEERROR);
            throw new MusicServiceException(ex.getMessage());
        }
        return result ? ResultType.SUCCESS : ResultType.FAILURE;
    }

    private static void syncQuorum(String keyspace, String table, String primaryKeyValue) throws Exception {
        logger.info(EELFLoggerDelegate.applicationLogger,"Performing sync operation---");
        PreparedQueryObject selectQuery = new PreparedQueryObject();
        PreparedQueryObject updateQuery = new PreparedQueryObject();

        // get the primary key d
        TableMetadata tableInfo = MusicDataStoreHandle.returnColumnMetadata(keyspace, table);
        String primaryKeyName = tableInfo.getPrimaryKey().get(0).getName(); // we only support single
                                                                            // primary key
        DataType primaryKeyType = tableInfo.getPrimaryKey().get(0).getType();
        Object cqlFormattedPrimaryKeyValue =
                        MusicUtil.convertToActualDataType(primaryKeyType, primaryKeyValue);

        // get the row of data from a quorum
        selectQuery.appendQueryString("SELECT *  FROM " + keyspace + "." + table + " WHERE "
                        + primaryKeyName + "= ?" + ";");
        selectQuery.addValue(cqlFormattedPrimaryKeyValue);
        MusicUtil.writeBackToQuorum(selectQuery, primaryKeyName, updateQuery, keyspace, table,
            cqlFormattedPrimaryKeyValue);
    }

    /**
     *
     * @param query
     * @return ResultSet
     */
    public ResultSet quorumGet(PreparedQueryObject query) {
        ResultSet results = null;
        try {
            results = MusicDataStoreHandle.getDSHandle().executeQuorumConsistencyGet(query);
        } catch (MusicServiceException | MusicQueryException e) {
            logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), AppMessages.UNKNOWNERROR,
                ErrorSeverity.MAJOR, ErrorTypes.GENERALSERVICEERROR, e);

        }
        return results;
    }

    public String whoseTurnIsIt(String fullyQualifiedKey) {
        String[] splitString = fullyQualifiedKey.split("\\.");
        String keyspace = splitString[0];
        String table = splitString[1];
        String primaryKeyValue = splitString[2];
        try {
            LockObject lockOwner = getLockingServiceHandle().peekLockQueue(keyspace, table, primaryKeyValue);
            if (!lockOwner.getIsLockOwner()) {
                return null;
            }
            return "$" + fullyQualifiedKey + "$" + lockOwner.getLockRef();
        } catch (MusicLockingException | MusicServiceException | MusicQueryException e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.LOCKINGERROR + fullyQualifiedKey,
                    ErrorSeverity.CRITICAL, ErrorTypes.LOCKINGERROR);
        }
        return null;
    }
    
    public List<String> getCurrentLockHolders(String fullyQualifiedKey) {
        String[] splitString = fullyQualifiedKey.split("\\.");
        String keyspace = splitString[0];
        String table = splitString[1];
        String primaryKeyValue = splitString[2];
        try {
            return getLockingServiceHandle().getCurrentLockHolders(keyspace, table, primaryKeyValue);
        } catch (MusicLockingException | MusicServiceException | MusicQueryException e) {
            logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), AppMessages.LOCKINGERROR+fullyQualifiedKey ,ErrorSeverity.CRITICAL, ErrorTypes.LOCKINGERROR);
        }
        return null;
    }

    /**
     *
     * @param lockReference
     * @return
     */
    public static String getLockNameFromId(String lockReference) {
        StringTokenizer st = new StringTokenizer(lockReference);
        return st.nextToken("$");
    }

    @Override
    public void destroyLockRef(String lockId) throws MusicLockingException {
        long start = System.currentTimeMillis();
        String fullyQualifiedKey = lockId.substring(1, lockId.lastIndexOf("$"));
        String lockRef = lockId.substring(lockId.lastIndexOf('$')+1);
        String[] splitString = fullyQualifiedKey.split("\\.");
        String keyspace = splitString[0];
        String table = splitString[1];
        String primaryKeyValue = splitString[2];
        try {
            getLockingServiceHandle().deQueueLockRef(keyspace, table, primaryKeyValue, lockRef,MusicUtil.getRetryCount());
        } catch (MusicLockingException | MusicServiceException | MusicQueryException e) {
            logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), AppMessages.DESTROYLOCK+lockRef,
                ErrorSeverity.CRITICAL, ErrorTypes.LOCKINGERROR, e);
            throw new MusicLockingException(e.getMessage());
        }
        long end = System.currentTimeMillis();
        logger.info(EELFLoggerDelegate.applicationLogger,"Time taken to destroy lock reference:" + (end - start) + " ms");
    }

    public MusicLockState destroyLockRef(String fullyQualifiedKey, String lockReference) throws MusicLockingException {
        long start = System.currentTimeMillis();
        String[] splitString = fullyQualifiedKey.split("\\.");
        String keyspace = splitString[0];
        String table = splitString[1];
        String primaryKeyValue = splitString[2];
        try {
            getLockingServiceHandle().deQueueLockRef(keyspace, table, primaryKeyValue, lockReference,MusicUtil.getRetryCount());
        } catch (MusicLockingException | MusicServiceException | MusicQueryException e) {
            logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), AppMessages.DESTROYLOCK + lockReference,
                ErrorSeverity.CRITICAL, ErrorTypes.LOCKINGERROR,e);
            throw new MusicLockingException(e.getMessage());
        }
        long end = System.currentTimeMillis();
        logger.info(EELFLoggerDelegate.applicationLogger,"Time taken to destroy lock reference:" + (end - start) + " ms");
        return new MusicLockState(LockStatus.UNLOCKED, "");
    }

    @Override
    public MusicLockState releaseLock(String lockId, boolean voluntaryRelease) throws MusicLockingException {
        String fullyQualifiedKey = lockId.substring(1, lockId.lastIndexOf("$"));
        String lockRef = lockId.substring(lockId.lastIndexOf('$')+1);
        if (voluntaryRelease) {
            return voluntaryReleaseLock(fullyQualifiedKey, lockRef);
        } else {
            return forciblyReleaseLock(fullyQualifiedKey, lockRef);
        }
    }

    public MusicLockState voluntaryReleaseLock(String fullyQualifiedKey, String lockReference)
            throws MusicLockingException {
        MusicLockState result = null;
        try {
            result = destroyLockRef(fullyQualifiedKey, lockReference);
        } catch (Exception ex) {
            logger.info(EELFLoggerDelegate.applicationLogger,
                    "Exception in voluntaryReleaseLock() for " + fullyQualifiedKey + "ref: " + lockReference);
            throw new MusicLockingException(ex.getMessage());
        }
        return result;
    }

    public MusicLockState forciblyReleaseLock(String fullyQualifiedKey, String lockReference) throws MusicLockingException {
        String[] splitString = fullyQualifiedKey.split("\\.");
        String keyspace = splitString[0];
        String table = splitString[1];

        //leave a signal that this key could potentially be unsynchronized
        String syncTable = keyspace+".unsyncedKeys_"+table;
        PreparedQueryObject queryObject = new PreparedQueryObject();
        String values = "(?)";
        queryObject.addValue(fullyQualifiedKey);
        String insQuery = "insert into "+syncTable+" (key) values "+values+";";
        queryObject.appendQueryString(insQuery);
        try {
            MusicDataStoreHandle.getDSHandle().executePut(queryObject, "critical");
        } catch (Exception e) {
            logger.error("Cannot forcibly release lock: " + fullyQualifiedKey + " " + lockReference + ". "
                        + e.getMessage(), e);
        }

        //now release the lock
        return destroyLockRef(fullyQualifiedKey, lockReference);
    }

    @Override
    public List<String> releaseAllLocksForOwner(String ownerId, String keyspace, String table) throws MusicLockingException, MusicServiceException, MusicQueryException {
//        System.out.println("IN RELEASEALLLOCKSFOROWNER, ");

        List<String> lockIds = getLockingServiceHandle().getAllLocksForOwner(ownerId, keyspace, table);
        for (String lockId : lockIds) {
//            System.out.println(" LOCKID = " + lockId);
            //return "$" + keyspace + "." + table + "." + lockName + "$" + String.valueOf(lockRef);
            releaseLock("$" + keyspace + "." + table + "." + lockId, true);
        }
        return lockIds;
    }

    /**
     *
     * @param lockName
     * @throws MusicLockingException
     */
    @Deprecated
    public  void deleteLock(String lockName) throws MusicLockingException {
        throw new MusicLockingException("Depreciated Method Delete Lock");
    }

    // Prepared Query Additions.

    /**
     *
     * @param queryObject
     * @return ReturnType
     * @throws MusicServiceException
     */
    public  ReturnType eventualPut(PreparedQueryObject queryObject) {
        boolean result = false;
        try {
            result = MusicDataStoreHandle.getDSHandle().executePut(queryObject, MusicUtil.EVENTUAL);
        } catch (MusicServiceException | MusicQueryException ex) {
            logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage(), "[ERR512E] Failed to get Lock Handle "  ,ErrorSeverity.WARN, ErrorTypes.MUSICSERVICEERROR);
            logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage() + "  " + ex.getCause() + " " + ex);
            return new ReturnType(ResultType.FAILURE, ex.getMessage());
        }
        if (result) {
            return new ReturnType(ResultType.SUCCESS, "Eventual Operation Successfully performed");
        } else {
            return new ReturnType(ResultType.FAILURE, "Eventual Operation failed to perform");
        }
    }

    /**
     *
     * @param queryObject
     * @return ReturnType
     * @throws MusicServiceException
     */
    public  ReturnType eventualPut_nb(PreparedQueryObject queryObject,String keyspace,String tablename,String primaryKey) {
        boolean result = false;
        long guard = 0;
        PreparedQueryObject getGaurd = new PreparedQueryObject();
        getGaurd.appendQueryString("SELECT guard FROM "+keyspace+".lockq_"+tablename+ " WHERE key = ? ;");
        getGaurd.addValue(primaryKey);
        try {
            ResultSet getGaurdResult = MusicDataStoreHandle.getDSHandle().executeQuorumConsistencyGet(getGaurd);
            Row row = getGaurdResult.one();
            if (row != null) {
                guard = row.getLong("guard");
                long timeOfWrite = System.currentTimeMillis();
                long ts = MusicUtil.v2sTimeStampInMicroseconds(guard, timeOfWrite);
                String query = queryObject.getQuery();
                if (!queryObject.getQuery().contains("USING TIMESTAMP")) {
                    if (queryObject.getOperation().equalsIgnoreCase("delete"))
                        query = query.replaceFirst("WHERE", " USING TIMESTAMP " + ts + " WHERE ");
                    else
                        query = query.replaceFirst("SET", "USING TIMESTAMP " + ts + " SET");
                }
                queryObject.replaceQueryString(query);
            }

        } catch (MusicServiceException | MusicQueryException e) {
            logger.error(EELFLoggerDelegate.applicationLogger,e.getMessage(), e);
        }
        try {
            result = MusicDataStoreHandle.getDSHandle().executePut(queryObject, MusicUtil.EVENTUAL);
        } catch (MusicServiceException | MusicQueryException ex) {
            logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage(),"[ERR512E] Failed to get Lock Handle ",
                ErrorSeverity.WARN, ErrorTypes.MUSICSERVICEERROR);
            logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage() + "  " + ex.getCause() + " ", ex);
            return new ReturnType(ResultType.FAILURE, ex.getMessage());
        }
        if (result) {
            return new ReturnType(ResultType.SUCCESS, "Eventual Operation Successfully performed");
        } else {
            return new ReturnType(ResultType.FAILURE, "Eventual Operation failed to perform");
        }
    }

    /**
     *
     * @param keyspace
     * @param table
     * @param primaryKeyValue
     * @param queryObject
     * @param lockId
     * @return
     */
    public ReturnType criticalPut(String keyspace, String table, String primaryKeyValue,
            PreparedQueryObject queryObject, String lockId, Condition conditionInfo) {
        long start = System.currentTimeMillis();
        try {
            String keyLock = lockId.substring(lockId.lastIndexOf(".") + 1,lockId.lastIndexOf("$"));
            if (lockId.contains(".") && !keyLock.equals(primaryKeyValue)) {
                return new ReturnType(ResultType.FAILURE,"Lock value '" + keyLock + "' and key value '"
                + primaryKeyValue + "' not match. Please check your values: " 
                + lockId + " .");
            }
            LockObject lockObject = getLockingServiceHandle().getLockInfo(keyspace, table, primaryKeyValue,
                    lockId.substring(lockId.lastIndexOf("$") + 1));

            if ( lockObject == null ) {
                return new ReturnType(ResultType.FAILURE, lockId + " does not exist.");
            } else if (!lockObject.getIsLockOwner()) {
                return new ReturnType(ResultType.FAILURE, lockId + " is not the lock holder");
            } else if (lockObject.getLocktype() != LockType.WRITE) {
                return new ReturnType(ResultType.FAILURE,
                        "Attempting to do write operation, but " + lockId + " is a read lock");
            }

            if (conditionInfo != null) {
                try {
                    if (conditionInfo.testCondition() == false)
                        return new ReturnType(ResultType.FAILURE, "Lock acquired but the condition is not true");
                } catch (Exception e) {
                    logger.error(EELFLoggerDelegate.errorLogger, e);
                    return new ReturnType(ResultType.FAILURE,
                            "Exception thrown while checking the condition, check its sanctity:\n" + e.getMessage());
                }
            }
            String query = queryObject.getQuery();
            long timeOfWrite = System.currentTimeMillis();
            long lockOrdinal = Long.parseLong(lockId.substring(lockId.lastIndexOf("$") + 1));
            long ts = MusicUtil.v2sTimeStampInMicroseconds(lockOrdinal, timeOfWrite);
            // TODO: use Statement instead of modifying query
            if (!queryObject.getQuery().contains("USING TIMESTAMP")) {
                if (queryObject.getOperation().equalsIgnoreCase("delete"))
                    query = query.replaceFirst("WHERE", " USING TIMESTAMP " + ts + " WHERE ");
                else if (queryObject.getOperation().equalsIgnoreCase("insert"))
                    query = query.replaceFirst(";", " USING TIMESTAMP " + ts + " ; ");
                else
                    query = query.replaceFirst("SET", "USING TIMESTAMP " + ts + " SET");
            }
            queryObject.replaceQueryString(query);
            MusicDataStore dsHandle = MusicDataStoreHandle.getDSHandle();
            dsHandle.executePut(queryObject, MusicUtil.CRITICAL);
            long end = System.currentTimeMillis();
            logger.info(EELFLoggerDelegate.applicationLogger,"Time taken for the critical put:" + (end - start) + " ms");
        } catch (MusicQueryException | MusicServiceException | MusicLockingException  e) {
            logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), e);
            return new ReturnType(ResultType.FAILURE,
                "Exception thrown while doing the critical put: "
                + e.getMessage());
        }
        return new ReturnType(ResultType.SUCCESS, "Update performed");
    }


    /**
     *
     * @param queryObject
     * @param consistency
     * @return Boolean Indicates success or failure
     * @throws MusicServiceException
     *
     *
     */
    public ResultType nonKeyRelatedPut(PreparedQueryObject queryObject, String consistency) throws MusicServiceException,MusicQueryException {
        // this is mainly for some functions like keyspace creation etc which does not
        // really need the bells and whistles of Music locking.
        boolean result = false;
//        try {
        result = MusicDataStoreHandle.getDSHandle().executePut(queryObject, consistency);
//        } catch (MusicQueryException | MusicServiceException ex) {
            // logger.error(EELFLoggerDelegate.errorLogger, ex.getMessage(), AppMessages.UNKNOWNERROR,
            //     ErrorSeverity.WARN, ErrorTypes.MUSICSERVICEERROR, ex);
//            throw new MusicServiceException(ex.getMessage(),ex);
//        }
        return result ? ResultType.SUCCESS : ResultType.FAILURE;
    }

    /**
     * This method performs DDL operation on cassandra.
     *
     * @param queryObject query object containing prepared query and values
     * @return ResultSet
     * @throws MusicServiceException
     */
    public ResultSet get(PreparedQueryObject queryObject) throws MusicServiceException {
        ResultSet results = null;
        try {
            results = MusicDataStoreHandle.getDSHandle().executeOneConsistencyGet(queryObject);
        } catch (MusicQueryException | MusicServiceException e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), e);
            throw new MusicServiceException(e.getMessage());
        }
        return results;
    }

    /**
     * This method performs DDL operations on cassandra, if the the resource is available. Lock ID
     * is used to check if the resource is free.
     *
     * @param keyspace name of the keyspace
     * @param table name of the table
     * @param primaryKeyValue primary key value
     * @param queryObject query object containing prepared query and values
     * @param lockId lock ID to check if the resource is free to perform the operation.
     * @return ResultSet
     */
    public ResultSet criticalGet(String keyspace, String table, String primaryKeyValue,
                    PreparedQueryObject queryObject, String lockId) throws MusicServiceException {
        ResultSet results = null;
        String keyLock = lockId.substring(lockId.lastIndexOf(".") + 1,lockId.lastIndexOf("$"));
        try {
            if (lockId.contains(".") && !keyLock.equals(primaryKeyValue)) {
                throw new MusicLockingException("Lock value '" + keyLock + "' and key value '"
                + primaryKeyValue + "' do not match. Please check your values: " 
                + lockId + " .");
            }
            LockObject lockObject = getLockingServiceHandle().getLockInfo(keyspace, table, primaryKeyValue,
                lockId.substring(lockId.lastIndexOf("$") + 1));
            if (null == lockObject) {
                throw new MusicLockingException("No Lock Object. Please check if lock name or key is correct." 
                    + lockId + " .");
            }
            if ( !lockObject.getIsLockOwner()) {
                return null;// not top of the lock store q
            }
            results = MusicDataStoreHandle.getDSHandle().executeQuorumConsistencyGet(queryObject);
        } catch ( MusicLockingException e ) {
            logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), AppMessages.UNKNOWNERROR  ,ErrorSeverity
                    .WARN, ErrorTypes.MUSICSERVICEERROR);
                throw new MusicServiceException(
                    "Cannot perform critical get for key: " + primaryKeyValue + " : " + e.getMessage());
        } catch (MusicQueryException | MusicServiceException e) {
            logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), AppMessages.UNKNOWNERROR  ,ErrorSeverity
                    .WARN, ErrorTypes.MUSICSERVICEERROR, e);
                throw new MusicServiceException(
                    "Cannot perform critical get for key: " + primaryKeyValue + " : " + e.getMessage());    
        }
        return results;
    }

    /**
     * This method performs DML operation on cassandra, when the lock of the dd is acquired.
     *
     * @param keyspaceName name of the keyspace
     * @param tableName name of the table
     * @param primaryKey primary key value
     * @param queryObject query object containing prepared query and values
     * @return ReturnType
     * @throws MusicLockingException
     * @throws MusicServiceException
     * @throws MusicQueryException
     */
    public ReturnType atomicPut(String keyspaceName, String tableName, String primaryKey,
            PreparedQueryObject queryObject, Condition conditionInfo)
            throws MusicLockingException, MusicQueryException, MusicServiceException {
        long start = System.currentTimeMillis();
        String fullyQualifiedKey = keyspaceName + "." + tableName + "." + primaryKey;
        String lockId = createLockReferenceAtomic(fullyQualifiedKey, LockType.WRITE);
        long lockCreationTime = System.currentTimeMillis();
        ReturnType lockAcqResult = null;
        logger.info(EELFLoggerDelegate.applicationLogger,
                "***Acquiring lock for atomicPut() query : " + queryObject.getQuery() + " : " + primaryKey);
        logger.info(EELFLoggerDelegate.applicationLogger,
                "***Acquiring lock for atomicPut() values: " + queryObject.getValues().toString());
        if (conditionInfo != null) {
            logger.info(EELFLoggerDelegate.applicationLogger,
                    "***Acquiring lock for atomicPut() conditions: " + conditionInfo.toString());
        }
        try {
            lockAcqResult = acquireLockWithLease(fullyQualifiedKey, lockId, MusicUtil.getDefaultLockLeasePeriod());
        } catch (MusicLockingException ex) {
            logger.error(EELFLoggerDelegate.errorLogger,
                    "Exception while acquireLockWithLease() in atomic put for key: " + primaryKey);
            logger.error(EELFLoggerDelegate.errorLogger, ex.getMessage());
            throw new MusicServiceException(
                    "Cannot perform atomic put for key: " + primaryKey + " : " + ex.getMessage());
        }
        long lockAcqTime = System.currentTimeMillis();

        /*
         * if (!lockAcqResult.getResult().equals(ResultType.SUCCESS)) { logger.info(EELFLoggerDelegate.
         * applicationLogger,"unable to acquire lock, id " + lockId);
         * voluntaryReleaseLock(fullyQualifiedKey,lockId); return lockAcqResult; }
         */

        logger.info(EELFLoggerDelegate.applicationLogger, "acquired lock with id " + lockId);
        String lockRef = lockId.substring(lockId.lastIndexOf("$"));
        ReturnType criticalPutResult = null;
        if (lockAcqResult.getResult().equals(ResultType.SUCCESS)) {
            criticalPutResult = criticalPut(keyspaceName, tableName, primaryKey, queryObject, lockRef, conditionInfo);
            long criticalPutTime = System.currentTimeMillis();
            long lockDeleteTime = System.currentTimeMillis();
            String timingInfo = "|lock creation time:" + (lockCreationTime - start) + "|lock accquire time:"
                    + (lockAcqTime - lockCreationTime) + "|critical put time:" + (criticalPutTime - lockAcqTime)
                    + "|lock delete time:" + (lockDeleteTime - criticalPutTime) + "|";
            criticalPutResult.setTimingInfo(timingInfo);
        } else {
            logger.info(EELFLoggerDelegate.applicationLogger, "unable to acquire lock, id " + lockId);
            criticalPutResult = lockAcqResult;
        }
        try {
            voluntaryReleaseLock(fullyQualifiedKey, lockId);
        } catch (MusicLockingException ex) {
            logger.info(EELFLoggerDelegate.applicationLogger,
                    "Exception occured while deleting lock after atomic put for key: " + primaryKey);
            criticalPutResult.setMessage(criticalPutResult.getMessage() + "Lock release failed");
        }
        return criticalPutResult;
    }



    /**
     * This method performs DDL operation on cassasndra, when the lock for the resource is acquired.
     *
     * @param keyspaceName name of the keyspace
     * @param tableName name of the table
     * @param primaryKey primary key value
     * @param queryObject query object containing prepared query and values
     * @return ResultSet
     * @throws MusicServiceException
     * @throws MusicLockingException
     * @throws MusicQueryException
     */
    public ResultSet atomicGet(String keyspaceName, String tableName, String primaryKey,
            PreparedQueryObject queryObject) throws MusicServiceException, MusicLockingException, MusicQueryException {
        String fullyQualifiedKey = keyspaceName + "." + tableName + "." + primaryKey;
        String lockId = createLockReferenceAtomic(fullyQualifiedKey, LockType.READ);
        ReturnType lockAcqResult = null;
        ResultSet result = null;
        logger.info(EELFLoggerDelegate.applicationLogger, "Acquiring lock for atomicGet() : " + queryObject.getQuery());
        try {
            lockAcqResult = acquireLockWithLease(fullyQualifiedKey, lockId, MusicUtil.getDefaultLockLeasePeriod());
        } catch (MusicLockingException ex) {
            logger.error(EELFLoggerDelegate.errorLogger,
                    "Exception while acquireLockWithLease() in atomic get for key: " + primaryKey);
            logger.error(EELFLoggerDelegate.errorLogger, ex.getMessage());
            throw new MusicServiceException(
                    "Cannot perform atomic get for key: " + primaryKey + " : " + ex.getMessage());
        }
        if (lockAcqResult.getResult().equals(ResultType.SUCCESS)) {
            logger.info(EELFLoggerDelegate.applicationLogger, "acquired lock with id " + lockId);
            String lockRef = lockId.substring(lockId.lastIndexOf("$"));
            result = criticalGet(keyspaceName, tableName, primaryKey, queryObject, lockRef);
        } else {
            logger.info(EELFLoggerDelegate.applicationLogger, "unable to acquire lock, id " + lockId);
        }
        try {
            voluntaryReleaseLock(fullyQualifiedKey, lockId);
        } catch (MusicLockingException ex) {
            logger.info(EELFLoggerDelegate.applicationLogger,
                    "Exception occured while deleting lock after atomic put for key: " + primaryKey);
            throw new MusicLockingException(ex.getMessage());
        }

        return result;
    }



    /**
     * @param lockName
     * @return
     */
    public Map<String, Object> validateLock(String lockName) {
        return MusicUtil.validateLock(lockName);
    }

    @Override
    @Deprecated
    public ReturnType atomicPutWithDeleteLock(String keyspaceName, String tableName, String primaryKey,
        PreparedQueryObject queryObject, Condition conditionInfo) throws MusicLockingException {
        return null;
    }

    @Override
    public List<String> getLockQueue(String fullyQualifiedKey)
        throws MusicServiceException, MusicQueryException, MusicLockingException {
        String[] splitString = fullyQualifiedKey.split("\\.");
        String keyspace = splitString[0];
        String table = splitString[1];
        String primaryKeyValue = splitString[2];

        return getLockingServiceHandle().getLockQueue(keyspace, table, primaryKeyValue);
    }
    @Override
    public long getLockQueueSize(String fullyQualifiedKey)
        throws MusicServiceException, MusicQueryException, MusicLockingException {
        String[] splitString = fullyQualifiedKey.split("\\.");
        String keyspace = splitString[0];
        String table = splitString[1];
        String primaryKeyValue = splitString[2];

        return getLockingServiceHandle().getLockQueueSize(keyspace, table, primaryKeyValue);
    }

    @Override
    @Deprecated
    public ResultSet atomicGetWithDeleteLock(String keyspaceName, String tableName, String primaryKey,
            PreparedQueryObject queryObject) throws MusicServiceException, MusicLockingException {
        //deprecated
        return null;
    }
    
    //Methods added for ORM changes
    
    public ResultType createKeyspace(JsonKeySpace jsonKeySpaceObject,String consistencyInfo) 
            throws MusicServiceException,MusicQueryException {
        ResultType result = nonKeyRelatedPut(jsonKeySpaceObject.genCreateKeyspaceQuery(), consistencyInfo);
        logger.info(EELFLoggerDelegate.applicationLogger, " Keyspace Creation Process completed successfully");

        return result;
    }
    
    public ResultType dropKeyspace(JsonKeySpace jsonKeySpaceObject, String consistencyInfo) 
            throws MusicServiceException,MusicQueryException {
        ResultType result = nonKeyRelatedPut(jsonKeySpaceObject.genDropKeyspaceQuery(),
                    consistencyInfo);
        logger.info(EELFLoggerDelegate.applicationLogger, " Keyspace deletion Process completed successfully");
        return result;
    }
    
    public ResultType createTable(JsonTable jsonTableObject, String consistencyInfo) 
            throws MusicServiceException, MusicQueryException {
        ResultType result = null;
        try {
            result = createTable(jsonTableObject.getKeyspaceName(), 
                    jsonTableObject.getTableName(), jsonTableObject.genCreateTableQuery(), consistencyInfo);
            
        } catch (MusicServiceException ex) {
            logger.error(EELFLoggerDelegate.errorLogger, ex.getMessage(), AppMessages.UNKNOWNERROR, ErrorSeverity.WARN,
                    ErrorTypes.MUSICSERVICEERROR);
            throw new MusicServiceException(ex.getMessage());
        }
        logger.info(EELFLoggerDelegate.applicationLogger, " Table Creation Process completed successfully ");
        return result;
    }
    
    public ResultType dropTable(JsonTable jsonTableObject,String consistencyInfo) 
            throws MusicServiceException,MusicQueryException {
        ResultType result = nonKeyRelatedPut(jsonTableObject.genDropTableQuery(),
                    consistencyInfo);
        logger.info(EELFLoggerDelegate.applicationLogger, " Table deletion Process completed successfully ");
        
        return result;
    }
    
    @Override
    public ResultType createIndex(JsonIndex jsonIndexObject, String consistencyInfo) 
            throws MusicServiceException, MusicQueryException{
        ResultType result = nonKeyRelatedPut(jsonIndexObject.genCreateIndexQuery(),
                    consistencyInfo);
        
        logger.info(EELFLoggerDelegate.applicationLogger, " Index creation Process completed successfully ");
        return result;
    }
    
    /**
     * This method performs DDL operation on cassandra.
     *
     * @param queryObject query object containing prepared query and values
     * @return ResultSet
     * @throws MusicServiceException
     */
    public  ResultSet select(JsonSelect jsonSelect, MultivaluedMap<String, String> rowParams) 
            throws MusicServiceException, MusicQueryException {
        ResultSet results = null;
        try {
            results = get(jsonSelect.genSelectQuery(rowParams));
        } catch (MusicServiceException e) {
            logger.error(EELFLoggerDelegate.errorLogger,e.getMessage());
            throw new MusicServiceException(e.getMessage());
        }
        return results;
    }
    
    /**
     * Select Critical
     */
    public ResultSet selectCritical(JsonInsert jsonInsertObj, MultivaluedMap<String, String> rowParams)
            throws MusicLockingException, MusicQueryException, MusicServiceException {
        
        ResultSet results = null;
        String consistency = "";
        if(null != jsonInsertObj && null != jsonInsertObj.getConsistencyInfo()) {
            consistency = jsonInsertObj.getConsistencyInfo().get("type");
        }
        
        String lockId = jsonInsertObj.getConsistencyInfo().get("lockId");
        
        PreparedQueryObject queryObject = jsonInsertObj.genSelectCriticalPreparedQueryObj(rowParams);
        
        if (consistency.equalsIgnoreCase(MusicUtil.CRITICAL)) {
            results = criticalGet(jsonInsertObj.getKeyspaceName(), jsonInsertObj.getTableName(), 
                    jsonInsertObj.getPrimaryKeyVal(), queryObject,lockId);
        } else if (consistency.equalsIgnoreCase(MusicUtil.ATOMIC)) {
            results = atomicGet(jsonInsertObj.getKeyspaceName(), jsonInsertObj.getTableName(),
                    jsonInsertObj.getPrimaryKeyVal(), queryObject);
        }
        
        return results;
    }
    
    /**
     * this is insert row into Table
     */
    public ReturnType insertIntoTable(JsonInsert jsonInsertObj)
            throws MusicLockingException, MusicQueryException, MusicServiceException {
        
        String consistency = "";
        if(null != jsonInsertObj && null != jsonInsertObj.getConsistencyInfo()) {
            consistency = jsonInsertObj.getConsistencyInfo().get("type");
        }
        
        ReturnType result = null;
        
        try {
            PreparedQueryObject queryObj = null;
            queryObj = jsonInsertObj.genInsertPreparedQueryObj();
            
            if (consistency.equalsIgnoreCase(MusicUtil.EVENTUAL)) {
                result = eventualPut(jsonInsertObj.genInsertPreparedQueryObj());
            } else if (consistency.equalsIgnoreCase(MusicUtil.CRITICAL)) {
                String lockId = jsonInsertObj.getConsistencyInfo().get("lockId");
                if(lockId == null) {
                    logger.error(EELFLoggerDelegate.errorLogger,"LockId cannot be null. Create lock reference or"
                            + " use ATOMIC instead of CRITICAL", ErrorSeverity.FATAL, ErrorTypes.MUSICSERVICEERROR);
                    return new ReturnType(ResultType.FAILURE, "LockId cannot be null. Create lock "
                            + "and acquire lock or use ATOMIC instead of CRITICAL");
                }
                result = criticalPut(jsonInsertObj.getKeyspaceName(), 
                        jsonInsertObj.getTableName(), jsonInsertObj.getPrimaryKeyVal(), jsonInsertObj.genInsertPreparedQueryObj(), lockId,null);
            } else if (consistency.equalsIgnoreCase(MusicUtil.ATOMIC)) {
                result = atomicPut(jsonInsertObj.getKeyspaceName(), jsonInsertObj.getTableName(), 
                        jsonInsertObj.getPrimaryKeyVal(), jsonInsertObj.genInsertPreparedQueryObj(), null);
            }
        } catch (Exception ex) {
            logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage(), AppMessages.UNKNOWNERROR  ,ErrorSeverity
                .WARN, ErrorTypes.MUSICSERVICEERROR, ex);
            return new ReturnType(ResultType.FAILURE, ex.getMessage());
        }
        
        return result;
    }
    
     /**
     * This is insert row into Table
     */
    public ReturnType updateTable(JsonUpdate jsonUpdateObj, MultivaluedMap<String, String> rowParams)
            throws MusicLockingException, MusicQueryException, MusicServiceException {
        
        ReturnType result = null;
        String consistency = "";
        if(null != jsonUpdateObj && null != jsonUpdateObj.getConsistencyInfo()) {
            consistency = jsonUpdateObj.getConsistencyInfo().get("type");
        }
        PreparedQueryObject queryObject = jsonUpdateObj.genUpdatePreparedQueryObj(rowParams);
        
        Condition conditionInfo;
        if (jsonUpdateObj.getConditions() == null) {
            conditionInfo = null;
        } else {
            // to avoid parsing repeatedly, just send the select query to obtain row
            PreparedQueryObject selectQuery = new PreparedQueryObject();
            selectQuery.appendQueryString("SELECT *  FROM " + jsonUpdateObj.getKeyspaceName() + "." + jsonUpdateObj.getTableName() + " WHERE "
                + jsonUpdateObj.getRowIdString() + ";");
            selectQuery.addValue(jsonUpdateObj.getPrimarKeyValue());
            conditionInfo = new Condition(jsonUpdateObj.getConditions(), selectQuery);
        }

        
        if (consistency.equalsIgnoreCase(MusicUtil.EVENTUAL)) {
            result = eventualPut(queryObject);
        } else if (consistency.equalsIgnoreCase(MusicUtil.CRITICAL)) {
            String lockId = jsonUpdateObj.getConsistencyInfo().get("lockId");
            if(lockId == null) {
                logger.error(EELFLoggerDelegate.errorLogger,"LockId cannot be null. Create lock reference or"
                        + " use ATOMIC instead of CRITICAL", ErrorSeverity.FATAL, ErrorTypes.MUSICSERVICEERROR);
               
                return new ReturnType(ResultType.FAILURE, "LockId cannot be null. Create lock "
                        + "and acquire lock or use ATOMIC instead of CRITICAL");
            }
            result = criticalPut(jsonUpdateObj.getKeyspaceName(), jsonUpdateObj.getTableName(), jsonUpdateObj.getPrimarKeyValue(),
                            queryObject, lockId, conditionInfo);
        } else if (consistency.equalsIgnoreCase("atomic_delete_lock")) {
            // this function is mainly for the benchmarks
            try {
                result = atomicPutWithDeleteLock(jsonUpdateObj.getKeyspaceName(), jsonUpdateObj.getTableName(),
                        jsonUpdateObj.getPrimarKeyValue(), queryObject, conditionInfo);
            } catch (MusicLockingException e) {
                logger.error(EELFLoggerDelegate.errorLogger,e, AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN,
                    ErrorTypes.GENERALSERVICEERROR, e);
                throw new MusicLockingException(AppMessages.UNKNOWNERROR.toString());
                
            }
        } else if (consistency.equalsIgnoreCase(MusicUtil.ATOMIC)) {
            try {
                result = atomicPut(jsonUpdateObj.getKeyspaceName(), jsonUpdateObj.getTableName(), jsonUpdateObj.getPrimarKeyValue(),
                    queryObject, conditionInfo);
            } catch (MusicLockingException e) {
                logger.error(EELFLoggerDelegate.errorLogger,e, AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes.GENERALSERVICEERROR, e);
                throw new MusicLockingException(AppMessages.UNKNOWNERROR.toString());
            }
        } else if (consistency.equalsIgnoreCase(MusicUtil.EVENTUAL_NB)) {
            try {
                result = eventualPut_nb(queryObject, jsonUpdateObj.getKeyspaceName(),
                        jsonUpdateObj.getTableName(), jsonUpdateObj.getPrimarKeyValue());
            }catch (Exception e) {
                return new ReturnType(ResultType.FAILURE, e.getMessage());
            }
            
        }
        
        return result;
    }
    
    /**
     * This method is for Delete From Table
     */
    public ReturnType deleteFromTable(JsonDelete jsonDeleteObj, MultivaluedMap<String, String> rowParams)
            throws MusicLockingException, MusicQueryException, MusicServiceException {
        
        ReturnType result = null;
        String consistency = "";
        if(null != jsonDeleteObj && null != jsonDeleteObj.getConsistencyInfo()) {
            consistency = jsonDeleteObj.getConsistencyInfo().get("type");
        }
        PreparedQueryObject queryObject = jsonDeleteObj.genDeletePreparedQueryObj(rowParams);
        
        // get the conditional, if any
        Condition conditionInfo;
        if (jsonDeleteObj.getConditions() == null) {
            conditionInfo = null;
        } else {
            // to avoid parsing repeatedly, just send the select query to obtain row
            PreparedQueryObject selectQuery = new PreparedQueryObject();
            selectQuery.appendQueryString("SELECT *  FROM " + jsonDeleteObj.getKeyspaceName() + "." + jsonDeleteObj.getTableName() + " WHERE "
                + jsonDeleteObj.getRowIdString() + ";");
            selectQuery.addValue(jsonDeleteObj.getPrimarKeyValue());
            conditionInfo = new Condition(jsonDeleteObj.getConditions(), selectQuery);
        }
        
        if (consistency.equalsIgnoreCase(MusicUtil.EVENTUAL))
            result = eventualPut(queryObject);
        else if (consistency.equalsIgnoreCase(MusicUtil.CRITICAL)) {
            String lockId = jsonDeleteObj.getConsistencyInfo().get("lockId");
            if(lockId == null) {
                logger.error(EELFLoggerDelegate.errorLogger,"LockId cannot be null. Create lock reference or"
                    + " use ATOMIC instead of CRITICAL", ErrorSeverity.FATAL, ErrorTypes.MUSICSERVICEERROR);
               
                return new ReturnType(ResultType.FAILURE, "LockId cannot be null. Create lock "
                        + "and acquire lock or use ATOMIC instead of CRITICAL");
            }
            result = criticalPut(jsonDeleteObj.getKeyspaceName(), 
                    jsonDeleteObj.getTableName(), jsonDeleteObj.getPrimarKeyValue(),
                queryObject, lockId, conditionInfo);
        } else if (consistency.equalsIgnoreCase(MusicUtil.ATOMIC)) {
            result = atomicPut(jsonDeleteObj.getKeyspaceName(), 
                    jsonDeleteObj.getTableName(), jsonDeleteObj.getPrimarKeyValue(),
                queryObject, conditionInfo);
        } else if(consistency.equalsIgnoreCase(MusicUtil.EVENTUAL_NB)) {                    
            result = eventualPut_nb(queryObject, jsonDeleteObj.getKeyspaceName(), 
                    jsonDeleteObj.getTableName(), jsonDeleteObj.getPrimarKeyValue());
        }
        
        return result;
    }


}
