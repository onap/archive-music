/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 *  Modifications Copyright (C) 2019 IBM.
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

package org.onap.music.lockingservice.cassandra;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.onap.music.datastore.MusicDataStore;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.DeadlockDetectionUtil;
import org.onap.music.main.DeadlockDetectionUtil.OwnershipType;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.main.ReturnType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

/*
 * This is the lock store that is built on top of Cassandra that is used by MUSIC to maintain lock state. 
 */

public class CassaLockStore {
    
    private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(CassaLockStore.class);
    private static String table_prepend_name = "lockQ_";
    private MusicDataStore dsHandle;
    
    public CassaLockStore() {
        dsHandle = new MusicDataStore();
    }
    
    public CassaLockStore(MusicDataStore dsHandle) {
        this.dsHandle=dsHandle;
    }
    public class LockObject{
        private boolean isLockOwner;
        private String lockRef;
        private String createTime;
        private String acquireTime;
        private LockType locktype;
        // Owner is the self-declared client which "owns" this row. It is used for deadlock detection.  It is not (directly) related to isLockOwner.
        private String owner;
        public LockObject(boolean isLockOwner, String lockRef, String createTime, String acquireTime, LockType locktype, String owner) {
            this.setIsLockOwner(isLockOwner);
            this.setLockRef(lockRef);
            this.setAcquireTime(acquireTime);
            this.setCreateTime(createTime);
            this.setLocktype(locktype);
            this.setOwner(owner);
        }
        public boolean getIsLockOwner() {
            return isLockOwner;
        }
        public void setIsLockOwner(boolean isLockOwner) {
            this.isLockOwner = isLockOwner;
        }
        public String getAcquireTime() {
            return acquireTime;
        }
        public void setAcquireTime(String acquireTime) {
            this.acquireTime = acquireTime;
        }
        public String getCreateTime() {
            return createTime;
        }
        public void setCreateTime(String createTime) {
            this.createTime = createTime;
        }
        public String getLockRef() {
            return lockRef;
        }
        public void setLockRef(String lockRef) {
            this.lockRef = lockRef;
        }
        public LockType getLocktype() {
            return locktype;
        }
        public void setLocktype(LockType locktype) {
            this.locktype = locktype;
        }
        public String getOwner() {
            return owner;
        }
        public void setOwner(String owner) {
            this.owner = owner;
        }
    }
    
    /**
     * 
     * This method creates a shadow locking table for every main table in Cassandra. This table tracks all information regarding locks. 
     * @param keyspace of the application. 
     * @param table of the application. 
     * @return true if the operation was successful.
     * @throws MusicServiceException
     * @throws MusicQueryException
     */
    public boolean createLockQueue(String keyspace, String table) throws MusicServiceException, MusicQueryException {
        logger.info(EELFLoggerDelegate.applicationLogger,
                "Create lock queue/table for " +  keyspace+"."+table);
        table = table_prepend_name+table;
        String tabQuery = "CREATE TABLE IF NOT EXISTS "+keyspace+"."+table
                + " ( key text, lockReference bigint, createTime text, acquireTime text, guard bigint static, "
                + "lockType text, owner text, PRIMARY KEY ((key), lockReference) ) "
                + "WITH CLUSTERING ORDER BY (lockReference ASC);";
        PreparedQueryObject queryObject = new PreparedQueryObject();
        
        queryObject.appendQueryString(tabQuery);
        boolean result;
        result = dsHandle.executePut(queryObject, "eventual");
        return result;
    }

    /**
     * This method creates a lock reference for each invocation. The lock references are monotonically increasing timestamps.
     * @param keyspace of the locks.
     * @param table of the locks.
     * @param lockName is the primary key of the lock table
     * @param lockType is the type of lock (read/write)
     * @param owner is the owner of the lock (optional, for deadlock detection)
     * @return the UUID lock reference.
     * @throws MusicServiceException
     * @throws MusicQueryException
     */
    public String genLockRefandEnQueue(String keyspace, String table, String lockName, LockType locktype, String owner) throws MusicServiceException, MusicQueryException, MusicLockingException {
        return genLockRefandEnQueue(keyspace, table, lockName, locktype, owner, 0);
    }
    
    private String genLockRefandEnQueue(String keyspace, String table, String lockName, LockType locktype, String owner, int count) throws MusicServiceException, MusicQueryException, MusicLockingException {
        logger.info(EELFLoggerDelegate.applicationLogger,
                "Create " + locktype + " lock reference for " +  keyspace + "." + table + "." + lockName);
        String lockTable ="";
        lockTable = table_prepend_name+table;
    
        PreparedQueryObject queryObject = new PreparedQueryObject();
        String selectQuery = "SELECT guard FROM " + keyspace + "." + lockTable + " WHERE key=?;";

        queryObject.addValue(lockName);
        queryObject.appendQueryString(selectQuery);
        ResultSet gqResult = dsHandle.executeOneConsistencyGet(queryObject);
        List<Row> latestGuardRow = gqResult.all();

        long prevGuard = 0;
        long lockRef = 1;
        if (!latestGuardRow.isEmpty()) {
            prevGuard = latestGuardRow.get(0).getLong(0);
            lockRef = prevGuard + 1;
        }

        long lockEpochMillis = System.currentTimeMillis();

        logger.info(EELFLoggerDelegate.applicationLogger,
                "Created lock reference for " +  keyspace + "." + lockTable + "." + lockName + ":" + lockRef);
        
        queryObject = new PreparedQueryObject();
        
        String insQuery = "BEGIN BATCH" +
                " UPDATE " + keyspace + "." + lockTable +
                " SET guard=? WHERE key=? IF guard = " + (prevGuard == 0 ? "NULL" : "?") +";" +
                " INSERT INTO " + keyspace + "." + lockTable +
                "(key, lockReference, createTime, acquireTime, lockType, owner) VALUES (?,?,?,?,?,?) IF NOT EXISTS; APPLY BATCH;";

        queryObject.addValue(lockRef);
        queryObject.addValue(lockName);
        if (prevGuard != 0)
            queryObject.addValue(prevGuard);

        queryObject.addValue(lockName);
        queryObject.addValue(lockRef);
        queryObject.addValue(String.valueOf(lockEpochMillis));
        queryObject.addValue("0");
        queryObject.addValue(locktype);
        queryObject.addValue(owner);
        queryObject.appendQueryString(insQuery);
        boolean pResult = dsHandle.executePut(queryObject, "critical");
        if (!pResult) {// couldn't create lock ref, retry
            count++;
            if (count > MusicUtil.getRetryCount()) {
                logger.warn(EELFLoggerDelegate.applicationLogger, "Unable to create lock reference");
                throw new MusicLockingException("Unable to create lock reference");
            }
            return genLockRefandEnQueue(keyspace, table, lockName, locktype, owner, count);
        }
        return "$" + keyspace + "." + table + "." + lockName + "$" + String.valueOf(lockRef);
    }

	/**
     * Returns a result set containing the list of clients waiting for a particular lock
     * 
     * @param keyspace
     * @param table
     * @param key
     * @return list of lockrefs in the queue
     * @throws MusicServiceException
     * @throws MusicQueryException
     */
    public List<String> getLockQueue(String keyspace, String table, String key)
            throws MusicServiceException, MusicQueryException {
        logger.info(EELFLoggerDelegate.applicationLogger,
                "Getting the queue for " + keyspace + "." + table + "." + key);
        table = table_prepend_name + table;
        String selectQuery = "select * from " + keyspace + "." + table + " where key='" + key + "';";
        PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString(selectQuery);
        ResultSet rs = dsHandle.executeOneConsistencyGet(queryObject);
        ArrayList<String> lockQueue = new ArrayList<>();
        for (Row row : rs) {
            lockQueue.add(Long.toString(row.getLong("lockReference")));
        }
        return lockQueue;
    }


    /**
     * Returns a result set containing the list of clients waiting for a particular lock
     * 
     * @param keyspace
     * @param table
     * @param key
     * @return size of lockrefs queue
     * @throws MusicServiceException
     * @throws MusicQueryException
     */
    public long getLockQueueSize(String keyspace, String table, String key)
            throws MusicServiceException, MusicQueryException {
        logger.info(EELFLoggerDelegate.applicationLogger,
                "Getting the queue size for " + keyspace + "." + table + "." + key);
        table = table_prepend_name + table;
        String selectQuery = "select count(*) from " + keyspace + "." + table + " where key='" + key + "';";
        PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString(selectQuery);
		ResultSet rs = dsHandle.executeOneConsistencyGet(queryObject);
		return rs.one().getLong("count");
	}


    /**
     * This method returns the top of lock table/queue for the key.
     * 
     * @param keyspace of the application.
     * @param table of the application.
     * @param key is the primary key of the application table
     * @return the UUID lock reference. Returns LockObject.isLockOwner=false if there is no owner or the
     *         lock doesn't exist
     * @throws MusicServiceException
     * @throws MusicQueryException
     */
    public LockObject peekLockQueue(String keyspace, String table, String key)
            throws MusicServiceException, MusicQueryException {
        logger.info(EELFLoggerDelegate.applicationLogger,
                "Peek in lock table for " + keyspace + "." + table + "." + key);
        table = table_prepend_name + table;
        String selectQuery = "select * from " + keyspace + "." + table + " where key='" + key + "' LIMIT 1;";
        PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString(selectQuery);
        ResultSet results = dsHandle.executeOneConsistencyGet(queryObject);
        Row row = results.one();
        if (row == null || row.isNull("lockReference")) {
            return new LockObject(false, null, null, null, null, null);
        }
        String lockReference = "" + row.getLong("lockReference");
        String createTime = row.getString("createTime");
        String acquireTime = row.getString("acquireTime");
        LockType locktype = row.get("lockType", LockType.class);
        String owner = row.getString("owner");

        return new LockObject(true, lockReference, createTime, acquireTime, locktype, owner);
    }

    public List<String> getCurrentLockHolders(String keyspace, String table, String key)
            throws MusicServiceException, MusicQueryException {
        logger.info(EELFLoggerDelegate.applicationLogger,
                "Getting lockholders in lock table for " + keyspace + "." + table + "." + key);
        String origTable = table;
        table = table_prepend_name + table;
        String selectQuery = "select * from " + keyspace + "." + table + " where key=?;";
        List<String> lockHolders = new ArrayList<>();
        PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString(selectQuery);
        queryObject.addValue(key);
        ResultSet rs = dsHandle.executeOneConsistencyGet(queryObject);
        boolean topOfQueue = true;
        StringBuilder lock = new StringBuilder().
        append("$").append(keyspace).append(".").append(origTable).
        append(".").append(key).append("$");
        StringBuilder lockReference = new StringBuilder();
        for (Row row : rs) {
                if ( row.isNull("lockReference") ) {
                    return lockHolders;
                }
                lockReference.append(lock).append(row.getLong("lockReference"));
            if (row.get("lockType", LockType.class)!=LockType.WRITE) {
                if (topOfQueue) {
                    lockHolders.add(lockReference.toString());
                    break;
                } else {
                    break;
                }
            }
            // read lock
            lockHolders.add(lockReference.toString());

            topOfQueue = false;
            lockReference.delete(0,lockReference.length());
        }
        return lockHolders;
    }

    /**
     * Determine if the lock is a valid current lock holder.
     * 
     * @param keyspace
     * @param table
     * @param key
     * @param lockRef
     * @return true if lockRef is a lock owner of key
     * @throws MusicServiceException
     * @throws MusicQueryException
     */
    public boolean isLockOwner(String keyspace, String table, String key, String lockRef)
            throws MusicServiceException, MusicQueryException {
        logger.info(EELFLoggerDelegate.applicationLogger,
                "Checking in lock table for " + keyspace + "." + table + "." + key);
        table = table_prepend_name + table;
        String selectQuery = 
                "select * from " + keyspace + "." + table + " where key=?;";
        PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString(selectQuery);
        queryObject.addValue(key);
        ResultSet rs = dsHandle.executeOneConsistencyGet(queryObject);

        boolean topOfQueue = true;
        for (Row row : rs) {
            String lockReference = "" + row.getLong("lockReference");
            if (row.get("lockType", LockType.class)==LockType.WRITE) {
                if (topOfQueue && lockRef.equals(lockReference)) {
                    return true;
                } else {
                    return false;
                }
            }
            if (lockRef.equals(lockReference)) {
                return true;
            }
            topOfQueue = false;
        }
        logger.info(EELFLoggerDelegate.applicationLogger, "Could not find " + lockRef
                + " in the lock queue. It has expired and no longer exists.");
        return false;
    }

    /**
     * Determine if the lock is a valid current lock holder.
     * 
     * @param keyspace
     * @param table
     * @param key
     * @param lockRef
     * @return true if lockRef is a lock owner of key
     * @throws MusicServiceException
     * @throws MusicQueryException
     */
    public LockObject getLockInfo(String keyspace, String table, String key, String lockRef)
            throws MusicServiceException, MusicQueryException {
        logger.info(EELFLoggerDelegate.applicationLogger,
                "Checking in lock table for " + keyspace + "." + table + "." + key);
        String lockQ_table = table_prepend_name + table;
        String selectQuery = 
                "select * from " + keyspace + "." + lockQ_table + " where key=? and lockReference=?;";
        PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString(selectQuery);
        queryObject.addValue(key);
        queryObject.addValue(Long.parseLong(lockRef));
        ResultSet rs = dsHandle.executeOneConsistencyGet(queryObject);
        Row row = rs.one();
        if (row == null || row.isNull("lockReference")) {
            return null;
        }

        String lockReference = "" + row.getLong("lockReference");
        String createTime = row.getString("createTime");
        String acquireTime = row.getString("acquireTime");
        LockType locktype = row.get("lockType", LockType.class);
        boolean isLockOwner = isLockOwner(keyspace, table, key, lockRef);
        String owner = row.getString("owner");

        return new LockObject(isLockOwner, lockReference, createTime, acquireTime, locktype, owner);
    }



    /**
     * This method removes the lock ref from the lock table/queue for the key.
     * 
     * @param keyspace of the application.
     * @param table of the application.
     * @param key is the primary key of the application table
     * @param lockReference the lock reference that needs to be dequeued.
     * @throws MusicServiceException
     * @throws MusicQueryException
     * @throws MusicLockingException
     */
    public void deQueueLockRef(String keyspace, String table, String key, String lockReference, int n)
            throws MusicServiceException, MusicQueryException, MusicLockingException {
        String prependTable = table_prepend_name + table;
        PreparedQueryObject queryObject = new PreparedQueryObject();
        Long lockReferenceL = Long.parseLong(lockReference.substring(lockReference.lastIndexOf("$") + 1));
        String deleteQuery = "delete from " + keyspace + "." + prependTable + " where key='" + key
                + "' AND lockReference =" + lockReferenceL + " IF EXISTS;";
        queryObject.appendQueryString(deleteQuery);
        logger.info(EELFLoggerDelegate.applicationLogger, "Removing lock for key: "+key+ " and reference: "+lockReference);
        try {
            dsHandle.executePut(queryObject, "critical");
            logger.info(EELFLoggerDelegate.applicationLogger,
                    "Lock removed for key: " + key + " and reference: " + lockReference);
        } catch (MusicServiceException ex) {
            logger.error(logger, ex.getMessage(), ex);
            logger.error(EELFLoggerDelegate.applicationLogger,
                    "Exception while deQueueLockRef for lockname: " + key + " reference:" + lockReference);
            if (n > 1) {
                logger.info(EELFLoggerDelegate.applicationLogger, "Trying again...");
                deQueueLockRef(keyspace, table, key, lockReference, n - 1);
            } else {
                logger.error(EELFLoggerDelegate.applicationLogger,
                        "deQueueLockRef failed for lockname: " + key + " reference:" + lockReference);
                logger.error(logger, ex.getMessage(), ex);
                throw new MusicLockingException("Error while deQueueLockRef: " + ex.getMessage());
            }
        }
    }


    public void updateLockAcquireTime(String keyspace, String table, String key, String lockReference) {
        table = table_prepend_name + table;
        Long lockReferenceL = Long.parseLong(lockReference);
        String updateQuery = "update " + keyspace + "." + table + " set acquireTime='" + System.currentTimeMillis()
                + "' where key='" + key + "' AND lockReference = " + lockReferenceL + " IF EXISTS;";

        //cannot use executePut because we need to ignore music timestamp adjustments for lock store
        dsHandle.getSession().execute(updateQuery);
    }  

    public boolean checkForDeadlock(String keyspace, String table, String lockName, LockType locktype, String owner, boolean forAcquire) throws MusicServiceException, MusicQueryException {
        if (locktype.equals(LockType.READ)) return false;
        if (owner==null || owner.length()==0) return false;

        String lockTable = table_prepend_name + table;
        PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString("SELECT key, acquiretime, owner FROM " + keyspace + "." + lockTable);
        queryObject.appendQueryString(" WHERE lockType = ? ALLOW FILTERING");
        queryObject.addValue(LockType.WRITE);

        DeadlockDetectionUtil ddu = new DeadlockDetectionUtil();

        ResultSet rs = dsHandle.executeLocalQuorumConsistencyGet(queryObject);
        logger.debug("rs has " + rs.getAvailableWithoutFetching() + (rs.isFullyFetched()?"":" (or more!)") );
        Iterator<Row> it = rs.iterator();
        while (it.hasNext()) {
            Row row = it.next();
            logger.debug("key = " + row.getString("key") + ", time = " + row.getString("acquiretime") + ", owner = " + row.getString("owner") );
            ddu.setExisting(row.getString("key"), row.getString("owner"), ("0".equals(row.getString("acquiretime")))?OwnershipType.CREATED:OwnershipType.ACQUIRED);
        }
        boolean deadlock = ddu.checkForDeadlock(lockName, owner, forAcquire?OwnershipType.ACQUIRED:OwnershipType.CREATED);
        if (deadlock) logger.warn("Deadlock detected when " + owner + " tried to create lock on " + keyspace + "." + lockTable + "." + lockName);
        return deadlock;
    }

    public List<String> getAllLocksForOwner(String ownerId, String keyspace, String table) throws MusicServiceException, MusicQueryException {
        List<String> toRet = new ArrayList<String>();
        String lockTable = table_prepend_name + table;
        PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString("SELECT key, lockreference FROM " + keyspace + "." + lockTable);
        queryObject.appendQueryString(" WHERE owner = '" + ownerId + "' ALLOW FILTERING");

        ResultSet rs = dsHandle.executeQuorumConsistencyGet(queryObject);
        Iterator<Row> it = rs.iterator();
        while (it.hasNext()) {
            Row row = it.next();
            toRet.add(row.getString("key") + "$" + row.getLong("lockreference"));
        }
        return toRet;
    }

    public ReturnType promoteLock(String keyspace, String table, String key, String lockRef)
            throws MusicServiceException, MusicQueryException {
        String lockqtable = table_prepend_name + table;
        String selectQuery = "select * from " + keyspace + "." + lockqtable + " where key=?;";

        PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString(selectQuery);
        queryObject.addValue(key);
        ResultSet rs = dsHandle.executeOneConsistencyGet(queryObject);
        
        long refToPromote = Long.parseLong(lockRef);

        boolean topOfQueue = true;
        boolean readBlock = false;
        boolean seenLockToPromote = false;
        boolean promotionOngoing = false;
        long readBlockStart = 0;
        long readBlockEnd = 0;


        for (Row row : rs) {
            long ref = row.getLong("lockreference");
            LockType lockType = row.get("lockType", LockType.class);
            
            if (refToPromote==ref) {
                if (promotionOngoing) {
                    return new ReturnType(ResultType.FAILURE, "Can't promote, already promoting another lockref.");
                }
                seenLockToPromote = true;
                if (!topOfQueue) {
                    readBlockStart = ref;
                    readBlockEnd = ref;
                    break;
                }
            } else if (!seenLockToPromote && refToPromote<ref) {
                return new ReturnType(ResultType.FAILURE, "Lockref does not exist.");
            }
            
            if (lockType==LockType.READ || lockType==LockType.PROMOTING) {
                if (!readBlock) {
                    readBlockStart = ref;
                    readBlock = true;
                }
                if (readBlock) {
                    readBlockEnd = ref;
                }
                if (lockType==LockType.PROMOTING) {
                    promotionOngoing = true;
                }
            }
            
            if (lockType==LockType.WRITE) {
                if (refToPromote==ref) {
                    return new ReturnType(ResultType.FAILURE, "Lockref is already write.");
                }
                if (readBlock) {
                    readBlock = false;
                    promotionOngoing = false;
                    if (seenLockToPromote) {
                        break;
                    }
                    //can no longer be lock holder after this
                    topOfQueue = false;
                }
            }
        }

        if (readBlockStart<=refToPromote && refToPromote<=readBlockEnd) {
            if (readBlockStart==refToPromote && refToPromote==readBlockEnd) {
                promoteLockTo(keyspace, lockqtable, key, lockRef, LockType.WRITE);
                return new ReturnType(ResultType.SUCCESS, "Lock has successfully been upgraded.");
            }
            promoteLockTo(keyspace, lockqtable, key, lockRef, LockType.PROMOTING);
            return new ReturnType(ResultType.FAILURE, "Your lock upgrade is in progress. Check again to see if successful."); 
        }
        
        //shouldn't reach here?
        return new ReturnType(ResultType.FAILURE,"Promotion failed.");
    }

    private void promoteLockTo(String keyspace, String table, String key, String lockRef, LockType newLockType)
            throws MusicServiceException, MusicQueryException {
        PreparedQueryObject queryObject =
                new PreparedQueryObject("UPDATE " + keyspace + "." + table + " SET lockType=? WHERE key='" + key
                        + "' AND lockReference = " + lockRef + " IF EXISTS;", newLockType);

        //cannot use executePut because we need to ignore music timestamp adjustments for lock store
        dsHandle.executePut(queryObject, MusicUtil.QUORUM);
    }
    


}
