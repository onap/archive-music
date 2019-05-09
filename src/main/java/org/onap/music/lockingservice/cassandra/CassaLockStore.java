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
import java.util.List;

import org.onap.music.datastore.MusicDataStore;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicUtil;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

/*
 * This is the lock store that is built on top of Cassandra that is used by MUSIC to maintain lock state. 
 */

public class CassaLockStore {
    
    private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(CassaLockStore.class);
    private static String table_prepend_name = "lockQ_";
    
    public class LockObject{
        public String lockRef;
        public String createTime;
        public String acquireTime;
        public LockObject(String lockRef, String createTime,     String acquireTime) {
            this.lockRef = lockRef;
            this.acquireTime = acquireTime;
            this.createTime = createTime;
            
        }
    }
    MusicDataStore dsHandle;
    public CassaLockStore() {
        dsHandle = new MusicDataStore();
    }
    
    public CassaLockStore(MusicDataStore dsHandle) {
        this.dsHandle=dsHandle;
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
                + " ( key text, lockReference bigint, createTime text, acquireTime text, guard bigint static, PRIMARY KEY ((key), lockReference) ) "
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
     * @return the UUID lock reference.
     * @throws MusicServiceException
     * @throws MusicQueryException
     */
    public String genLockRefandEnQueue(String keyspace, String table, String lockName) throws MusicServiceException, MusicQueryException, MusicLockingException {
        return genLockRefandEnQueue(keyspace, table, lockName, 0);
    }
    
    private String genLockRefandEnQueue(String keyspace, String table, String lockName, int count) throws MusicServiceException, MusicQueryException, MusicLockingException {
        logger.info(EELFLoggerDelegate.applicationLogger,
                "Create lock reference for " +  keyspace + "." + table + "." + lockName);
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
                "(key, lockReference, createTime, acquireTime) VALUES (?,?,?,?) IF NOT EXISTS; APPLY BATCH;";

        queryObject.addValue(lockRef);
        queryObject.addValue(lockName);
        if (prevGuard != 0)
            queryObject.addValue(prevGuard);

        queryObject.addValue(lockName);
        queryObject.addValue(lockRef);
        queryObject.addValue(String.valueOf(lockEpochMillis));
        queryObject.addValue("0");
        queryObject.appendQueryString(insQuery);
        boolean pResult = dsHandle.executePut(queryObject, "critical");
        if (!pResult) {//couldn't create lock ref, retry
            count++;
            if (count>MusicUtil.getRetryCount()) {
                logger.warn(EELFLoggerDelegate.applicationLogger, "Unable to create lock reference");
                throw new MusicLockingException("Unable to create lock reference");
            }
            return genLockRefandEnQueue(keyspace, table, lockName, count);
        }
        return "$" + keyspace + "." + table + "." + lockName + "$" + String.valueOf(lockRef);
    }
    
    
    
    /**
     * Returns a result set containing the list of clients waiting for a particular lock
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
                "Getting the queue for " +  keyspace+"."+table+"."+key);
        table = table_prepend_name+table;
        String selectQuery = "select * from " + keyspace + "." + table + " where key='" + key + "';";
        PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString(selectQuery);
        ResultSet rs = dsHandle.executeOneConsistencyGet(queryObject);
        ArrayList<String> lockQueue = new ArrayList<>();
        for (Row row: rs) {
            lockQueue.add(Long.toString(row.getLong("lockReference")));
        }
        return lockQueue;
    }
    
    
    /**
     * Returns a result set containing the list of clients waiting for a particular lock
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
                "Getting the queue size for " +  keyspace+"."+table+"."+key);
        table = table_prepend_name+table;
        String selectQuery = "select count(*) from " + keyspace + "." + table + " where key='" + key + "';";
        PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString(selectQuery);
        ResultSet rs = dsHandle.executeOneConsistencyGet(queryObject);
        return rs.one().getLong("count");
    }


    /**
     * This method returns the top of lock table/queue for the key.
     * @param keyspace of the application.
     * @param table of the application.
     * @param key is the primary key of the application table
     * @return the UUID lock reference. Returns null if there is no owner or the lock doesn't exist
     * @throws MusicServiceException
     * @throws MusicQueryException
     */
    public LockObject peekLockQueue(String keyspace, String table, String key) throws MusicServiceException, MusicQueryException{
        logger.info(EELFLoggerDelegate.applicationLogger,
                "Peek in lock table for " +  keyspace+"."+table+"."+key);
        table = table_prepend_name+table; 
        String selectQuery = "select * from "+keyspace+"."+table+" where key='"+key+"' LIMIT 1;";    
        PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString(selectQuery);
        ResultSet results = dsHandle.executeOneConsistencyGet(queryObject);
        Row row = results.one();
        if (row==null || row.isNull("lockReference")) {
            return null;
        }
        String lockReference = "" + row.getLong("lockReference");
        String createTime = row.getString("createTime");
        String acquireTime = row.getString("acquireTime");

        return new LockObject(lockReference, createTime,acquireTime);
    }
    
    
    /**
     * This method removes the lock ref from the lock table/queue for the key. 
     * @param keyspace of the application. 
     * @param table of the application. 
     * @param key is the primary key of the application table
     * @param lockReference the lock reference that needs to be dequeued.
     * @throws MusicServiceException
     * @throws MusicQueryException
     * @throws MusicLockingException 
     */    
    public void deQueueLockRef(String keyspace, String table, String key, String lockReference, int n) throws MusicServiceException, MusicQueryException, MusicLockingException{
        String prependTable = table_prepend_name+table;
        PreparedQueryObject queryObject = new PreparedQueryObject();
        Long lockReferenceL = Long.parseLong(lockReference.substring(lockReference.lastIndexOf("$")+1));
        String deleteQuery = "delete from "+keyspace+"."+prependTable+" where key='"+key+"' AND lockReference ="+lockReferenceL+" IF EXISTS;";
        queryObject.appendQueryString(deleteQuery);
        logger.info(EELFLoggerDelegate.applicationLogger, "Removing lock for key: "+key+ " and reference: "+lockReference);
        try {
        dsHandle.executePut(queryObject, "critical"); 
        logger.info(EELFLoggerDelegate.applicationLogger, "Lock removed for key: "+key+ " and reference: "+lockReference);
        }catch(MusicServiceException ex) {
            logger.error(logger, ex.getMessage(),ex);
            logger.error(EELFLoggerDelegate.applicationLogger,"Exception while deQueueLockRef for lockname: " + key + " reference:" +lockReference);
            if(n>1) {
                logger.info(EELFLoggerDelegate.applicationLogger, "Trying again...");
                deQueueLockRef(keyspace, table, key, lockReference, n-1);
            }
            else {
                logger.error(EELFLoggerDelegate.applicationLogger,"deQueueLockRef failed for lockname: " + key + " reference:" +lockReference);
                logger.error(logger, ex.getMessage(),ex);
                throw new MusicLockingException("Error while deQueueLockRef: "+ex.getMessage());
            }
        }
    }
    

    public void updateLockAcquireTime(String keyspace, String table, String key, String lockReference) throws MusicServiceException, MusicQueryException{
        table = table_prepend_name+table;
        PreparedQueryObject queryObject = new PreparedQueryObject();
        Long lockReferenceL = Long.parseLong(lockReference);
        String updateQuery = "update "+keyspace+"."+table+" set acquireTime='"+ System.currentTimeMillis()+"' where key='"+key+"' AND lockReference = "+lockReferenceL+" IF EXISTS;";
        queryObject.appendQueryString(updateQuery);
        dsHandle.executePut(queryObject, "eventual");    

    }
    

}
