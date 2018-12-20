package org.onap.music.lockingservice.cassandra;

import java.util.ArrayList;
import java.util.List;

import org.onap.music.datastore.MusicDataStore;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.onap.music.util.TimeMeasureInstance;

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
		public LockObject(String lockRef, String createTime, 	String acquireTime) {
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
				+ " ( key text, lockReference bigint, createTime text, acquireTime text, guard bigint static, writeLock boolean, PRIMARY KEY ((key), lockReference) ) "
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
	public String genLockRefandEnQueue(String keyspace, String table, String lockName)
			throws MusicServiceException, MusicQueryException {
		return genLockRefandEnQueue(keyspace, table, lockName, true);
	}

	
	/**
	 * This method creates a lock reference for each invocation. The lock references are monotonically increasing timestamps.
	 * @param keyspace of the locks.
	 * @param table of the locks.
	 * @param lockName is the primary key of the lock table
	 * @param isWriteLock true if this lock needs to be a write lock
	 * @return the UUID lock reference.
	 * @throws MusicServiceException
	 * @throws MusicQueryException
	 */
	public String genLockRefandEnQueue(String keyspace, String table, String lockName, boolean isWriteLock)
		throws MusicServiceException, MusicQueryException {
		TimeMeasureInstance.instance().enter("genLockRefandEnQueue");
		try {
			logger.info(EELFLoggerDelegate.applicationLogger,
					"Create lock reference for " + keyspace + "." + table + "." + lockName);
			table = table_prepend_name+table;


			PreparedQueryObject queryObject = new PreparedQueryObject();
			String selectQuery = "SELECT guard FROM " + keyspace + "." + table + " WHERE key=?;";

			queryObject.addValue(lockName);
			queryObject.appendQueryString(selectQuery);
			ResultSet gqResult = dsHandle.executeOneConsistencyGet(queryObject);
			List<Row> latestGuardRow = gqResult.all();

			long prevGuard = 0;
			long lockRef = 1;
			if (latestGuardRow.size() > 0) {
				prevGuard = latestGuardRow.get(0).getLong(0);
				lockRef = prevGuard + 1;
			}

			long lockEpochMillis = System.currentTimeMillis();

//        System.out.println("guard(" + lockName + "): " + prevGuard + "->" + lockRef);
			logger.info(EELFLoggerDelegate.applicationLogger,
					"Created lock reference for " + keyspace + "." + table + "." + lockName + ":" + lockRef);

			queryObject = new PreparedQueryObject();
			String insQuery = "BEGIN BATCH" +
					" UPDATE " + keyspace + "." + table +
					" SET guard=? WHERE key=? IF guard = " + (prevGuard == 0 ? "NULL" : "?") + ";" +
					" INSERT INTO " + keyspace + "." + table +
					"(key, lockReference, createTime, acquireTime, writeLock) VALUES (?,?,?,?,?) IF NOT EXISTS; APPLY BATCH;";

			queryObject.addValue(lockRef);
			queryObject.addValue(lockName);
			if (prevGuard != 0)
				queryObject.addValue(prevGuard);

			queryObject.addValue(lockName);
			queryObject.addValue(lockRef);
			queryObject.addValue(String.valueOf(lockEpochMillis));
			queryObject.addValue("0");
	        queryObject.addValue(isWriteLock);
			queryObject.appendQueryString(insQuery);
			boolean pResult = dsHandle.executePut(queryObject, "critical");
			return String.valueOf(lockRef);
		}
		finally {
			TimeMeasureInstance.instance().exit();
		}
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
	 * @return the lock reference.
	 * @throws MusicServiceException
	 * @throws MusicQueryException
	 */
	public LockObject peekLockQueue(String keyspace, String table, String key)
			throws MusicServiceException, MusicQueryException {
		TimeMeasureInstance.instance().enter("peekLockQueue");
		try {
			logger.info(EELFLoggerDelegate.applicationLogger,
					"Peek in lock table for " +  keyspace+"."+table+"."+key);
	        table = table_prepend_name+table; 
			String selectQuery = "select * from "+keyspace+"."+table+" where key='"+key+"' LIMIT 1;";
			PreparedQueryObject queryObject = new PreparedQueryObject();
			queryObject.appendQueryString(selectQuery);
			ResultSet results = dsHandle.executeOneConsistencyGet(queryObject);
			Row row = results.one();
			if (row == null)
			    return null;
			String lockReference = "" + row.getLong("lockReference");
			String createTime = row.getString("createTime");
			String acquireTime = row.getString("acquireTime");

			return new LockObject(lockReference, createTime, acquireTime);
		}
		finally {
			TimeMeasureInstance.instance().exit();
		}
	}
	
    public boolean isTopOfLockQueue(String keyspace, String table, String key, String lockRef)
            throws MusicServiceException, MusicQueryException {
        logger.info(EELFLoggerDelegate.applicationLogger,
                "Checking in lock table for " + keyspace + "." + table + "." + key);
        table = table_prepend_name + table;
        String selectQuery =
                "select * from " + keyspace + "." + table + " where key='" + key + "';";
        PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString(selectQuery);
        ResultSet rs = dsHandle.executeOneConsistencyGet(queryObject);

        boolean topOfQueue = true;
        for (Row row : rs) {
            if (row.getBool("writeLock")) {
                if (topOfQueue && lockRef.equals("" + row.getLong("lockReference"))) {
                    return true;
                } else {
                    return false;
                }
            }
            if (lockRef.equals("" + row.getLong("lockReference"))) {
                return true;
            }
            topOfQueue = false;
        }
        logger.info(EELFLoggerDelegate.applicationLogger, "Could not find " + lockRef
                + " in the lock queue. It has expired and no longer exists.");
        return false;
    }
	
	/**
	 * This method removes the lock ref from the lock table/queue for the key. 
	 * @param keyspace of the application. 
	 * @param table of the application. 
	 * @param key is the primary key of the application table
	 * @param lockReference the lock reference that needs to be dequeued.
	 * @throws MusicServiceException
	 * @throws MusicQueryException
	 */	
	public void deQueueLockRef(String keyspace, String table, String key, String lockReference) throws MusicServiceException, MusicQueryException{
		TimeMeasureInstance.instance().enter("deQueueLockRef");
		try {
			   table = table_prepend_name+table; 
			PreparedQueryObject queryObject = new PreparedQueryObject();
			Long lockReferenceL = Long.parseLong(lockReference);
			String deleteQuery = "delete from "+keyspace+"."+table+" where key='"+key+"' AND lockReference ="+lockReferenceL+" IF EXISTS;";
			queryObject.appendQueryString(deleteQuery);
			dsHandle.executePut(queryObject, "critical");
		}
		finally {
			TimeMeasureInstance.instance().exit();
		}
	}
	

	public void updateLockAcquireTime(String keyspace, String table, String key, String lockReference) throws MusicServiceException, MusicQueryException{
        TimeMeasureInstance.instance().enter("updateLockAcquireTime");
        try {
			table = table_prepend_name+table;
            PreparedQueryObject queryObject = new PreparedQueryObject();
            Long lockReferenceL = Long.parseLong(lockReference);
            String updateQuery = "update "+keyspace+"."+table+" set acquireTime='"+ System.currentTimeMillis()+"' where key='"+key+"' AND lockReference = "+lockReferenceL+";";
            queryObject.appendQueryString(updateQuery);
            dsHandle.executePut(queryObject, "eventual");
        }
        finally {
            TimeMeasureInstance.instance().exit();
        }
	}
	

}
