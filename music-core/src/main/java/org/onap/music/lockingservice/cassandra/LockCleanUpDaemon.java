/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2019 AT&T Intellectual Property
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

import java.util.HashSet;
import java.util.Set;
import org.onap.music.datastore.MusicDataStoreHandle;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class LockCleanUpDaemon extends Thread {
    
    boolean terminated = false;
    private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(LockCleanUpDaemon.class);

    
    public LockCleanUpDaemon() {
    }
    
    @Override
    public void run() {
        if (MusicUtil.getLockDaemonSleepTimeMs()<0) {
            terminate();
        }
        while (!terminated) {
            try {
                cleanupStaleLocks();
            } catch (MusicServiceException e) {
                logger.warn(EELFLoggerDelegate.applicationLogger, "Unable to clean up locks", e);
            }
            try {
                Thread.sleep(MusicUtil.getLockDaemonSleepTimeMs());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void cleanupStaleLocks() throws MusicServiceException {
        Set<String> lockQTables = getLockQTables();
        logger.info(EELFLoggerDelegate.applicationLogger, "Lock q tables found: " + lockQTables);
        for(String lockTable: lockQTables) {
            try {
                cleanUpLocksFromTable(lockTable);
            } catch (MusicServiceException e) {
                logger.warn(EELFLoggerDelegate.applicationLogger, "Unable to clear locks on table " + lockTable, e);
            }
        }
    }


    private Set<String> getLockQTables() throws MusicServiceException {
        Set<String> keyspacesToCleanUp = MusicUtil.getKeyspacesToCleanLocks();
        Set<String> lockQTables = new HashSet<>();
        
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString("SELECT keyspace_name, table_name FROM system_schema.tables;");
        ResultSet results = MusicCore.get(query);
        
        for (Row row: results) {
            if (keyspacesToCleanUp.contains(row.getString("keyspace_name"))
                    && row.getString("table_name").toLowerCase().startsWith(CassaLockStore.table_prepend_name.toLowerCase()) ) {
                lockQTables.add(row.getString("keyspace_name") + "." + row.getString("table_name"));
            }
        }
        return lockQTables;
    }

    private void cleanUpLocksFromTable(String lockTable) throws MusicServiceException {
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString("SELECT * from " + lockTable);
        ResultSet results = MusicCore.get(query);
        for (Row lock: results) {
            if (!lock.isNull("lockreference")) {
                try {
                    deleteLockIfStale(lockTable, lock);
                } catch (MusicServiceException e) {
                    logger.warn(EELFLoggerDelegate.applicationLogger, "Unable to delete a potentially stale lock " + lock, e);
                }
            }
        }
    }
    
    
    private void deleteLockIfStale(String lockTable, Row lock) throws MusicServiceException {
        if (lock.isNull("createtime") && lock.isNull("acquiretime")) {
            return;
        }

        long createTime = lock.isNull("createtime") ? 0 : Long.parseLong(lock.getString("createtime"));
        long acquireTime = lock.isNull("acquiretime") ? 0 : Long.parseLong(lock.getString("acquiretime"));
        long row_access_time = Math.max(createTime, acquireTime);
        if (System.currentTimeMillis() > row_access_time + MusicUtil.getDefaultLockLeasePeriod()) {
            logger.info(EELFLoggerDelegate.applicationLogger, "Stale lock detected and being removed: " + lock);
            PreparedQueryObject query = new PreparedQueryObject();
            query.appendQueryString("DELETE FROM " + lockTable + " WHERE key='" + lock.getString("key") + "' AND " +
                    "lockreference=" + lock.getLong("lockreference") + " IF EXISTS;");
            MusicDataStoreHandle.getDSHandle().getSession().execute(query.getQuery());
        }
    }

    public void terminate() {
        terminated = true;
    }
}
