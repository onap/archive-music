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
package org.onap.music.unittests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.lockingservice.cassandra.CassaLockStore;
import org.onap.music.datastore.MusicDataStore;
import org.onap.music.datastore.PreparedQueryObject;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MusicLockStoreTest {

	static MusicDataStore dataStore;
    static CassaLockStore lockStore;
    static PreparedQueryObject testObject;

    @BeforeClass
    public static void init() throws MusicServiceException, MusicQueryException {
        dataStore = CassandraCQL.connectToEmbeddedCassandra();
        lockStore = new CassaLockStore(dataStore);

        testObject = new PreparedQueryObject();
        testObject.appendQueryString(CassandraCQL.createKeySpace);
        dataStore.executePut(testObject, "eventual");
        testObject = new PreparedQueryObject();
    }

    @AfterClass
    public static void close() throws MusicServiceException, MusicQueryException {
        testObject = new PreparedQueryObject();
        testObject.appendQueryString(CassandraCQL.dropKeyspace);
        dataStore.executePut(testObject, "eventual");
        dataStore.close();

    }

    @Before
    public void beforeEachTest() throws MusicServiceException, MusicQueryException {
        testObject = new PreparedQueryObject();
        testObject.appendQueryString(CassandraCQL.createKeySpace);
        dataStore.executePut(testObject, "eventual");
        testObject = new PreparedQueryObject();
    }
    
    @After
    public void afterEachTest() throws MusicServiceException, MusicQueryException {
        testObject = new PreparedQueryObject();
        testObject.appendQueryString(CassandraCQL.dropKeyspace);
        dataStore.executePut(testObject, "eventual");
        testObject = new PreparedQueryObject();
    }

    
    @Test
    public void Test_createLockQueue() throws MusicServiceException, MusicQueryException {
     	boolean result = lockStore.createLockQueue(CassandraCQL.keyspace,CassandraCQL.table);
        assertEquals(true, result);
    }

    @Test
    public void Test_testGetLockQueue() throws MusicServiceException, MusicQueryException {
    	lockStore.createLockQueue(CassandraCQL.keyspace, CassandraCQL.table);
    	String lockRef = lockStore.genLockRefandEnQueue(CassandraCQL.keyspace,  CassandraCQL.table, "test");
    	List<String> lockRefs = lockStore.getLockQueue(CassandraCQL.keyspace, CassandraCQL.table, "test");
    	
        assertEquals(1, lockRefs.size());
        assertEquals(lockRef, lockRefs.get(0));
        
        //add more locks
        for (int i=0; i<20; i++) {
        	lockStore.genLockRefandEnQueue(CassandraCQL.keyspace,  CassandraCQL.table, "test");
        }
        lockRefs = lockStore.getLockQueue(CassandraCQL.keyspace, CassandraCQL.table, "test");
    	assertEquals(21, lockRefs.size());
    	assertEquals(lockRef, lockRefs.get(0));
    }
   
    
    @Test
    public void Test_testGetLockQueueSize() throws MusicServiceException, MusicQueryException {
    	lockStore.createLockQueue(CassandraCQL.keyspace, CassandraCQL.table);
    	String lockRef = lockStore.genLockRefandEnQueue(CassandraCQL.keyspace,  CassandraCQL.table, "test");
    	assertEquals(1, lockStore.getLockQueueSize(CassandraCQL.keyspace, CassandraCQL.table, "test"));
        
        //add more locks
        for (int i=0; i<20; i++) {
        	lockStore.genLockRefandEnQueue(CassandraCQL.keyspace,  CassandraCQL.table, "test");
        }
        assertEquals(21, lockStore.getLockQueueSize(CassandraCQL.keyspace, CassandraCQL.table, "test"));
    }
}
