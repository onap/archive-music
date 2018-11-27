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

import java.util.ArrayList;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.mockito.Mock;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicCore;
import org.onap.music.datastore.CassaDataStore;
import org.onap.music.datastore.CassaLockStore;
import org.onap.music.datastore.PreparedQueryObject;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MusicLockStoreTest {

    static CassaDataStore dataStore;
    static CassaLockStore lockStore;
    static PreparedQueryObject testObject;

    @BeforeClass
    public static void init() throws MusicServiceException, MusicQueryException {
        dataStore = CassandraCQL.connectToEmbeddedCassandra();
        lockStore = new CassaLockStore(dataStore);

        boolean result = false;
        testObject = new PreparedQueryObject();
        testObject.appendQueryString(CassandraCQL.createKeySpace);
        result = dataStore.executePut(testObject, "eventual");
        testObject = new PreparedQueryObject();
        /*
        testObject.appendQueryString(CassandraCQL.createTableEmployees);
        MusicCore.createTable(CassandraCQL.keyspace, CassandraCQL.table, testObject, "eventual");
        assertEquals(true, result);
        */
    }

    @AfterClass
    public static void close() throws MusicServiceException, MusicQueryException {
        testObject = new PreparedQueryObject();
        testObject.appendQueryString(CassandraCQL.dropKeyspace);
        dataStore.executePut(testObject, "eventual");
        dataStore.close();

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
    	ResultSet rs = lockStore.getLockQueue(CassandraCQL.keyspace, CassandraCQL.table, "test");
    	ArrayList<Object> queue = new ArrayList<>();
    	for (Row row: rs) {
    		System.out.println(row);
    		queue.add(row);   		
    	}
        assertEquals(1, queue.size());
        
        //add more locks
        for (int i=0; i<20; i++) {
        	lockStore.genLockRefandEnQueue(CassandraCQL.keyspace,  CassandraCQL.table, "test");
	    	for (int j=0; j<10; j++) {
	    		rs = lockStore.getLockQueue(CassandraCQL.keyspace, CassandraCQL.table, "test");
	    		queue = new ArrayList<>();
	    		for (Row row: rs) {
	    			queue.add(row);
	    		}
	    	}
        }
    	assertEquals(21, queue.size());
    }
}
