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
import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mindrot.jbcrypt.BCrypt;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.datastore.jsonobjects.JsonInsert;
import org.onap.music.datastore.jsonobjects.JsonLeasedLock;
import org.onap.music.datastore.jsonobjects.JsonLock;
import org.onap.music.datastore.jsonobjects.JsonTable;
import org.onap.music.datastore.jsonobjects.JsonUpdate;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.lockingservice.cassandra.CassaLockStore;
import org.onap.music.lockingservice.cassandra.LockType;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import org.onap.music.rest.RestMusicDataAPI;
import org.onap.music.rest.RestMusicLocksAPI;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.sun.jersey.core.util.Base64;
import com.sun.jersey.core.util.MultivaluedMapImpl;

@RunWith(MockitoJUnitRunner.class)
public class TstRestMusicLockAPI {


    @Mock
    UriInfo info;

    RestMusicLocksAPI lock = new RestMusicLocksAPI();
    RestMusicDataAPI data = new RestMusicDataAPI();
    static PreparedQueryObject testObject;

    static String appName = "TestApp";
    static String userId = "TestUser";
    static String password = "TestPassword";
    static String authData = userId + ":" + password;
    static String wrongAuthData = userId + ":" + "pass";
    static String authorization = new String(Base64.encode(authData.getBytes()));
    static String wrongAuthorization = new String(Base64.encode(wrongAuthData.getBytes()));
    static boolean isAAF = false;
    static UUID uuid = UUID.fromString("abc66ccc-d857-4e90-b1e5-df98a3d40ce6");
    static String keyspaceName = "testcassa";
    static String tableName = "employees";
    static String onboardUUID = null;
    static String lockName = "testcassa.employees.testname";

    @BeforeClass
    public static void init() throws Exception {
        System.out.println("Testing RestMusicLock class");
        try {
            createKeyspace();
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("Unable to initialize before TestRestMusicData test class. " + e.getMessage());
        }
    }
    
    @After
    public void afterEachTest() throws MusicServiceException {
        clearAllTablesFromKeyspace();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        testObject = new PreparedQueryObject();
        testObject.appendQueryString("DROP KEYSPACE IF EXISTS " + keyspaceName);
        MusicCore.eventualPut(testObject);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test_createLockReference() throws Exception {
        System.out.println("Testing create lockref");
        createAndInsertIntoTable();
        Response response = lock.createLockReference(lockName, "1", "1", authorization,
                "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", null, null, appName);
        Map<String, Object> respMap = (Map<String, Object>) response.getEntity();
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());

        assertEquals(200, response.getStatus());
        assertTrue(respMap.containsKey("lock"));
        assertTrue(((Map<String, String>) respMap.get("lock")).containsKey("lock"));
    }
    
    @Test
    public void test_createBadLockReference() throws Exception {
        System.out.println("Testing create bad lockref");
        createAndInsertIntoTable();
        Response response = lock.createLockReference("badlock", "1", "1", authorization,
                "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", null, null, appName);
        Map<String, Object> respMap = (Map<String, Object>) response.getEntity();
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());

        assertEquals(400, response.getStatus());
    }

    @Test
    public void test_createReadLock() throws Exception {
        System.out.println("Testing create read lockref");
        createAndInsertIntoTable();
        JsonLock jsonLock = createJsonLock(LockType.READ);
        Response response = lock.createLockReference(lockName, "1", "1", authorization,
                "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", jsonLock, null, appName);
        Map<String, Object> respMap = (Map<String, Object>) response.getEntity();
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());

        assertEquals(200, response.getStatus());
        assertTrue(respMap.containsKey("lock"));
        assertTrue(((Map<String, String>) respMap.get("lock")).containsKey("lock"));
    }

    @Test
    public void test_createWriteLock() throws Exception {
        System.out.println("Testing create read lockref");
        createAndInsertIntoTable();
        JsonLock jsonLock = createJsonLock(LockType.WRITE);
        Response response = lock.createLockReference(lockName, "1", "1", authorization,
                "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", jsonLock, null, appName);
        Map<String, Object> respMap = (Map<String, Object>) response.getEntity();
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());

        assertEquals(200, response.getStatus());
        assertTrue(respMap.containsKey("lock"));
        assertTrue(((Map<String, String>) respMap.get("lock")).containsKey("lock"));
    }

    @Test
    public void test_accquireLock() throws Exception {
        System.out.println("Testing acquire lock");
        createAndInsertIntoTable();
        String lockRef = createLockReference();

        Response response =
                lock.accquireLock(lockRef, "1", "1", authorization, "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        assertEquals(200, response.getStatus());
    }

    @Test
    public void test_acquireReadLock() throws Exception {
        System.out.println("Testing acquire read lock");
        createAndInsertIntoTable();
        String lockRef = createLockReference(LockType.READ);
        String lockRef2 = createLockReference(LockType.READ);

        Response response =
                lock.accquireLock(lockRef, "1", "1", authorization, "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        assertEquals(200, response.getStatus());
        response =
                lock.accquireLock(lockRef2, "1", "1", authorization, "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        assertEquals(200, response.getStatus());
    }
    
    @Test
    public void test_acquireReadLockaFail() throws Exception {
        System.out.println("Testing acquire read lock");
        createAndInsertIntoTable();
        String lockRef = createLockReference(LockType.WRITE);
        String lockRef2 = createLockReference(LockType.READ);

        Response response =
                lock.accquireLock(lockRef, "1", "1", authorization, "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        assertEquals(200, response.getStatus());
        response =
                lock.accquireLock(lockRef2, "1", "1", authorization, "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        assertEquals(400, response.getStatus());
    }

    @Test
    public void test_writeWReadLock() throws Exception {
        System.out.println("Testing writing with a read lock");
        createAndInsertIntoTable();
        String lockRef = createLockReference(LockType.READ);

        JsonUpdate jsonUpdate = new JsonUpdate();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> values = new HashMap<>();
        values.put("emp_salary", 2500);
        consistencyInfo.put("type", "critical");
        consistencyInfo.put("lockId", lockRef);
        jsonUpdate.setConsistencyInfo(consistencyInfo);
        jsonUpdate.setKeyspaceName(keyspaceName);
        jsonUpdate.setTableName(tableName);
        jsonUpdate.setValues(values);
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        row.add("emp_name", "testname");
        Mockito.when(info.getQueryParameters()).thenReturn(row);

        Response response = data.updateTable("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName,
                authorization, jsonUpdate, keyspaceName, tableName, info);

        assertEquals(400, response.getStatus());
    }

    @Test
    public void test_writeWWriteLock() throws Exception {
        System.out.println("Testing writing with a read lock");
        createAndInsertIntoTable();
        String lockRef = createLockReference(LockType.WRITE);

        JsonUpdate jsonUpdate = new JsonUpdate();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> values = new HashMap<>();
        values.put("emp_salary", 2500);
        consistencyInfo.put("type", "critical");
        consistencyInfo.put("lockId", lockRef);
        jsonUpdate.setConsistencyInfo(consistencyInfo);
        jsonUpdate.setKeyspaceName(keyspaceName);
        jsonUpdate.setTableName(tableName);
        jsonUpdate.setValues(values);
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        row.add("emp_name", "testname");
        Mockito.when(info.getQueryParameters()).thenReturn(row);

        Response response = data.updateTable("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName,
                authorization, jsonUpdate, keyspaceName, tableName, info);

        assertEquals(200, response.getStatus());
    }

    @Test
    public void test_accquireLockWLease() throws Exception {
        System.out.println("Testing acquire lock with lease");
        createAndInsertIntoTable();
        String lockRef = createLockReference();

        JsonLeasedLock jsonLock = new JsonLeasedLock();
        jsonLock.setLeasePeriod(10000); // 10 second lease period?
        Response response = lock.accquireLockWithLease(jsonLock, lockRef, "1", "1", authorization,
                "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        assertEquals(200, response.getStatus());
    }
    
    @Test
    public void test_accquireBadLockWLease() throws Exception {
        System.out.println("Testing acquire bad lock ref with lease");
        createAndInsertIntoTable();
        String lockRef = createLockReference();

        JsonLeasedLock jsonLock = new JsonLeasedLock();
        jsonLock.setLeasePeriod(10000); // 10 second lease period?
        Response response = lock.accquireLockWithLease(jsonLock, "badlock", "1", "1", authorization,
                "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        assertEquals(400, response.getStatus());
    }
    
    @Test
    public void test_accquireBadLock() throws Exception {
        System.out.println("Testing acquire lock that is not lock-holder");
        createAndInsertIntoTable();
        // This is required to create an initial loc reference.
        String lockRef1 = createLockReference();
        // This will create the next lock reference, whcih will not be avalale yet.
        String lockRef2 = createLockReference();

        Response response = lock.accquireLock(lockRef2, "1", "1", authorization,
                "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        assertEquals(400, response.getStatus());
    }
    
    @Test
    public void test_accquireBadLockRef() throws Exception {
        System.out.println("Testing acquire bad lock ref");
        createAndInsertIntoTable();
        // This is required to create an initial loc reference.
        String lockRef1 = createLockReference();

        Response response = lock.accquireLock("badlockref", "1", "1", authorization,
                "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        assertEquals(400, response.getStatus());
    }
    
    @Test
    public void test_currentLockHolder() throws Exception {
        System.out.println("Testing get current lock holder");
        createAndInsertIntoTable();

        String lockRef = createLockReference();

        Response response =
                lock.enquireLock(lockName, "1", "1", authorization, "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        assertEquals(200, response.getStatus());
        Map<String, Object> respMap = (Map<String, Object>) response.getEntity();
        assertEquals(lockRef, ((Map<String, String>) respMap.get("lock")).get("lock-holder"));
    }
    
    @Test
    public void test_nocurrentLockHolder() throws Exception {
        System.out.println("Testing get current lock holder w/ bad lockref");
        createAndInsertIntoTable();

        Response response =
                lock.enquireLock(lockName, "1", "1", authorization, "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        assertEquals(400, response.getStatus());
    }
    
    @Test
    public void test_badcurrentLockHolder() throws Exception {
        System.out.println("Testing get current lock holder w/ bad lockref");
        createAndInsertIntoTable();

        String lockRef = createLockReference();

        Response response =
                lock.enquireLock("badlock", "1", "1", authorization, "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        assertEquals(400, response.getStatus());
    }
    
    @Test
    public void test_holders() throws Exception {
        System.out.println("Testing holders api");
        createAndInsertIntoTable();

        String lockRef = createLockReference();
        
        Response response =
                lock.currentLockHolder(lockName, "1", "1", authorization, "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        assertEquals(200, response.getStatus());
        Map<String, Object> respMap = (Map<String, Object>) response.getEntity();
        assertEquals(lockRef, ((Map<String, List>) respMap.get("lock")).get("lock-holder").get(0));
    }
    
    @Test
    public void test_holdersbadRef() throws Exception {
        System.out.println("Testing holders api w/ bad lockref");
        createAndInsertIntoTable();

        String lockRef = createLockReference();
        
        Response response =
                lock.currentLockHolder("badname", "1", "1", authorization, "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        assertEquals(400, response.getStatus());
    }
    
    @Test
    public void test_unLock() throws Exception {
        System.out.println("Testing unlock");
        createAndInsertIntoTable();
        String lockRef = createLockReference();

        Response response =
                lock.unLock(lockRef, "1", "1", authorization, "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        assertEquals(200, response.getStatus());
    }
    
    @Test
    public void test_unLockBadRef() throws Exception {
        System.out.println("Testing unlock w/ bad lock ref");
        createAndInsertIntoTable();
        String lockRef = createLockReference();

        Response response =
                lock.unLock("badref", "1", "1", authorization, "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        assertEquals(400, response.getStatus());
    }
    
    @Test
    public void test_getLockState() throws Exception {
        System.out.println("Testing get lock state");
        createAndInsertIntoTable();

        String lockRef = createLockReference();

        Response response = lock.currentLockState(lockName, "1", "1", authorization,
                "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        assertEquals(200, response.getStatus());
        Map<String,Object> respMap = (Map<String, Object>) response.getEntity();
        assertEquals(lockRef, ((Map<String,String>) respMap.get("lock")).get("lock-holder"));
    }
    
    @Test
    public void test_getLockStateBadRef() throws Exception {
        System.out.println("Testing get lock state w/ bad ref");
        createAndInsertIntoTable();

        String lockRef = createLockReference();

        Response response = lock.currentLockState("badname", "1", "1", authorization,
                "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        assertEquals(400, response.getStatus());
        }

        @SuppressWarnings("unchecked")
        @Test
        public void test_deadlock() throws Exception {
        System.out.println("Testing deadlock");
        createAndInsertIntoTable();
        insertAnotherIntoTable();

        // Process 1 creates and acquires a lock on row 1
        JsonLock jsonLock = createJsonLock(LockType.WRITE);
        Response responseCreate1 = lock.createLockReference(lockName, "1", "1", authorization,
                "abcde001-d857-4e90-b1e5-df98a3d40ce6", jsonLock, "process1", appName);
        Map<String, Object> respMapCreate1 = (Map<String, Object>) responseCreate1.getEntity();
        String lockRefCreate1 = ((Map<String, String>) respMapCreate1.get("lock")).get("lock");

        Response responseAcquire1 =
                lock.accquireLock(lockRefCreate1, "1", "1", authorization, "abc66001-d857-4e90-b1e5-df98a3d40ce6", appName);

        // Process 2 creates and acquires a lock on row 2
        Response responseCreate2 = lock.createLockReference(lockName + "2", "1", "1", authorization,
                "abcde002-d857-4e90-b1e5-df98a3d40ce6", jsonLock, "process2", appName);
        Map<String, Object> respMapCreate2 = (Map<String, Object>) responseCreate2.getEntity();
        String lockRefCreate2 = ((Map<String, String>) respMapCreate2.get("lock")).get("lock");

        Response responseAcquire2 =
                lock.accquireLock(lockRefCreate2, "1", "1", authorization, "abc66002-d857-4e90-b1e5-df98a3d40ce6", appName);

        // Process 2 creates a lock on row 1
        Response responseCreate3 = lock.createLockReference(lockName, "1", "1", authorization,
                "abcde003-d857-4e90-b1e5-df98a3d40ce6", jsonLock, "process2", appName);

        // Process 1 creates a lock on row 2, causing deadlock
        Response responseCreate4 = lock.createLockReference(lockName + "2", "1", "1", authorization,
                "abcde004-d857-4e90-b1e5-df98a3d40ce6", jsonLock, "process1", appName);
        Map<String, Object> respMapCreate4 = (Map<String, Object>) responseCreate4.getEntity();

        System.out.println("Status: " + responseCreate4.getStatus() + ". Entity " + responseCreate4.getEntity());
        assertEquals(400, responseCreate4.getStatus());
        assertTrue(respMapCreate4.containsKey("error"));
        assertTrue( ((String)respMapCreate4.get("error")).toLowerCase().indexOf("deadlock") > -1 );
    }

    
    @SuppressWarnings("unchecked")
    @Test
    public void test_lockPromotion() throws Exception {
        System.out.println("Testing lock promotion");
        createAndInsertIntoTable();
        insertAnotherIntoTable();

        // creates a lock 1
        JsonLock jsonLock = createJsonLock(LockType.READ);
        Response responseCreate1 = lock.createLockReference(lockName, "1", "1", authorization,
                "abcde001-d857-4e90-b1e5-df98a3d40ce6", jsonLock, "process1", appName);
        Map<String, Object> respMapCreate1 = (Map<String, Object>) responseCreate1.getEntity();
        String lockRefCreate1 = ((Map<String, String>) respMapCreate1.get("lock")).get("lock");

        Response respMapPromote = lock.promoteLock(lockRefCreate1, "1", "1", authorization);
        System.out.println("Status: " + respMapPromote.getStatus() + ". Entity " + respMapPromote.getEntity());
        
        assertEquals(200, respMapPromote.getStatus());
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void test_lockPromotionReadWrite() throws Exception {
        System.out.println("Testing lock promotion with read and writes");
        createAndInsertIntoTable();
        insertAnotherIntoTable();

        // creates a lock 1
        JsonLock jsonLockRead = createJsonLock(LockType.READ);
        Response responseCreate1 = lock.createLockReference(lockName, "1", "1", authorization,
                "abcde001-d857-4e90-b1e5-df98a3d40ce6", jsonLockRead, "process1", appName);
        Map<String, Object> respMapCreate1 = (Map<String, Object>) responseCreate1.getEntity();
        String lockRefCreate1 = ((Map<String, String>) respMapCreate1.get("lock")).get("lock");
        
        JsonLock jsonLockWrite = createJsonLock(LockType.WRITE);
        Response responseCreate2 = lock.createLockReference(lockName, "1", "1", authorization,
                "abcde001-d857-4e90-b1e5-df98a3d40ce6", jsonLockWrite, "process1", appName);
        Map<String, Object> respMapCreate2 = (Map<String, Object>) responseCreate2.getEntity();
        String lockRefCreate2 = ((Map<String, String>) respMapCreate2.get("lock")).get("lock");

        Response respMapPromote = lock.promoteLock(lockRefCreate1, "1", "1", authorization);
        System.out.println("Status: " + respMapPromote.getStatus() + ". Entity " + respMapPromote.getEntity());
        
        assertEquals(200, respMapPromote.getStatus());
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void test_lockPromotionWriteRead() throws Exception {
        System.out.println("Testing lock promotion with reads not at top of queue");
        createAndInsertIntoTable();
        insertAnotherIntoTable();

        // creates a lock 1
        JsonLock jsonLockWrite = createJsonLock(LockType.WRITE);
        Response responseCreate2 = lock.createLockReference(lockName, "1", "1", authorization,
                "abcde001-d857-4e90-b1e5-df98a3d40ce6", jsonLockWrite, "process1", appName);
        Map<String, Object> respMapCreate2 = (Map<String, Object>) responseCreate2.getEntity();
        String lockRefCreate2 = ((Map<String, String>) respMapCreate2.get("lock")).get("lock");
        
        // creates a lock 2
        JsonLock jsonLockRead = createJsonLock(LockType.READ);
        Response responseCreate1 = lock.createLockReference(lockName, "1", "1", authorization,
                "abcde001-d857-4e90-b1e5-df98a3d40ce6", jsonLockRead, "process1", appName);
        Map<String, Object> respMapCreate1 = (Map<String, Object>) responseCreate1.getEntity();
        String lockRefCreate1 = ((Map<String, String>) respMapCreate1.get("lock")).get("lock");

        Response respMapPromote = lock.promoteLock(lockRefCreate1, "1", "1", authorization);
        System.out.println("Status: " + respMapPromote.getStatus() + ". Entity " + respMapPromote.getEntity());

        assertEquals(200, respMapPromote.getStatus());
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void test_lockPromotion2Reads() throws Exception {
        System.out.println("Testing lock promotion w/ 2 ReadLocks");
        createAndInsertIntoTable();
        insertAnotherIntoTable();

        // creates a lock 1
        JsonLock jsonLockRead = createJsonLock(LockType.READ);
        Response responseCreate1 = lock.createLockReference(lockName, "1", "1", authorization,
                "abcde001-d857-4e90-b1e5-df98a3d40ce6", jsonLockRead, "process1", appName);
        Map<String, Object> respMapCreate1 = (Map<String, Object>) responseCreate1.getEntity();
        String lockRefCreate1 = ((Map<String, String>) respMapCreate1.get("lock")).get("lock");
        
        Response responseCreate2 = lock.createLockReference(lockName, "1", "1", authorization,
                "abcde001-d857-4e90-b1e5-df98a3d40ce6", jsonLockRead, "process1", appName);
        Map<String, Object> respMapCreate2 = (Map<String, Object>) responseCreate1.getEntity();
        String lockRefCreate2 = ((Map<String, String>) respMapCreate1.get("lock")).get("lock");

        Response respMapPromote = lock.promoteLock(lockRefCreate1, "1", "1", authorization);
        System.out.println("Status: " + respMapPromote.getStatus() + ". Entity " + respMapPromote.getEntity());
        
        assertEquals(400, respMapPromote.getStatus());
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void test_2lockPromotions() throws Exception {
        System.out.println("Testing 2 lock promotions");
        createAndInsertIntoTable();
        insertAnotherIntoTable();

        // creates a lock 1
        JsonLock jsonLockRead = createJsonLock(LockType.READ);
        Response responseCreate1 = lock.createLockReference(lockName, "1", "1", authorization,
                "abcde001-d857-4e90-b1e5-df98a3d40ce6", jsonLockRead, "process1", appName);
        Map<String, Object> respMapCreate1 = (Map<String, Object>) responseCreate1.getEntity();
        String lockRefCreate1 = ((Map<String, String>) respMapCreate1.get("lock")).get("lock");
        
        Response responseCreate2 = lock.createLockReference(lockName, "1", "1", authorization,
                "abcde001-d857-4e90-b1e5-df98a3d40ce6", jsonLockRead, "process1", appName);
        Map<String, Object> respMapCreate2 = (Map<String, Object>) responseCreate2.getEntity();
        String lockRefCreate2 = ((Map<String, String>) respMapCreate2.get("lock")).get("lock");

        Response respMapPromote = lock.promoteLock(lockRefCreate1, "1", "1", authorization);
        System.out.println("Status: " + respMapPromote.getStatus() + ". Entity " + respMapPromote.getEntity());
        
        assertEquals(400, respMapPromote.getStatus());
        
        Response respMap2Promote = lock.promoteLock(lockRefCreate2, "1", "1", authorization);
        System.out.println("Status: " + respMap2Promote.getStatus() + ". Entity " + respMap2Promote.getEntity());
        
        assertEquals(400, respMapPromote.getStatus());
    }
    
    

    // Ignoring since this is now a duplicate of delete lock ref.
    @Test
    @Ignore
    public void test_deleteLock() throws Exception {
        System.out.println("Testing get lock state");
        createAndInsertIntoTable();
        
        String lockRef = createLockReference();

        Response response = lock.deleteLock(lockName, "1", "1",
                "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", authorization, appName);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        assertEquals(200, response.getStatus());
    }
    
    /**
    * Create table and lock reference
     * 
    * @return the lock ref created
     * @throws Exception
    */
    @SuppressWarnings("unchecked")
    private String createLockReference() throws Exception {
        Response response = lock.createLockReference(lockName, "1", "1", authorization,
                "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", null, null, appName);
        Map<String, Object> respMap = (Map<String, Object>) response.getEntity();
        return ((Map<String, String>) respMap.get("lock")).get("lock");
    }

    /**
     * Create table and lock reference
     * 
     * @return the lock ref created
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    private String createLockReference(LockType lockType) throws Exception {
        JsonLock jsonLock = createJsonLock(lockType);
        Response response = lock.createLockReference(lockName, "1", "1", authorization,
                "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", jsonLock, null, appName);
        Map<String, Object> respMap = (Map<String, Object>) response.getEntity();
        return ((Map<String, String>) respMap.get("lock")).get("lock");
    }

    private static void createKeyspace() throws Exception {
        // shouldn't really be doing this here, but create keyspace is currently turned off
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString(CassandraCQL.createKeySpace);
        MusicCore.eventualPut(query);
        
        boolean isAAF = false;
        String hashedpwd = BCrypt.hashpw(password, BCrypt.gensalt());
        query = new PreparedQueryObject();
        query.appendQueryString("INSERT into admin.keyspace_master (uuid, keyspace_name, application_name, is_api, "
                                    + "password, username, is_aaf) values (?,?,?,?,?,?,?)");
        query.addValue(MusicUtil.convertToActualDataType(DataType.uuid(), uuid));
        query.addValue(MusicUtil.convertToActualDataType(DataType.text(), keyspaceName));
        query.addValue(MusicUtil.convertToActualDataType(DataType.text(), appName));
        query.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(), "True"));
        query.addValue(MusicUtil.convertToActualDataType(DataType.text(), hashedpwd));
        query.addValue(MusicUtil.convertToActualDataType(DataType.text(), userId));
        query.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(), isAAF));
        //CachingUtil.updateMusicCache(keyspaceName, appName);
        //CachingUtil.updateMusicValidateCache(appName, userId, hashedpwd);
        MusicCore.eventualPut(query);
    }
    
    private void clearAllTablesFromKeyspace() throws MusicServiceException {
        ArrayList<String> tableNames = new ArrayList<>();
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString(
                "SELECT table_name FROM system_schema.tables WHERE keyspace_name = '" + keyspaceName + "';");
        ResultSet rs = MusicCore.get(query);
        for (Row row : rs) {
            tableNames.add(row.getString("table_name"));
        }
        for (String table : tableNames) {
            query = new PreparedQueryObject();
            query.appendQueryString("DROP TABLE " + keyspaceName + "." + table);
            MusicCore.eventualPut(query);
        }
    }
    
    /**
     * Create a table {@link tableName} in {@link keyspaceName}
     * 
     * @throws Exception
     */
    private void createTable() throws Exception {
        JsonTable jsonTable = new JsonTable();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_salary", "varint");
        fields.put("PRIMARY KEY", "(emp_name)");
        consistencyInfo.put("type", "eventual");
        jsonTable.setConsistencyInfo(consistencyInfo);
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPrimaryKey("emp_name");
        jsonTable.setTableName(tableName);
        jsonTable.setFields(fields);
        // Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createTable("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName,
                authorization, jsonTable, keyspaceName, tableName);
    }
    
    /**
     * Create table {@link createTable} and insert into said table
     * 
     * @throws Exception
     */
    private void createAndInsertIntoTable() throws Exception {
        createTable();
        
        JsonInsert jsonInsert = new JsonInsert();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> values = new HashMap<>();
        values.put("uuid", "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6");
        values.put("emp_name", "testname");
        values.put("emp_salary", 500);
        consistencyInfo.put("type", "eventual");
        jsonInsert.setConsistencyInfo(consistencyInfo);
        jsonInsert.setKeyspaceName(keyspaceName);
        jsonInsert.setTableName(tableName);
        jsonInsert.setValues(values);
        Response response = data.insertIntoTable("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName,
                authorization, jsonInsert, keyspaceName, tableName);
    }

    private void insertAnotherIntoTable() throws Exception {
        createTable();

        JsonInsert jsonInsert = new JsonInsert();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> values = new HashMap<>();
        values.put("uuid", "cccccccc-d857-4e90-b1e5-df98a3d40cd6");
        values.put("emp_name", "testname2");
        values.put("emp_salary", 700);
        consistencyInfo.put("type", "eventual");
        jsonInsert.setConsistencyInfo(consistencyInfo);
        jsonInsert.setKeyspaceName(keyspaceName);
        jsonInsert.setTableName(tableName);
        jsonInsert.setValues(values);
        Response response = data.insertIntoTable("1", "1", "1", "abcdef00-d857-4e90-b1e5-df98a3d40ce6", appName,
                authorization, jsonInsert, keyspaceName, tableName);
    }

    private JsonLock createJsonLock(LockType lockType) {
        JsonLock jsonLock = new JsonLock();
        jsonLock.setLockType(lockType);
        return jsonLock;
    }

}