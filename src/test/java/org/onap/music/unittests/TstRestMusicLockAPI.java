/*
 * ============LICENSE_START========================================== org.onap.music
 * =================================================================== Copyright (c) 2017 AT&T
 * Intellectual Property ===================================================================
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
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

import javax.ws.rs.core.Response;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mindrot.jbcrypt.BCrypt;
import org.mockito.junit.MockitoJUnitRunner;
import org.onap.music.datastore.MusicDataStoreHandle;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.datastore.jsonobjects.JsonInsert;
import org.onap.music.datastore.jsonobjects.JsonLeasedLock;
import org.onap.music.datastore.jsonobjects.JsonTable;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.lockingservice.cassandra.CassaLockStore;
import org.onap.music.main.CachingUtil;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import org.onap.music.rest.RestMusicDataAPI;
import org.onap.music.rest.RestMusicLocksAPI;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.sun.jersey.core.util.Base64;

@RunWith(MockitoJUnitRunner.class)
public class TstRestMusicLockAPI {

	RestMusicLocksAPI lock = new RestMusicLocksAPI();
	RestMusicDataAPI data = new RestMusicDataAPI();
	static PreparedQueryObject testObject;

	static String appName = "TestApp";
	static String userId = "TestUser";
	static String password = "TestPassword";
	static String authData = userId+":"+password;
	static String wrongAuthData = userId+":"+"pass";
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
	public void afterEachTest( ) throws MusicServiceException {
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
    	Response response =lock.createLockReference(lockName,"1","1",authorization,
    			"abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName);
		Map<String,Object> respMap = (Map<String, Object>) response.getEntity();
    	System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());

    	assertEquals(200, response.getStatus());
    	assertTrue(respMap.containsKey("lock"));
    	assertTrue(((Map<String,String>) respMap.get("lock")).containsKey("lock"));
    }

    @Test
    public void test_accquireLock() throws Exception {
    	System.out.println("Testing acquire lock");
		createAndInsertIntoTable();
    	String lockRef = createLockReference();

    	Response response = lock.accquireLock(lockRef, "1", "1", authorization,
    			"abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName);
    	System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
    	assertEquals(200, response.getStatus());
    }
    
    @Test
    public void test_accquireLockWLease() throws Exception {
        System.out.println("Testing acquire lock with lease");
        createAndInsertIntoTable();
        String lockRef = createLockReference();

        JsonLeasedLock jsonLock = new JsonLeasedLock();
        jsonLock.setLeasePeriod(10000); //10 second lease period?
        Response response = lock.accquireLockWithLease(jsonLock, lockRef, "1", "1", authorization,
                "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        assertEquals(200, response.getStatus());
    }
    
    @Test
    public void test_accquireBadLock() throws Exception {
    	System.out.println("Testing acquire lock that is not lock-holder");
		createAndInsertIntoTable();

    	String lockRef1 = createLockReference();
    	String lockRef2 = createLockReference();


    	Response response = lock.accquireLock(lockRef2, "1", "1", authorization,
    			"abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName);
    	System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
    	assertEquals(400, response.getStatus());
    }
    
    @Test
    public void test_currentLockHolder() throws Exception {
    	System.out.println("Testing get current lock holder");
		createAndInsertIntoTable();

    	String lockRef = createLockReference();

    	Response response = lock.currentLockHolder(lockName, "1", "1", authorization,
    			"abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName);
    	System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
    	assertEquals(200, response.getStatus());
    	Map<String,Object> respMap = (Map<String, Object>) response.getEntity();
    	assertEquals(lockRef, ((Map<String,String>) respMap.get("lock")).get("lock-holder"));
    }
    
    @Test
    public void test_unLock() throws Exception {
    	System.out.println("Testing unlock");
		createAndInsertIntoTable();
    	String lockRef = createLockReference();

    	Response response = lock.unLock(lockRef, "1", "1", authorization,
    			"abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName);
    	System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
    	assertEquals(200, response.getStatus());
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
	 * @return the lock ref created
	 * @throws Exception 
	 */
	@SuppressWarnings("unchecked")
	private String createLockReference() throws Exception {
    	Response response =lock.createLockReference(lockName,"1","1",authorization,
    			"abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName);
    	Map<String,Object> respMap = (Map<String, Object>) response.getEntity();
		return ((Map<String,String>) respMap.get("lock")).get("lock");
	}

	
	

	private static void createKeyspace() throws Exception {
		//shouldn't really be doing this here, but create keyspace is currently turned off
		PreparedQueryObject query = new PreparedQueryObject();
		query.appendQueryString(CassandraCQL.createKeySpace);
		MusicCore.eventualPut(query);
		
		boolean isAAF = false;
        String hashedpwd = BCrypt.hashpw(password, BCrypt.gensalt());
        query = new PreparedQueryObject();
        query.appendQueryString(
                    "INSERT into admin.keyspace_master (uuid, keyspace_name, application_name, is_api, "
                                    + "password, username, is_aaf) values (?,?,?,?,?,?,?)");
        query.addValue(MusicUtil.convertToActualDataType(DataType.uuid(), uuid));
        query.addValue(MusicUtil.convertToActualDataType(DataType.text(), keyspaceName));
        query.addValue(MusicUtil.convertToActualDataType(DataType.text(), appName));
        query.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(), "True"));
        query.addValue(MusicUtil.convertToActualDataType(DataType.text(), hashedpwd));
        query.addValue(MusicUtil.convertToActualDataType(DataType.text(), userId));
        query.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(), isAAF));
        CachingUtil.updateMusicCache(keyspaceName, appName);
        CachingUtil.updateMusicValidateCache(appName, userId, hashedpwd);
        MusicCore.eventualPut(query);
	}
	
	private void clearAllTablesFromKeyspace() throws MusicServiceException {
		ArrayList<String> tableNames = new ArrayList<>();
		PreparedQueryObject query = new PreparedQueryObject();
		query.appendQueryString("SELECT table_name FROM system_schema.tables WHERE keyspace_name = '"+keyspaceName+"';");
		ResultSet rs = MusicCore.get(query);
		for (Row row: rs) {
			tableNames.add(row.getString("table_name"));
		}
		for (String table: tableNames) {
			query = new PreparedQueryObject();
			query.appendQueryString("DROP TABLE " + keyspaceName + "." + table);
			MusicCore.eventualPut(query);
		}
	}
	
	/**
	 * Create a table {@link tableName} in {@link keyspaceName}
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
		//Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
		Response response = data.createTable("1", "1", "1",
				"abc66ccc-d857-4e90-b1e5-df98a3d40ce6",appName, authorization, 
				jsonTable, keyspaceName, tableName);
	}
	
	/**
	 * Create table {@link createTable} and insert into said table
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
		Response response = data.insertIntoTable("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
				appName, authorization, jsonInsert, keyspaceName, tableName);
	}
}
