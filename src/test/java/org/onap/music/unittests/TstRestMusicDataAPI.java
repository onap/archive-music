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
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MultivaluedHashMap;
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
import org.onap.music.datastore.MusicDataStoreHandle;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.datastore.jsonobjects.JsonDelete;
import org.onap.music.datastore.jsonobjects.JsonInsert;
import org.onap.music.datastore.jsonobjects.JsonKeySpace;
import org.onap.music.datastore.jsonobjects.JsonSelect;
import org.onap.music.datastore.jsonobjects.JsonTable;
import org.onap.music.datastore.jsonobjects.JsonUpdate;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.CachingUtil;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.rest.RestMusicDataAPI;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.sun.jersey.core.util.Base64;
import com.sun.jersey.core.util.MultivaluedMapImpl;

@RunWith(MockitoJUnitRunner.class)
public class TstRestMusicDataAPI {

	RestMusicDataAPI data = new RestMusicDataAPI();
	static PreparedQueryObject testObject;

	@Mock
	HttpServletResponse http;

	@Mock
	UriInfo info;

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
	static String xLatestVersion = "X-latestVersion";
	static String onboardUUID = null;

	@BeforeClass
	public static void init() throws Exception {
		System.out.println("Testing RestMusicData class");
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

	@Test
	public void test1_createKeyspace() throws Exception {
		System.out.println("Testing create keyspace");
		JsonKeySpace jsonKeyspace = new JsonKeySpace();
		Map<String, String> consistencyInfo = new HashMap<>();
		Map<String, Object> replicationInfo = new HashMap<>();
		consistencyInfo.put("type", "eventual");
		replicationInfo.put("class", "SimpleStrategy");
		replicationInfo.put("replication_factor", 1);
		jsonKeyspace.setConsistencyInfo(consistencyInfo);
		jsonKeyspace.setDurabilityOfWrites("true");
		jsonKeyspace.setKeyspaceName(keyspaceName);
		jsonKeyspace.setReplicationInfo(replicationInfo);
		//Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
		Response response = data.createKeySpace("1", "1", "1", null,authorization, appName,  jsonKeyspace, keyspaceName);
		System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
		assertEquals(400,response.getStatus());
		Map<String,String> respMap = (Map<String, String>) response.getEntity();
		assertEquals(ResultType.FAILURE, respMap.get("status"));
	}

	@Test
	public void test3_createTable() throws Exception {
		System.out.println("Testing create table");
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
		Response response = data.createTable("1", "1", "1",
				"abc66ccc-d857-4e90-b1e5-df98a3d40ce6",appName, authorization, 
				jsonTable, keyspaceName, tableName);
		System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
		assertEquals(200, response.getStatus());
	}


	@Test
	public void test3_createTableClusterOrderBad() throws Exception {
		System.out.println("Testing create table bad clustering");
		JsonTable jsonTable = new JsonTable();
		Map<String, String> consistencyInfo = new HashMap<>();
		Map<String, String> fields = new HashMap<>();
		fields.put("uuid", "text");
		fields.put("emp_name", "text");
		fields.put("emp_salary", "varint");
		fields.put("PRIMARY KEY", "(emp_name,emp_salary)");
		consistencyInfo.put("type", "eventual");
		jsonTable.setConsistencyInfo(consistencyInfo);
		jsonTable.setKeyspaceName(keyspaceName);
		jsonTable.setPrimaryKey("emp_name,emp_salary");
		jsonTable.setClusteringOrder("ASC");
		jsonTable.setTableName(tableName);
		jsonTable.setFields(fields);
		Response response = data.createTable("1", "1", "1",
				"abc66ccc-d857-4e90-b1e5-df98a3d40ce6",appName, authorization, 
				jsonTable, keyspaceName, tableName);
		System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
		assertEquals(400, response.getStatus());
	}

	@Test
	public void test3_createTable_withPropertiesNotNull() throws Exception {
		System.out.println("Testing create table with properties");
		JsonTable jsonTable = new JsonTable();
		Map<String, String> consistencyInfo = new HashMap<>();
		Map<String, String> fields = new HashMap<>();
		fields.put("uuid", "text");
		fields.put("emp_name", "text");
		fields.put("emp_salary", "varint");
		fields.put("PRIMARY KEY", "(emp_name)");
		consistencyInfo.put("type", "eventual");
		Map<String, Object> properties = new HashMap<>();
		properties.put("comment","Testing prperties not null");
		jsonTable.setConsistencyInfo(consistencyInfo);
		jsonTable.setKeyspaceName(keyspaceName);
		jsonTable.setPrimaryKey("emp_name");
		String tableName_prop=tableName+"_Prop";
		jsonTable.setTableName(tableName_prop);
		jsonTable.setFields(fields);
		jsonTable.setProperties(properties);

		Response response = data.createTable("1", "1", "1",
				"abc66ccc-d857-4e90-b1e5-df98a3d40ce6",appName, authorization, 
				jsonTable, keyspaceName, tableName_prop);
		System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
		assertEquals(200, response.getStatus());
	}

	@Test
	public void test3_createTable_duplicateTable() throws Exception {
		System.out.println("Testing creating duplicate tables");
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
		String tableNameDup=tableName+"x";
		jsonTable.setTableName(tableNameDup);
		jsonTable.setFields(fields);
		//Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
		Response response1 = data.createTable("1", "1", "1",
				"abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
				jsonTable, keyspaceName, tableNameDup);

		Response response2 = data.createTable("1", "1", "1",
				"abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
				jsonTable, keyspaceName, tableNameDup);
		System.out.println("Status: " + response2.getStatus() + ". Entity " + response2.getEntity());

		assertEquals(400, response2.getStatus());
		Map<String,String> respMap = (Map<String, String>) response2.getEntity();
		assertEquals(ResultType.FAILURE, respMap.get("status"));
		assertEquals("Table " + keyspaceName + "." + tableNameDup + " already exists",
				respMap.get("error"));
	}

	// Improper Auth
	@Test
	public void test3_createTable1() throws Exception {
		System.out.println("Testing create table w/ improper authentication");
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
		Response response = data.createTable("1", "1", "1",
				"abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, wrongAuthorization,
				jsonTable, keyspaceName, tableName);
		System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
		assertEquals(401, response.getStatus());
	}

	// Improper keyspace
	@Test
	public void test3_createTable3() throws Exception {
		System.out.println("Testing create table for wrong keyspace");
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
		Response response = data.createTable("1", "1", "1",
				"abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
				jsonTable, "wrong", tableName);
		System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
		assertEquals(401, response.getStatus());
	}

	// Improper parenthesis in key field
	@Test
	public void test3_createTable_badParantesis() throws Exception {
		System.out.println("Testing malformed create table request");
		String tableNameC ="testTable0";
		JsonTable jsonTable = new JsonTable();
		Map<String, String> consistencyInfo = new HashMap<>();
		Map<String, String> fields = new HashMap<>();
		fields.put("uuid", "text");
		fields.put("emp_name", "text");
		fields.put("emp_salary", "varint");
		fields.put("PRIMARY KEY", "(emp_name),emp_id)");
		fields.put("emp_id", "varint");
		consistencyInfo.put("type", "eventual");
		jsonTable.setConsistencyInfo(consistencyInfo);
		jsonTable.setKeyspaceName(keyspaceName);
		jsonTable.setPrimaryKey("emp_name");
		jsonTable.setTableName(tableNameC);
		jsonTable.setClusteringOrder("emp_id Desc");
		jsonTable.setFields(fields);
		Response response = data.createTable("1", "1", "1",
				"abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
				jsonTable, keyspaceName, tableNameC);
		System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());

		assertEquals(400, response.getStatus());
	}


	// good clustering key
	@Test
	public void test3_createTable_1_clusterKey_good() throws Exception {
		System.out.println("Testing create w/ clusterKey");

		String tableNameC ="testTableC1";
		JsonTable jsonTable = new JsonTable();
		Map<String, String> consistencyInfo = new HashMap<>();
		Map<String, String> fields = new HashMap<>();
		fields.put("uuid", "text");
		fields.put("emp_name", "text");
		fields.put("emp_salary", "varint");
		fields.put("PRIMARY KEY", "((emp_name),emp_salary)");
		consistencyInfo.put("type", "eventual");
		jsonTable.setConsistencyInfo(consistencyInfo);
		jsonTable.setKeyspaceName(keyspaceName);
		// jsonTable.setPrimaryKey("emp_name");
		jsonTable.setTableName(tableNameC);
		jsonTable.setClusteringOrder("emp_salary ASC");
		jsonTable.setFields(fields);
		Response response = data.createTable("1", "1", "1",
				"abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
				jsonTable, keyspaceName, tableNameC);
		System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());

		assertEquals(200, response.getStatus());
	}

	// bad partition key=clustering key
	@Test
	public void test3_createTable_2_clusterKey_bad() throws Exception {
		System.out.println("Testing create w/ bad clusterKey");
		String tableNameC ="testTableC2";
		JsonTable jsonTable = new JsonTable();
		Map<String, String> consistencyInfo = new HashMap<>();
		Map<String, String> fields = new HashMap<>();
		fields.put("uuid", "text");
		fields.put("emp_name", "text");
		fields.put("emp_salary", "varint");
		fields.put("PRIMARY KEY", "((emp_name),emp_name)");
		consistencyInfo.put("type", "eventual");
		jsonTable.setConsistencyInfo(consistencyInfo);
		jsonTable.setKeyspaceName(keyspaceName);
		jsonTable.setPrimaryKey("emp_name");  // "PRIMARY KEY" overrides if primaryKey present
		jsonTable.setTableName(tableNameC);
		jsonTable.setClusteringOrder("emp_salary ASC");
		jsonTable.setFields(fields);
		Response response = data.createTable("1", "1", "1",
				"abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
				jsonTable, keyspaceName, tableNameC);
		System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());

		assertEquals(400, response.getStatus());
	}

	// good composite partition key,clustering key
	@Test
	public void test3_createTable_3_pfartition_clusterKey_good() throws Exception {
		System.out.println("Testing create w/ composite partition key, clusterKey");

		String tableNameC ="testTableC3";
		JsonTable jsonTable = new JsonTable();
		Map<String, String> consistencyInfo = new HashMap<>();
		Map<String, String> fields = new HashMap<>();
		fields.put("uuid", "text");
		fields.put("emp_name", "text");
		fields.put("emp_id", "varint");
		fields.put("emp_salary", "varint");
		fields.put("PRIMARY KEY", "((emp_name,emp_id),emp_salary)");
		consistencyInfo.put("type", "eventual");
		jsonTable.setConsistencyInfo(consistencyInfo);
		jsonTable.setKeyspaceName(keyspaceName);
		jsonTable.setPrimaryKey("emp_name");
		jsonTable.setTableName(tableNameC);
		jsonTable.setClusteringOrder("emp_salary ASC");
		jsonTable.setFields(fields);
		Response response = data.createTable("1", "1", "1",
				"abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
				jsonTable, keyspaceName, tableNameC);
		System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());

		assertEquals(200, response.getStatus());
	}

	// bad - wrong cols in order by of composite partition key,clustering key
	@Test
	public void test3_createTable_5_clusteringOrder_bad() throws Exception {
		System.out.println("Testing create table bad request with clustering & composite keys");
		String tableNameC ="testTableC5";
		JsonTable jsonTable = new JsonTable();
		Map<String, String> consistencyInfo = new HashMap<>();
		Map<String, String> fields = new HashMap<>();
		fields.put("uuid", "text");
		fields.put("emp_name", "text");
		fields.put("emp_id", "varint");
		fields.put("emp_salary", "varint");
		fields.put("PRIMARY KEY", "((uuid,emp_name),emp_id,emp_salary)");
		consistencyInfo.put("type", "eventual");
		jsonTable.setConsistencyInfo(consistencyInfo);
		jsonTable.setKeyspaceName(keyspaceName);
		jsonTable.setPrimaryKey("emp_name");
		jsonTable.setTableName(tableNameC);
		jsonTable.setClusteringOrder("emp_idx desc, emp_salary ASC");
		jsonTable.setFields(fields);
		Response response = data.createTable("1", "1", "1",
				"abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
				jsonTable, keyspaceName, tableNameC);
		System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());

		assertEquals(400, response.getStatus());
	}


	// good clustering key, need to pass queryparameter
	@Test
	public void test3_createTableIndex_1() throws Exception {
		System.out.println("Testing index in create table");
		String tableNameC ="testTableCinx";
		JsonTable jsonTable = new JsonTable();
		Map<String, String> consistencyInfo = new HashMap<>();
		Map<String, String> fields = new HashMap<>();
		fields.put("uuid", "text");
		fields.put("emp_name", "text");
		fields.put("emp_salary", "varint");
		fields.put("PRIMARY KEY", "((emp_name),emp_salary)");
		consistencyInfo.put("type", "eventual");
		jsonTable.setConsistencyInfo(consistencyInfo);
		jsonTable.setKeyspaceName(keyspaceName);
		jsonTable.setTableName(tableNameC);
		jsonTable.setClusteringOrder("emp_salary ASC");
		jsonTable.setFields(fields);
		Response response = data.createTable("1", "1", "1",
				"abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
				jsonTable, keyspaceName, tableNameC);
		// if 200 print to log otherwise fail assertEquals(200, response.getStatus());
		// info.setQueryParameters("index_name=inx_uuid");
		Map<String,String> queryParametersMap =new HashMap<String, String>();

		queryParametersMap.put("index_name","inxuuid");
		Mockito.when(info.getQueryParameters()).thenReturn(new MultivaluedHashMap<String, String>(queryParametersMap));
		response = data.createIndex("1", "1", "1",
				"abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
				keyspaceName, tableNameC,"uuid",info);
		System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());

		assertEquals(200, response.getStatus());
	}

	@Test
	public void test4_insertIntoTable() throws Exception {
		System.out.println("Testing insert into table");
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
		System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());

		assertEquals(200, response.getStatus());
	}

	@Test
	public void test4_insertIntoTable2() throws Exception {
		System.out.println("Testing insert into table #2");
		createTable();
		JsonInsert jsonInsert = new JsonInsert();
		Map<String, String> consistencyInfo = new HashMap<>();
		Map<String, Object> values = new HashMap<>();
		values.put("uuid", "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6");
		values.put("emp_name", "test1");
		values.put("emp_salary", 1500);
		consistencyInfo.put("type", "eventual");
		jsonInsert.setConsistencyInfo(consistencyInfo);
		jsonInsert.setKeyspaceName(keyspaceName);
		jsonInsert.setTableName(tableName);
		jsonInsert.setValues(values);
		Response response = data.insertIntoTable("1", "1", "1",
				"abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
				jsonInsert, keyspaceName, tableName);
		System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
		
		assertEquals(200, response.getStatus());
	}

	// Auth Error
	@Test
	public void test4_insertIntoTable3() throws Exception {
		System.out.println("Testing insert into table with bad credentials");
		createTable();
		JsonInsert jsonInsert = new JsonInsert();
		Map<String, String> consistencyInfo = new HashMap<>();
		Map<String, Object> values = new HashMap<>();
		values.put("uuid", "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6");
		values.put("emp_name", "test1");
		values.put("emp_salary", 1500);
		consistencyInfo.put("type", "eventual");
		jsonInsert.setConsistencyInfo(consistencyInfo);
		jsonInsert.setKeyspaceName(keyspaceName);
		jsonInsert.setTableName(tableName);
		jsonInsert.setValues(values);
		Response response = data.insertIntoTable("1", "1", "1",
				"abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, wrongAuthorization,
				jsonInsert, keyspaceName, tableName);
		System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());

		assertEquals(401, response.getStatus());
	}

	// Table wrong
	@Test
	public void test4_insertIntoTable4() throws Exception {
		System.out.println("Testing insert into wrong table");
		createTable();
		JsonInsert jsonInsert = new JsonInsert();
		Map<String, String> consistencyInfo = new HashMap<>();
		Map<String, Object> values = new HashMap<>();
		values.put("uuid", "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6");
		values.put("emp_name", "test1");
		values.put("emp_salary", 1500);
		consistencyInfo.put("type", "eventual");
		jsonInsert.setConsistencyInfo(consistencyInfo);
		jsonInsert.setKeyspaceName(keyspaceName);
		jsonInsert.setTableName(tableName);
		jsonInsert.setValues(values);
		Response response = data.insertIntoTable("1", "1", "1",
				"abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
				jsonInsert, keyspaceName, "wrong");
		System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());

		assertEquals(400, response.getStatus());
	}


	@Test
	public void test5_updateTable() throws Exception {
		System.out.println("Testing update table");
		createTable();
		
		JsonUpdate jsonUpdate = new JsonUpdate();
		Map<String, String> consistencyInfo = new HashMap<>();
		MultivaluedMap<String, String> row = new MultivaluedMapImpl();
		Map<String, Object> values = new HashMap<>();
		row.add("emp_name", "testname");
		values.put("emp_salary", 2500);
		consistencyInfo.put("type", "atomic");
		jsonUpdate.setConsistencyInfo(consistencyInfo);
		jsonUpdate.setKeyspaceName(keyspaceName);
		jsonUpdate.setTableName(tableName);
		jsonUpdate.setValues(values);
		Mockito.when(info.getQueryParameters()).thenReturn(row);
		Response response = data.updateTable("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName,
				authorization, jsonUpdate, keyspaceName, tableName, info);
		
		System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
		
		assertEquals(200, response.getStatus());
	} 

	// need mock code to create error for MusicCore methods
	@Test
	public void test5_updateTableAuthE() throws Exception {
		System.out.println("Testing update table #2");
		createTable();
		//MockitoAnnotations.initMocks(this);
		JsonUpdate jsonUpdate = new JsonUpdate();
		Map<String, String> consistencyInfo = new HashMap<>();
		MultivaluedMap<String, String> row = new MultivaluedMapImpl();
		Map<String, Object> values = new HashMap<>();
		row.add("emp_name", "testname");
		values.put("emp_salary", 2500);
		consistencyInfo.put("type", "atomic");
		jsonUpdate.setConsistencyInfo(consistencyInfo);
		jsonUpdate.setKeyspaceName(keyspaceName);
		jsonUpdate.setTableName(tableName);
		jsonUpdate.setValues(values);
		//add ttl & timestamp
		//Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
		Mockito.when(info.getQueryParameters()).thenReturn(row);
		//Map<String, Object> m1= new HashMap<>() ;
		//Mockito.when(MusicCore.autheticateUser(appName,userId,password,keyspaceName,"abc66ccc-d857-4e90-b1e5-df98a3d40ce6","updateTable")).thenReturn(m1);
		Response response = data.updateTable("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName,
				authorization, jsonUpdate, keyspaceName, tableName, info);
		System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());

		assertEquals(200, response.getStatus());
	} 

	@Ignore
	@Test
	public void test5_updateTableAuthException1() throws Exception {
		System.out.println("Testing update table authentication error");
		createTable();
		JsonUpdate jsonUpdate = new JsonUpdate();
		Map<String, String> consistencyInfo = new HashMap<>();
		MultivaluedMap<String, String> row = new MultivaluedMapImpl();
		Map<String, Object> values = new HashMap<>();
		row.add("emp_name", "testname");
		values.put("emp_salary", 2500);
		consistencyInfo.put("type", "atomic");
		jsonUpdate.setConsistencyInfo(consistencyInfo);
		jsonUpdate.setKeyspaceName(keyspaceName);
		jsonUpdate.setTableName(tableName);
		jsonUpdate.setValues(values);
		
		Mockito.when(info.getQueryParameters()).thenReturn(row);
		String authDatax = ":";
		String authorizationx = new String(Base64.encode(authDatax.getBytes()));
		Response response = data.updateTable("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName,
				authorizationx, jsonUpdate, keyspaceName, tableName, info);
		System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());

		assertEquals(401, response.getStatus());
	}

	@Ignore
	@Test
	public void test5_updateTableAuthEmpty() throws Exception {
		System.out.println("Testing update table without authentication");
		createTable();
		
		JsonUpdate jsonUpdate = new JsonUpdate();
		Map<String, String> consistencyInfo = new HashMap<>();
		MultivaluedMap<String, String> row = new MultivaluedMapImpl();
		Map<String, Object> values = new HashMap<>();
		row.add("emp_name", "testname");
		values.put("emp_salary", 2500);
		consistencyInfo.put("type", "atomic");
		jsonUpdate.setConsistencyInfo(consistencyInfo);
		jsonUpdate.setKeyspaceName(keyspaceName);
		jsonUpdate.setTableName(tableName);
		jsonUpdate.setValues(values);

		Mockito.when(info.getQueryParameters()).thenReturn(row);
		String authDatax =":"+password;
		String authorizationx = new String(Base64.encode(authDatax.getBytes()));
		String appNamex="xx";
		Response response = data.updateTable("1", "1", "1", "", appNamex,
				authorizationx, jsonUpdate, keyspaceName, tableName, info);
		System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());

		assertEquals(401, response.getStatus());
	}


	@Test
	public void test6_select() throws Exception {
		System.out.println("Testing select");
		createAndInsertIntoTable();
		JsonSelect jsonSelect = new JsonSelect();
		Map<String, String> consistencyInfo = new HashMap<>();
		MultivaluedMap<String, String> row = new MultivaluedMapImpl();
		row.add("emp_name", "testname");
		consistencyInfo.put("type", "atomic");
		jsonSelect.setConsistencyInfo(consistencyInfo);
		Mockito.when(info.getQueryParameters()).thenReturn(row);
		Response response = data.select("1", "1", "1","abc66ccc-d857-4e90-b1e5-df98a3d40ce6", 
				appName, authorization, keyspaceName, tableName, info);
		HashMap<String,HashMap<String,Object>> map = (HashMap<String, HashMap<String, Object>>) response.getEntity();
		HashMap<String, Object> result = map.get("result");
		System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
		
		Map<String, String> row0 = (Map<String, String>) result.get("row 0");
		assertEquals("testname", row0.get("emp_name"));
		assertEquals(BigInteger.valueOf(500), row0.get("emp_salary"));
	}

	@Test
	public void test6_selectCritical() throws Exception {
		System.out.println("Testing select critical");
		createAndInsertIntoTable();
		JsonInsert jsonInsert = new JsonInsert();
		Map<String, String> consistencyInfo = new HashMap<>();
		MultivaluedMap<String, String> row = new MultivaluedMapImpl();
		row.add("emp_name", "testname");
		consistencyInfo.put("type", "atomic");
		jsonInsert.setConsistencyInfo(consistencyInfo);
		Mockito.when(info.getQueryParameters()).thenReturn(row);
		Response response = data.selectCritical("1", "1", "1","abc66ccc-d857-4e90-b1e5-df98a3d40ce6", 
				appName, authorization, jsonInsert, keyspaceName, tableName,info);
		HashMap<String,HashMap<String,Object>> map = (HashMap<String, HashMap<String, Object>>) response.getEntity();
		HashMap<String, Object> result = map.get("result");
		System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
		
		Map<String, String> row0 = (Map<String, String>) result.get("row 0");
		assertEquals("testname", row0.get("emp_name"));
		assertEquals(BigInteger.valueOf(500), row0.get("emp_salary"));
	}

	@Test
	public void test6_deleteFromTable() throws Exception {
		System.out.println("Testing delete from table");
		createAndInsertIntoTable();
		JsonDelete jsonDelete = new JsonDelete();
		Map<String, String> consistencyInfo = new HashMap<>();
		MultivaluedMap<String, String> row = new MultivaluedMapImpl();
		row.add("emp_name", "test1");
		consistencyInfo.put("type", "atomic");
		jsonDelete.setConsistencyInfo(consistencyInfo);
		Mockito.when(info.getQueryParameters()).thenReturn(row);
		Response response = data.deleteFromTable("1", "1", "1",
				"abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
				jsonDelete, keyspaceName, tableName, info);
		System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());

		assertEquals(200, response.getStatus());
	}

	// Values
	@Ignore
	@Test
	public void test6_deleteFromTable1() throws Exception {
		System.out.println("Testing delete from table missing delete object");
		createAndInsertIntoTable();
		
		JsonDelete jsonDelete = new JsonDelete();
		Map<String, String> consistencyInfo = new HashMap<>();
		MultivaluedMap<String, String> row = new MultivaluedMapImpl();
		consistencyInfo.put("type", "atomic");
		jsonDelete.setConsistencyInfo(consistencyInfo);
		//Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
		Mockito.when(info.getQueryParameters()).thenReturn(row);
		Response response = data.deleteFromTable("1", "1", "1",
				"abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
				jsonDelete, keyspaceName, tableName, info);
		System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());

		assertEquals(400, response.getStatus());
	}

	// delObj
	@Test
	public void test6_deleteFromTable2() throws Exception {
		System.out.println("Testing delete from table missing delete object");
		createAndInsertIntoTable();
		JsonDelete jsonDelete = new JsonDelete();
		Map<String, String> consistencyInfo = new HashMap<>();
		consistencyInfo.put("type", "atomic");
		jsonDelete.setConsistencyInfo(consistencyInfo);
		Response response = data.deleteFromTable("1", "1", "1",
				"abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
				null, keyspaceName, tableName, info);
		System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
		
		assertEquals(400, response.getStatus());
	}

	@Test
	public void test7_dropTable() throws Exception {
		System.out.println("Testing drop table");
		createTable();
		JsonTable jsonTable = new JsonTable();
		Map<String, String> consistencyInfo = new HashMap<>();
		consistencyInfo.put("type", "atomic");
		jsonTable.setConsistencyInfo(consistencyInfo);
		Response response = data.dropTable("1", "1", "1",
				"abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
				keyspaceName, tableName);
		System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
		
		assertEquals(200, response.getStatus());
	}


	@Test
	public void test8_deleteKeyspace() throws Exception {
		System.out.println("Testing drop keyspace");

		JsonKeySpace jsonKeyspace = new JsonKeySpace();
		Map<String, String> consistencyInfo = new HashMap<>();
		Map<String, Object> replicationInfo = new HashMap<>();
		consistencyInfo.put("type", "eventual");
		replicationInfo.put("class", "SimpleStrategy");
		replicationInfo.put("replication_factor", 1);
		jsonKeyspace.setConsistencyInfo(consistencyInfo);
		jsonKeyspace.setDurabilityOfWrites("true");
		jsonKeyspace.setKeyspaceName("TestApp1");
		jsonKeyspace.setReplicationInfo(replicationInfo);
		Response response = data.dropKeySpace("1", "1", "1",
				"abc66ccc-d857-4e90-b1e5-df98a3d40ce6", authorization,appName, keyspaceName);
		System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());

		assertEquals(400, response.getStatus());
	}
	
	private static void createAdminTable() throws Exception {
		testObject = new PreparedQueryObject();
		testObject.appendQueryString(CassandraCQL.createAdminKeyspace);
		MusicCore.eventualPut(testObject);
		testObject = new PreparedQueryObject();
		testObject.appendQueryString(CassandraCQL.createAdminTable);
		MusicCore.eventualPut(testObject);

		testObject = new PreparedQueryObject();
		testObject.appendQueryString(
				"INSERT INTO admin.keyspace_master (uuid, keyspace_name, application_name, is_api, "
						+ "password, username, is_aaf) VALUES (?,?,?,?,?,?,?)");
		testObject.addValue(MusicUtil.convertToActualDataType(DataType.uuid(), uuid));
		testObject.addValue(MusicUtil.convertToActualDataType(DataType.text(),
				MusicUtil.DEFAULTKEYSPACENAME));
		testObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), appName));
		testObject.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(), "True"));
		testObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), BCrypt.hashpw(password, BCrypt.gensalt())));
		testObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), userId));
		testObject.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(), isAAF));
		MusicCore.eventualPut(testObject);

		testObject = new PreparedQueryObject();
		testObject.appendQueryString(
				"select uuid from admin.keyspace_master where application_name = ? allow filtering");
		testObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), appName));
		ResultSet rs = MusicCore.get(testObject);
		List<Row> rows = rs.all();
		if (rows.size() > 0) {
			System.out.println("#######UUID is:" + rows.get(0).getUUID("uuid"));
		}
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
