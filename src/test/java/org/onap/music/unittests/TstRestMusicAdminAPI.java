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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.mindrot.jbcrypt.BCrypt;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.onap.music.authentication.MusicAuthentication;
import org.onap.music.conductor.conditionals.JsonConditional;
import org.onap.music.conductor.conditionals.RestMusicConditionalAPI;
import org.onap.music.datastore.MusicDataStoreHandle;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.datastore.jsonobjects.JsonDelete;
import org.onap.music.datastore.jsonobjects.JsonInsert;
import org.onap.music.datastore.jsonobjects.JsonKeySpace;
import org.onap.music.datastore.jsonobjects.JsonLeasedLock;
import org.onap.music.datastore.jsonobjects.JsonOnboard;
import org.onap.music.datastore.jsonobjects.JsonSelect;
import org.onap.music.datastore.jsonobjects.JsonTable;
import org.onap.music.datastore.jsonobjects.JsonUpdate;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.lockingservice.zookeeper.MusicLockingService;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.rest.RestMusicAdminAPI;
import org.onap.music.rest.RestMusicBmAPI;
import org.onap.music.rest.RestMusicDataAPI;
import org.onap.music.rest.RestMusicHealthCheckAPI;
import org.onap.music.rest.RestMusicLocksAPI;
import org.onap.music.rest.RestMusicTestAPI;
import org.onap.music.rest.RestMusicVersionAPI;
import org.onap.music.service.impl.MusicZKCore;

import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.sun.jersey.core.util.Base64;
import com.sun.jersey.core.util.MultivaluedMapImpl;

@Ignore //TODO need to resolve static calls to music authenticate
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
//@RunWith(PowerMockRunner.class)
//@PowerMockRunnerDelegate(SpringJUnit4ClassRunner.class)
//@PrepareForTest(MusicAuthentication.class)
public class TstRestMusicAdminAPI {

    RestMusicAdminAPI admin = new RestMusicAdminAPI();
    static PreparedQueryObject testObject;

    @Mock
    HttpServletResponse http;

    @Mock
    UriInfo info;
    
    static String appName = "TestApp";
    static String userId = "TestUser";
    static String password = "TestPassword";
    static String adminName = "username";
    static String adminPassword = "password";
    static String adminAuthData = adminName +":"+adminPassword;
    static String wrongAdminAuthData = adminName+"123"+":"+adminPassword;
    static String authData = userId+":"+password;
    static String wrongAuthData = userId+":"+"pass";
    static String authorization = new String(Base64.encode(authData.getBytes()));
    static String wrongAuthorization = new String(Base64.encode(wrongAuthData.getBytes()));
    static String adminAuthorization = new String(Base64.encode(adminAuthData.getBytes()));
    static String worngAdminAuthorization = new String(Base64.encode(wrongAdminAuthData.getBytes()));
    
    static boolean isAAF = false;
    static UUID uuid = UUID.fromString("abc66ccc-d857-4e90-b1e5-df98a3d40ce6");
    static String keyspaceName = "testCassa";
    static String tableName = "employees";
    static String tableNameConditional = "Conductor";
    static String xLatestVersion = "X-latestVersion";
    static String onboardUUID = null;
    static String lockId = null;
    static String lockName = "testCassa.employees.sample3";

    @BeforeClass
    public static void init() throws Exception {
		System.out.println("Testing RestMusicAdmin class");
		//PowerMockito.mockStatic(MusicAuthentication.class);
    	try {
    		MusicDataStoreHandle.mDstoreHandle = CassandraCQL.connectToEmbeddedCassandra();
			createAdminTable();
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("Unable to initialize before TestRestMusicData test class. " + e.getMessage());
		}
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        testObject = new PreparedQueryObject();
        testObject.appendQueryString("DROP KEYSPACE IF EXISTS " + keyspaceName);
        MusicCore.eventualPut(testObject);
        testObject = new PreparedQueryObject();
        testObject.appendQueryString("DROP KEYSPACE IF EXISTS admin");
        MusicCore.eventualPut(testObject);
        MusicDataStoreHandle.mDstoreHandle.close();
    }

    
    //TODO FIX tests for admin
    

    @Test
    public void test6_onboard() throws Exception {
    	System.out.println("Testing application onboarding");
    	
    	authenticateTrue();
    	
    	JsonOnboard jsonOnboard = new JsonOnboard();
    	jsonOnboard.setAppname("TestApp2");
    	jsonOnboard.setIsAAF("false"); jsonOnboard.setUserId("TestUser2");
    	jsonOnboard.setPassword("TestPassword2");

    	Response response = admin.onboardAppWithMusic(jsonOnboard,adminAuthorization);
    	System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
    	/*resultMap.containsKey("success"); onboardUUID =
    			resultMap.get("Generated AID").toString();
    	assertEquals("Your application TestApp2 has been onboarded with MUSIC.",
    			resultMap.get("Success")); */
    	assertEquals(200, response.getStatus());
    }

	@Test
    public void Test6_onboard_duplicate() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setUserId("TestUser2");
        jsonOnboard.setPassword("TestPassword2");
        Response response = admin.onboardAppWithMusic(jsonOnboard,adminAuthorization);
        assertEquals(204, response.getStatus());
    }

    // Missing appname
    @Test
    public void Test6_onboard1() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setUserId("TestUser2");
        jsonOnboard.setPassword("TestPassword2");
        Map<String, Object> resultMap = (Map<String, Object>) admin.onboardAppWithMusic(jsonOnboard,adminAuthorization).getEntity();
//        assertTrue(resultMap.containsKey("error"));
        //System.out.println("--->" + resultMap.toString());
        //assertEquals("Unauthorized: Please check the request parameters. Some of the required values appName(ns), userId, password, isAAF are missing.", resultMap.get("Exception"));
    }


    @Test
    public void Test7_onboardSearch() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setAid(onboardUUID);
        Map<String, Object> resultMap = (Map<String, Object>) admin.getOnboardedInfoSearch(jsonOnboard,adminAuthorization).getEntity();
        resultMap.containsKey("success");
        assertEquals(null, resultMap.get(onboardUUID));
    }

    // Missing appname
    @Test
    public void Test7_onboardSearch1() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setAid(onboardUUID);
        Map<String, Object> resultMap = (Map<String, Object>) admin.getOnboardedInfoSearch(jsonOnboard,adminAuthorization).getEntity();
        System.out.println("--->" + resultMap.toString());
        resultMap.containsKey("success");
        assertEquals(null, resultMap.get(onboardUUID));
    }
    
    @Test
    public void Test7_onboardSearch_empty() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        Response response =  admin.getOnboardedInfoSearch(jsonOnboard,adminAuthorization);
      //  assertEquals(400, response.getStatus());
    }

    @Test
    public void Test7_onboardSearch_invalidAid() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setAid("abc66ccc-d857-4e90-b1e5-df98a3d40ce6");
        Response response = admin.getOnboardedInfoSearch(jsonOnboard,adminAuthorization);
       // assertEquals(400, response.getStatus());
    }

    @Test
    public void Test8_onboardUpdate() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setUserId("TestUser3");
        jsonOnboard.setPassword("TestPassword3");
        jsonOnboard.setAid(onboardUUID);
        Map<String, Object> resultMap = (Map<String, Object>) admin.updateOnboardApp(jsonOnboard,adminAuthorization).getEntity();
        System.out.println("--->" + resultMap.toString());
        resultMap.containsKey("success");
        assertNotNull(resultMap);
    }

    // Aid null
    @Test
    public void Test8_onboardUpdate1() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setUserId("TestUser3");
        jsonOnboard.setPassword("TestPassword3");
        Map<String, Object> resultMap = (Map<String, Object>) admin.updateOnboardApp(jsonOnboard,adminAuthorization).getEntity();
        System.out.println("--->" + resultMap.toString());
        assertNotNull(resultMap);
    }

    // Appname not null
    @Test
    public void Test8_onboardUpdate2() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setUserId("TestUser3");
        jsonOnboard.setPassword("TestPassword3");
        jsonOnboard.setAid(onboardUUID);
        Map<String, Object> resultMap = (Map<String, Object>) admin.updateOnboardApp(jsonOnboard,adminAuthorization).getEntity();
        assertNotNull(resultMap);
    }

    // All null
    @Test
    public void Test8_onboardUpdate3() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAid(onboardUUID);
        Map<String, Object> resultMap = (Map<String, Object>) admin.updateOnboardApp(jsonOnboard,adminAuthorization).getEntity();
        assertNotNull(resultMap);
    }

    @Test
    public void Test9_onboardDelete() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setAid(onboardUUID);
        Map<String, Object> resultMap = (Map<String, Object>) admin.deleteOnboardApp(jsonOnboard,adminAuthorization).getEntity();
        resultMap.containsKey("success");
        assertNotNull(resultMap);
    }

    @Test
    public void Test9_onboardDelete1() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        Map<String, Object> resultMap = (Map<String, Object>) admin.deleteOnboardApp(jsonOnboard,adminAuthorization).getEntity();
        assertNotNull(resultMap);
    }

    //Music Health Check
    @Test
    public void Test3_HealthCheck_cassandra() {
        String consistency = "ONE";
        RestMusicHealthCheckAPI healthCheck = new RestMusicHealthCheckAPI();
        HttpServletResponse servletResponse = Mockito.mock(HttpServletResponse.class);
        Response response = healthCheck.cassandraStatus(servletResponse, consistency);
        assertEquals(200, response.getStatus());
    }

    @Test
    public void Test3_HealthCheck_cassandra_cosistencyQuorum() {
        String consistency = "QUORUM";
        RestMusicHealthCheckAPI healthCheck = new RestMusicHealthCheckAPI();
        HttpServletResponse servletResponse = Mockito.mock(HttpServletResponse.class);
        Response response = healthCheck.cassandraStatus(servletResponse, consistency);
        assertEquals(200, response.getStatus());
    }

    @Ignore
    @Test
    public void Test3_HealthCheck_zookeeper() {
        RestMusicHealthCheckAPI healthCheck = new RestMusicHealthCheckAPI();
        HttpServletResponse servletResponse = Mockito.mock(HttpServletResponse.class);
        Response response = healthCheck.ZKStatus(servletResponse);
        assertEquals(200, response.getStatus());
    }

    @Ignore
    public void Test4_pureZKcreate() throws Exception {
        RestMusicBmAPI bmApi = new RestMusicBmAPI();
        bmApi.pureZkCreate("sample");
    }

    
    @Ignore
    public void Test4_pureZKUpdate() throws Exception {
        RestMusicBmAPI bmApi = new RestMusicBmAPI();
        bmApi.pureZkCreate("sample1");
        JsonInsert jsonInsert = new JsonInsert();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> values = new HashMap<>();
        values.put("uuid", "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6");
        values.put("emp_name", "testName_create");
        values.put("emp_salary", 500);
        consistencyInfo.put("type", "eventual");
        jsonInsert.setConsistencyInfo(consistencyInfo);
        jsonInsert.setKeyspaceName(keyspaceName);
        jsonInsert.setTableName(tableName);
        jsonInsert.setValues(values);
        bmApi.pureZkUpdate(jsonInsert, "sampleNode1");
    }

    @Ignore
    public void Test4_pureZKGet() throws Exception {
        RestMusicBmAPI bmApi = new RestMusicBmAPI();
        bmApi.pureZkGet("sample");
    }

    /*
     * @Test public void Test5_ZKAtomicPut_atomic() throws Exception {
     * RestMusicBmAPI bmApi = new RestMusicBmAPI(); JsonInsert jsonInsert = new
     * JsonInsert(); Map<String, String> consistencyInfo = new HashMap<>();
     * Map<String, Object> values = new HashMap<>(); values.put("uuid",
     * "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6"); values.put("emp_name",
     * "testName_create"); values.put("emp_salary", 1500);
     * consistencyInfo.put("type", "atomic");
     * jsonInsert.setConsistencyInfo(consistencyInfo);
     * jsonInsert.setKeyspaceName(keyspaceName); jsonInsert.setTableName(tableName);
     * jsonInsert.setValues(values); bmApi.pureZkAtomicPut(jsonInsert, lockName,
     * "sampleNode1"); }
     */
    /*
     * @Test public void Test5_ZKAtomicPut_atomic_with_delete() throws Exception {
     * RestMusicBmAPI bmApi = new RestMusicBmAPI(); JsonInsert jsonInsert = new
     * JsonInsert(); Map<String, String> consistencyInfo = new HashMap<>();
     * Map<String, Object> values = new HashMap<>(); values.put("uuid",
     * "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6"); values.put("emp_name",
     * "testName_create"); values.put("emp_salary", 1500);
     * consistencyInfo.put("type", "atomic_delete_lock");
     * jsonInsert.setConsistencyInfo(consistencyInfo);
     * jsonInsert.setKeyspaceName(keyspaceName); jsonInsert.setTableName(tableName);
     * jsonInsert.setValues(values); bmApi.pureZkAtomicPut(jsonInsert, lockName,
     * "sampleNode1"); }
     */

    /*
     * @Test public void Test5_ZKAtomicGet_atomic() throws Exception {
     * RestMusicBmAPI bmApi = new RestMusicBmAPI(); JsonInsert jsonInsert = new
     * JsonInsert(); Map<String, String> consistencyInfo = new HashMap<>();
     * Map<String, Object> values = new HashMap<>(); values.put("uuid",
     * "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6"); values.put("emp_name",
     * "testName_create"); values.put("emp_salary", 1500);
     * consistencyInfo.put("type", "atomic_delete_lock");
     * jsonInsert.setConsistencyInfo(consistencyInfo);
     * jsonInsert.setKeyspaceName(keyspaceName); jsonInsert.setTableName(tableName);
     * jsonInsert.setValues(values); bmApi.pureZkAtomicGet(jsonInsert, lockName,
     * "sampleNode1"); }
     */

    /*
     * @Test public void Test5_ZKAtomicGet_atomic_with_delete() throws Exception {
     * RestMusicBmAPI bmApi = new RestMusicBmAPI(); JsonInsert jsonInsert = new
     * JsonInsert(); Map<String, String> consistencyInfo = new HashMap<>();
     * Map<String, Object> values = new HashMap<>(); values.put("uuid",
     * "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6"); values.put("emp_name",
     * "testName_create"); values.put("emp_salary", 1500);
     * consistencyInfo.put("type", "atomic_delete_lock");
     * jsonInsert.setConsistencyInfo(consistencyInfo);
     * jsonInsert.setKeyspaceName(keyspaceName); jsonInsert.setTableName(tableName);
     * jsonInsert.setValues(values); bmApi.pureZkAtomicGet(jsonInsert, lockName,
     * "sampleNode1"); }
     */

    @Ignore
    @Test
    public void Test5_updateCassa() throws Exception {
        RestMusicBmAPI bmApi = new RestMusicBmAPI();
        JsonInsert jsonInsert = new JsonInsert();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> values = new HashMap<>();
        values.put("emp_salary", 1500);
        consistencyInfo.put("type", "eventual");
        jsonInsert.setConsistencyInfo(consistencyInfo);
        jsonInsert.setKeyspaceName(keyspaceName);
        jsonInsert.setTableName(tableName);
        jsonInsert.setValues(values);
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        row.add("emp_name", "testName_create");
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        //bmApi.updateTableCassa(jsonInsert, keyspaceName, tableName, info);
    }

    // RestMusicConditional
    @Test
    public void Test5_createTable_conditional() throws Exception {
        JsonTable jsonTable = new JsonTable();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, String> fields = new HashMap<>();
        fields.put("id", "text");
        fields.put("plans", "Map<text,text>");
        fields.put("PRIMARY KEY", "(id)");
        consistencyInfo.put("type", "eventual");
        jsonTable.setConsistencyInfo(consistencyInfo);
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPrimaryKey("id");
        jsonTable.setTableName(tableNameConditional);
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        /*Response response = data.createTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6",appName, authorization,
                        jsonTable, keyspaceName, tableNameConditional);
        System.out.println("#######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        //assertEquals(200, response.getStatus());
        assertEquals(401, response.getStatus());
        */
    }

    @Test
    public void Test6_insertConditional() throws Exception {
        RestMusicConditionalAPI conditionalApi = new RestMusicConditionalAPI();
        JsonConditional json = new JsonConditional();
        json.setPrimaryKey("id");
        json.setPrimaryKeyValue("123|abc|port");
        json.setCasscadeColumnName("plans");
        Map<String, Object> tableValues =  new HashMap<>();
        tableValues.put("id", "123|abc|port");
        json.setTableValues(tableValues);
        Map<String, Object> columnData =  new HashMap<>();
        Map<String, String> column =  new HashMap<>();
        column.put("created", "time");
        columnData.put("key", "P2");
        columnData.put("value", column);
        json.setCasscadeColumnData(columnData);
        Map<String, String> cond = new HashMap<>();
        Map<String, String> cond1 = new HashMap<>();
        Map<String, Map<String, String>> conditions = new HashMap<String, Map<String, String>>();
        cond.put("status", "under-spin-up");
        cond1.put("status", "parked");
        conditions.put("exists", cond);
        conditions.put("nonexists", cond1);
        json.setConditions(conditions);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = conditionalApi.insertConditional("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
                appName, authorization, keyspaceName, tableNameConditional, json);
        assertEquals(401, response.getStatus());
    }

    @Test
    public void Test6_insertConditional_primaryKey_null() throws Exception {
        RestMusicConditionalAPI conditionalApi = new RestMusicConditionalAPI();
        JsonConditional json = new JsonConditional();
        json.setPrimaryKeyValue("123|abc|port");
        json.setCasscadeColumnName("plans");
        Map<String, Object> tableValues =  new HashMap<>();
        tableValues.put("id", "123|abc|port");
        json.setTableValues(tableValues);
        Map<String, Object> columnData =  new HashMap<>();
        Map<String, String> column =  new HashMap<>();
        column.put("created", "time");
        columnData.put("key", "P2");
        columnData.put("value", column);
        json.setCasscadeColumnData(columnData);
        Map<String, String> cond = new HashMap<>();
        Map<String, String> cond1 = new HashMap<>();
        Map<String, Map<String, String>> conditions = new HashMap<String, Map<String, String>>();
        cond.put("status", "under-spin-up");
        cond1.put("status", "parked");
        conditions.put("exists", cond);
        conditions.put("nonexists", cond1);
        json.setConditions(conditions);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = conditionalApi.insertConditional("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
                appName, authorization, keyspaceName, tableNameConditional, json);
        assertEquals(401, response.getStatus());
    }

    @Test
    public void Test6_insertConditional_wrongAuth() throws Exception {
        RestMusicConditionalAPI conditionalApi = new RestMusicConditionalAPI();
        JsonConditional json = new JsonConditional();
        json.setPrimaryKey("id");
        json.setPrimaryKeyValue("123|abc|port");
        json.setCasscadeColumnName("plans");
        Map<String, Object> tableValues =  new HashMap<>();
        tableValues.put("id", "123|abc|port");
        json.setTableValues(tableValues);
        Map<String, Object> columnData =  new HashMap<>();
        Map<String, String> column =  new HashMap<>();
        column.put("created", "time");
        columnData.put("key", "P2");
        columnData.put("value", column);
        json.setCasscadeColumnData(columnData);
        Map<String, String> cond = new HashMap<>();
        Map<String, String> cond1 = new HashMap<>();
        Map<String, Map<String, String>> conditions = new HashMap<String, Map<String, String>>();
        cond.put("status", "under-spin-up");
        cond1.put("status", "parked");
        conditions.put("exists", cond);
        conditions.put("nonexists", cond1);
        json.setConditions(conditions);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = conditionalApi.insertConditional("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
                appName, wrongAuthorization, keyspaceName, tableNameConditional, json);
        assertEquals(401, response.getStatus());
    }

    @Test
    public void Test7_updateConditional() throws Exception {
        RestMusicConditionalAPI conditionalApi = new RestMusicConditionalAPI();
        JsonConditional json = new JsonConditional();
        json.setPrimaryKey("id");
        json.setPrimaryKeyValue("123|abc|port");
        json.setCasscadeColumnName("plans");
        Map<String, Object> tableValues =  new HashMap<>();
        tableValues.put("id", "123|abc|port");
        json.setTableValues(tableValues);
        Map<String, Object> columnData =  new HashMap<>();
        Map<String, String> column =  new HashMap<>();
        column.put("created", "time");
        columnData.put("key", "P2");
        columnData.put("value", column);
        json.setCasscadeColumnData(columnData);
        Map<String, String> cond = new HashMap<>();
        Map<String, String> cond1 = new HashMap<>();
        Map<String, Map<String, String>> conditions = new HashMap<String, Map<String, String>>();
        cond.put("updated", "new time");
        conditions.put("exists", cond);
        conditions.put("nonexists", cond1);
        json.setConditions(conditions);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = conditionalApi.updateConditional("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
                appName, authorization, keyspaceName, tableNameConditional, json);
        assertEquals(401, response.getStatus());
    }

    @Test
    public void Test7_updateConditional_wrongAuth() throws Exception {
        RestMusicConditionalAPI conditionalApi = new RestMusicConditionalAPI();
        JsonConditional json = new JsonConditional();
        json.setPrimaryKey("id");
        json.setPrimaryKeyValue("123|abc|port");
        json.setCasscadeColumnName("plans");
        Map<String, Object> tableValues =  new HashMap<>();
        tableValues.put("id", "123|abc|port");
        json.setTableValues(tableValues);
        Map<String, Object> columnData =  new HashMap<>();
        Map<String, String> column =  new HashMap<>();
        column.put("created", "time");
        columnData.put("key", "P2");
        columnData.put("value", column);
        json.setCasscadeColumnData(columnData);
        Map<String, String> cond = new HashMap<>();
        Map<String, String> cond1 = new HashMap<>();
        Map<String, Map<String, String>> conditions = new HashMap<String, Map<String, String>>();
        cond.put("updated", "new time");
        conditions.put("exists", cond);
        conditions.put("nonexists", cond1);
        json.setConditions(conditions);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = conditionalApi.updateConditional("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
                appName, wrongAuthorization, keyspaceName, tableNameConditional, json);
        assertEquals(401, response.getStatus());
    }

    @Test
    public void Test7_updateConditional_primarykey_null() throws Exception {
        RestMusicConditionalAPI conditionalApi = new RestMusicConditionalAPI();
        JsonConditional json = new JsonConditional();
        json.setPrimaryKeyValue("123|abc|port");
        json.setCasscadeColumnName("plans");
        Map<String, Object> tableValues =  new HashMap<>();
        tableValues.put("id", "123|abc|port");
        json.setTableValues(tableValues);
        Map<String, Object> columnData =  new HashMap<>();
        Map<String, String> column =  new HashMap<>();
        column.put("created", "time");
        columnData.put("key", "P2");
        columnData.put("value", column);
        json.setCasscadeColumnData(columnData);
        Map<String, String> cond = new HashMap<>();
        Map<String, String> cond1 = new HashMap<>();
        Map<String, Map<String, String>> conditions = new HashMap<String, Map<String, String>>();
        cond.put("updated", "new time");
        conditions.put("exists", cond);
        conditions.put("nonexists", cond1);
        json.setConditions(conditions);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = conditionalApi.updateConditional("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
                appName, authorization, keyspaceName, tableNameConditional, json);
        assertEquals(401, response.getStatus());
    }
    
    @Ignore
    @Test
    public void Test8_HealthCheck_cassandra_musicHealthCheck() {
        RestMusicHealthCheckAPI healthCheck = new RestMusicHealthCheckAPI();
        Response response = healthCheck.musicHealthCheck();
        assertEquals(200, response.getStatus());
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
    
    /**
     * Confirm an authentication
     * @throws Exception 
     */
    private void authenticateTrue() throws Exception {
		//PowerMockito.when(MusicAuthentication.authenticateAdmin(Mockito.anyString())).thenReturn(true);
	}
   
}