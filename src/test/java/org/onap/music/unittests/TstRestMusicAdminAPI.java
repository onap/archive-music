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
import org.onap.music.datastore.jsonobjects.JsonTable;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import org.onap.music.rest.RestMusicAdminAPI;
import org.onap.music.rest.RestMusicHealthCheckAPI;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.sun.jersey.core.util.Base64;

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
    }    

    @Ignore
    @Test
    public void test6_onboard() throws Exception {
    	System.out.println("Testing application onboarding");
    	    	
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
    public void test6_onboardCantReachAAF() throws Exception {
        System.out.println("Testing application onboarding without reaching aaf");        
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setIsAAF("false"); jsonOnboard.setUserId("TestUser2");
        jsonOnboard.setPassword("TestPassword2");

        Response response = admin.onboardAppWithMusic(jsonOnboard,adminAuthorization);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        //TODO FIX when we can authenticate
        assertEquals(401, response.getStatus());
    }

    @Ignore
	@Test
    public void test6_onboard_duplicate() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setUserId("TestUser2");
        jsonOnboard.setPassword("TestPassword2");
        Response response = admin.onboardAppWithMusic(jsonOnboard,adminAuthorization);
        assertEquals(204, response.getStatus());
    }

    // Missing appname
	@Ignore
    @Test
    public void test6_onboard1() throws Exception {
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
    public void test7_onboardSearch() throws Exception {
        System.out.println("Testing application onboarding search w/o reaching aaf");        
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setAid(onboardUUID);
        Response response = admin.getOnboardedInfoSearch(jsonOnboard,adminAuthorization);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        //TODO FIX when we can authenticate
        assertEquals(401, response.getStatus());
    }

    // Missing appname
    @Ignore
    @Test
    public void test7_onboardSearch1() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setAid(onboardUUID);
        Map<String, Object> resultMap = (Map<String, Object>) admin.getOnboardedInfoSearch(jsonOnboard,adminAuthorization).getEntity();
        System.out.println("--->" + resultMap.toString());
        resultMap.containsKey("success");
        assertEquals(null, resultMap.get(onboardUUID));
    }
    
    @Ignore
    @Test
    public void test7_onboardSearch_empty() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        Response response =  admin.getOnboardedInfoSearch(jsonOnboard,adminAuthorization);
      //  assertEquals(400, response.getStatus());
    }

    @Ignore
    @Test
    public void test7_onboardSearch_invalidAid() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setAid("abc66ccc-d857-4e90-b1e5-df98a3d40ce6");
        Response response = admin.getOnboardedInfoSearch(jsonOnboard,adminAuthorization);
       // assertEquals(400, response.getStatus());
    }

    @Ignore
    @Test
    public void test8_onboardUpdate() throws Exception {
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
    
    @Test
    public void test8_onboardUpdateNoAAF() throws Exception {
        System.out.println("Testing update application onboarding search w/o reaching aaf");
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setUserId("TestUser3");
        jsonOnboard.setPassword("TestPassword3");
        jsonOnboard.setAid(onboardUUID);
        Response response = admin.updateOnboardApp(jsonOnboard,adminAuthorization);
        
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        assertEquals(401, response.getStatus());
    }

    // Aid null
    @Ignore
    @Test
    public void test8_onboardUpdate1() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setUserId("TestUser3");
        jsonOnboard.setPassword("TestPassword3");
        Map<String, Object> resultMap = (Map<String, Object>) admin.updateOnboardApp(jsonOnboard,adminAuthorization).getEntity();
        System.out.println("--->" + resultMap.toString());
        assertNotNull(resultMap);
    }

    // Appname not null
    @Ignore
    @Test
    public void test8_onboardUpdate2() throws Exception {
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
    @Ignore
    @Test
    public void test8_onboardUpdate3() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAid(onboardUUID);
        Map<String, Object> resultMap = (Map<String, Object>) admin.updateOnboardApp(jsonOnboard,adminAuthorization).getEntity();
        assertNotNull(resultMap);
    }

    @Ignore
    @Test
    public void test9_onboardDelete() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setAid(onboardUUID);
        Map<String, Object> resultMap = (Map<String, Object>) admin.deleteOnboardApp(jsonOnboard,adminAuthorization).getEntity();
        resultMap.containsKey("success");
        assertNotNull(resultMap);
    }
    
    @Test
    public void test9_onboardDeleteNoAAF() throws Exception {
        System.out.println("Testing onboard delete without aaf");
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setAid(onboardUUID);
        Response response = admin.deleteOnboardApp(jsonOnboard,adminAuthorization);
        
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        assertEquals(401, response.getStatus());
    }

    @Ignore
    @Test
    public void test9_onboardDelete1() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        Map<String, Object> resultMap = (Map<String, Object>) admin.deleteOnboardApp(jsonOnboard,adminAuthorization).getEntity();
        assertNotNull(resultMap);
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
   
}