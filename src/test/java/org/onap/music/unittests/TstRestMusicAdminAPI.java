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

package org.onap.music.unittests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.UUID;

import javax.ws.rs.core.Response;


import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.FieldSetter;
import org.onap.music.authentication.MusicAuthentication;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.datastore.jsonobjects.JsonOnboard;
import org.onap.music.main.MusicCore;
import org.onap.music.rest.RestMusicAdminAPI;
import com.sun.jersey.core.util.Base64;

public class TstRestMusicAdminAPI {

    RestMusicAdminAPI admin = new RestMusicAdminAPI();
    static PreparedQueryObject testObject;
    
    @Mock
    MusicAuthentication authMock;
    
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
    static String wrongAdminAuthorization = new String(Base64.encode(wrongAdminAuthData.getBytes()));
    
    static boolean isAAF = false;
    static UUID uuid = UUID.fromString("abc66ccc-d857-4e90-b1e5-df98a3d40ce6");
    static String keyspaceName = "testCassa";
    static String tableName = "employees";
    static String tableNameConditional = "Conductor";
    static String xLatestVersion = "X-latestVersion";
    static String onboardUUID = TestsUsingCassandra.onboardUUID;
    static String lockId = null;
    static String lockName = "testCassa.employees.sample3";

    @BeforeClass
    public static void init() throws Exception {
		System.out.println("Testing RestMusicAdmin class");
		//PowerMockito.mockStatic(MusicAuthentication.class);
    	try {
    		//MusicDataStoreHandle.mDstoreHandle = CassandraCQL.connectToEmbeddedCassandra();
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("Unable to initialize before TestRestMusicData test class. " + e.getMessage());
		}
    }

    @Before
    public void beforeEach() throws NoSuchFieldException {
        authenticateAdminTrue();
    }
    
    @After
    public void afterEach() {
        testObject = new PreparedQueryObject();
        testObject.appendQueryString("DELETE * FROM admin.keyspace_master;");
        MusicCore.eventualPut(testObject);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        testObject = new PreparedQueryObject();
        testObject.appendQueryString("DROP KEYSPACE IF EXISTS " + keyspaceName);
        MusicCore.eventualPut(testObject);
    }
    
    @Test
    public void test6_onboard() throws Exception {
        System.out.println("Testing application onboarding");
        
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setIsAAF("false"); jsonOnboard.setUserId("TestUser2");
        jsonOnboard.setPassword("TestPassword2");

        Response response = admin.onboardAppWithMusic(jsonOnboard,adminAuthorization);

        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        assertEquals(200, response.getStatus());
    }    
    
    @Test
    public void test6_onboard_wrongCredentials() throws Exception {
        System.out.println("Testing application onboarding wrong credentials");        
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setIsAAF("false"); jsonOnboard.setUserId("TestUser2");
        jsonOnboard.setPassword("TestPassword2");

        Response response = admin.onboardAppWithMusic(jsonOnboard,wrongAdminAuthorization);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        assertEquals(401, response.getStatus());
    }

	@Test
    public void test6_onboard_duplicate() throws Exception {
	    System.out.println("Testing a duplicate onboarding call");  
	    
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setUserId("TestUser2");
        jsonOnboard.setPassword("TestPassword2");
        Response response = admin.onboardAppWithMusic(jsonOnboard,adminAuthorization);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        assertEquals(200, response.getStatus());
    }

    // Missing appname
    @Test
    public void test6_onboard_noAppName() throws Exception {
        System.out.println("Testing onboard missing app name");

        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setUserId("TestUser2");
        jsonOnboard.setPassword("TestPassword2");
        Response response = admin.onboardAppWithMusic(jsonOnboard,adminAuthorization);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());

        assertEquals(400, response.getStatus());
    }


    @Test
    public void test7_onboardSearch_notOnboarded() throws Exception {
        System.out.println("Testing application onboarding search for app that isn't onboarded");
        
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setAid(onboardUUID);
        Response response = admin.getOnboardedInfoSearch(jsonOnboard,adminAuthorization);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        //Application is not onboarded
        assertEquals(400, response.getStatus());
    }
    
    @Test
    public void test7_onboardSearch() throws Exception {
        System.out.println("Testing application onboarding search no matching app");       
        onboardApp();
        
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setAid(onboardUUID);
        
        Response response = admin.getOnboardedInfoSearch(jsonOnboard,adminAuthorization);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());

        assertEquals(200, response.getStatus());
    }
    
    @Test
    public void test7_onboardSearch_wrongCredentials() throws Exception {
        System.out.println("Testing application onboarding search w/ wrong credentials");    
        
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setAid(onboardUUID);
        Response response = admin.getOnboardedInfoSearch(jsonOnboard,wrongAdminAuthorization);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());

        assertEquals(401, response.getStatus());
    }

    // Missing appname
    @Test
    public void test7_onboardSearch_noAppName() throws Exception {
        System.out.println("Testing application onboarding search w/o appname");
        onboardApp();
        
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setAid(onboardUUID);
        Response response = admin.getOnboardedInfoSearch(jsonOnboard,adminAuthorization);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());

        assertEquals(200, response.getStatus());
    }
    
    @Test
    public void test7_onboardSearch_empty() throws Exception {
        System.out.println("Testing onboard search no app information");
        
        JsonOnboard jsonOnboard = new JsonOnboard();
        Response response =  admin.getOnboardedInfoSearch(jsonOnboard,adminAuthorization);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());

        assertEquals(400, response.getStatus());
    }

    @Test
    public void test8_onboardUpdate() throws Exception {
        System.out.println("Testing application onboarding update");
        onboardApp();
        
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setUserId("TestUser3");
        jsonOnboard.setPassword("TestPassword3");
        jsonOnboard.setAid(onboardUUID);
        Response response = admin.updateOnboardApp(jsonOnboard,adminAuthorization);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());

        assertEquals(200, response.getStatus());
    }
    
    @Ignore //not working correctly
    @Test
    public void test8_onboardUpdate_withAppName() throws Exception {
        System.out.println("Testing application onboarding update w appname");
        onboardApp();
        
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setUserId("TestUser3");
        jsonOnboard.setPassword("TestPassword3");
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setAid(onboardUUID);
        Response response = admin.updateOnboardApp(jsonOnboard,adminAuthorization);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());

        assertEquals(200, response.getStatus());
    }
    
    @Test
    public void test8_onboardUpdate_noUUID() throws Exception {
        System.out.println("Testing application onboarding update null uuid");
        onboardApp();
        
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setUserId("TestUser3");
        jsonOnboard.setPassword("TestPassword3");
        jsonOnboard.setAid(null);
        Response response = admin.updateOnboardApp(jsonOnboard,adminAuthorization);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());

        assertEquals(400, response.getStatus());
    }
    
    @Test
    public void test8_onboardUpdate_wrongCredentialsNoAAF() throws Exception {
        System.out.println("Testing update application onboarding search w/ wrong credentials");
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setUserId("TestUser3");
        jsonOnboard.setPassword("TestPassword3");
        jsonOnboard.setAid(onboardUUID);
        Response response = admin.updateOnboardApp(jsonOnboard,wrongAdminAuthorization);
        
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        assertEquals(401, response.getStatus());
    }

    // All null
    @Test
    public void test8_onboardUpdate_noAppInfo() throws Exception {
        System.out.println("Testing update application onboarding update no app information");
        onboardApp();
        
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAid(onboardUUID);
        Response response = admin.updateOnboardApp(jsonOnboard,adminAuthorization);
        
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        assertEquals(400, response.getStatus());
    }

    @Test
    public void test9_onboardDelete() throws Exception {
        System.out.println("Testing update application onboarding delete");
        onboardApp();
        
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setAid(onboardUUID);
        Response response = admin.deleteOnboardApp(jsonOnboard,adminAuthorization);
        
        //only 1 app matches keyspace
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        assertEquals(200, response.getStatus());
    }
    
    @Ignore //not working as expected
    @Test
    public void test9_onboardDelete_noAID() throws Exception {
        System.out.println("Testing update application onboarding delete no AID");
        onboardApp();
        
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setAid(null);
        Response response = admin.deleteOnboardApp(jsonOnboard,adminAuthorization);
        
        //only 1 app matches name
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        assertEquals(200, response.getStatus());
    }
    
    @Test
    public void test9_onboardDelete_noAIDManyMatch() throws Exception {
        System.out.println("Testing update application onboarding delete no AID many apps in namespace");
        onboardApp();
        onboardApp();
        
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setAid(null);
        Response response = admin.deleteOnboardApp(jsonOnboard,adminAuthorization);
        
        //multiple apps matches name
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        assertEquals(400, response.getStatus());
    }
    
    @Test
    public void test9_onboardDelete_noAID_noApp() throws Exception {
        System.out.println("Testing update application onboarding delete no AID, app not onboarded");
        
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setAid(null);
        Response response = admin.deleteOnboardApp(jsonOnboard,adminAuthorization);
        
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        assertEquals(400, response.getStatus());
    }
    
    @Test
    public void test9_onboardDelete_noAppToDelete() throws Exception {
        System.out.println("Testing update application onboarding delete no app information");
        onboardApp();
        
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname(null);
        jsonOnboard.setAid(null);
        Response response = admin.deleteOnboardApp(jsonOnboard,adminAuthorization);
        
        //only 1 app matches keyspace
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        assertEquals(400, response.getStatus());
    }
    
    @Test
    public void test9_onboardDelete_wrongCredentials() throws Exception {
        System.out.println("Testing onboard delete with wrong credentials");
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setAid(onboardUUID);
        Response response = admin.deleteOnboardApp(jsonOnboard,wrongAdminAuthorization);
        
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
        assertEquals(401, response.getStatus());
    }
    
    @Test
    public void test10_delete() throws Exception {
        System.out.println("Testing GUI delete call");
        onboardApp();
        
        assertTrue(admin.delete(adminAuthorization, onboardUUID));
    }
    
    @Test
    public void test11_getAll() {
        System.out.println("Testing GUI getall call");
        
        System.out.println("admin.getall(adminAuthorization)");
    }
    
    /**
     * Inject our mocked class and accept admin credentials
     * @throws NoSuchFieldException
     */
    public void authenticateAdminTrue() throws NoSuchFieldException {
        authMock = Mockito.mock(MusicAuthentication.class);
        FieldSetter.setField(admin, admin.getClass().getDeclaredField("authenticator"), authMock);
        
        Mockito.when(authMock.authenticateAdmin(Mockito.matches(adminAuthorization))).thenReturn(true);
    }
    
    /**
     * onboard the application and store generate uuid into {@link onboardUUID}
     * @param onboard
     * @throws Exception
     */
    public void onboardApp() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setUserId("TestUser2");
        jsonOnboard.setPassword("TestPassword2");

        Response response = admin.onboardAppWithMusic(jsonOnboard,adminAuthorization);
        Map<String, String> respMap = (Map<String, String>) response.getEntity();
        onboardUUID = respMap.get("Generated AID");
    }
   
}