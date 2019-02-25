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
import org.onap.music.authentication.CachingUtil;
import org.onap.music.datastore.MusicDataStoreHandle;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.datastore.jsonobjects.JsonInsert;
import org.onap.music.datastore.jsonobjects.JsonLeasedLock;
import org.onap.music.datastore.jsonobjects.JsonTable;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.lockingservice.cassandra.CassaLockStore;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import org.onap.music.rest.RestMusicDataAPI;
import org.onap.music.rest.RestMusicLocksAPI;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.sun.jersey.core.util.Base64;

@RunWith(MockitoJUnitRunner.class)
public class TstCachingUtil {

	RestMusicLocksAPI lock = new RestMusicLocksAPI();
	RestMusicDataAPI data = new RestMusicDataAPI();
	static PreparedQueryObject testObject;

	static String appName = TestsUsingCassandra.appName;
	static String userId = TestsUsingCassandra.userId;
	static String password = TestsUsingCassandra.password;
	static String authData = TestsUsingCassandra.authData;
	static String authorization = TestsUsingCassandra.authorization;
	static boolean isAAF = TestsUsingCassandra.isAAF;
	static UUID uuid = TestsUsingCassandra.uuid;
	static String keyspaceName = TestsUsingCassandra.keyspaceName;
	static String tableName = TestsUsingCassandra.tableName;
	static String onboardUUID = null;

	@BeforeClass
	public static void init() throws Exception {
		System.out.println("Testing CachingUtil class");
		try {
			
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("Unable to initialize before TestRestMusicData test class. " + e.getMessage());
		}
	}
	
	@After
	public void afterEachTest( ) throws MusicServiceException {

	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		testObject = new PreparedQueryObject();
		testObject.appendQueryString("DROP KEYSPACE IF EXISTS " + keyspaceName);
		MusicCore.eventualPut(testObject);
	}

	@Test
    public void test_verifyOnboard() throws Exception {
	    System.out.println("Testing onboarding of app");
    	CachingUtil cache = new CachingUtil();
    	cache.initializeAafCache();
    	Map<String, Object> authMap = CachingUtil.verifyOnboarding(appName, userId, password);
    	assertEquals(0, authMap.size());
    }
    
    @Test
    public void test_authenticateAIDUser() throws Exception {
        System.out.println("Testing authentication of AID user");
        createKeyspace();
        CachingUtil cache = new CachingUtil();
        cache.initializeAafCache();
        Map<String, Object> authMap = CachingUtil.authenticateAIDUser(appName,
                userId, password, keyspaceName);
        System.out.println("authMap is: " + authMap);
        assertEquals(0, authMap.size());
    }
    
    @Test
    public void test_getAppName() throws MusicServiceException {
        System.out.println("Testing getAppName");
        CachingUtil cache = new CachingUtil();
        cache.initializeAafCache();
        assertEquals(appName, CachingUtil.getAppName(keyspaceName));
    }

    @Test
    public void test_getUUIDFromCache() throws MusicServiceException {
        System.out.println("Testing getUUID");
        CachingUtil cache = new CachingUtil();
        cache.initializeAafCache();
        assertEquals(uuid.toString(), CachingUtil.getUuidFromMusicCache(keyspaceName));
    }
    
    @Test
    public void test_isAAFApplcation() throws MusicServiceException {
        System.out.println("Testing to see if cache gets correct isAAF info");
        CachingUtil cache = new CachingUtil();
        cache.initializeAafCache();
        assertEquals(isAAF, Boolean.valueOf(CachingUtil.isAAFApplication(appName)));
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

}
