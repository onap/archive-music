/*
 * ============LICENSE_START========================================== org.onap.music
 * =================================================================== Copyright (c) 2017 AT&T Intellectual Property
 * =================================================================== Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the
 * License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 * 
 * ============LICENSE_END=============================================
 * ====================================================================
 */

package org.onap.music.unittests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.UUID;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mindrot.jbcrypt.BCrypt;
import org.mockito.junit.MockitoJUnitRunner;
import org.onap.music.authentication.CachingUtil;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import org.onap.music.rest.RestMusicDataAPI;
import com.datastax.driver.core.DataType;
import com.sun.jersey.core.util.Base64;

public class TstCachingUtil {

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
    static String xLatestVersion = "X-latestVersion";
    static String onboardUUID = null;

    @BeforeClass
    public static void init() throws Exception {
        System.out.println("Testing CachingUtil class");
        try {
            createKeyspace();
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("Unable to initialize before CachingUtil test class. " + e.getMessage());
        }
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        testObject = new PreparedQueryObject();
        testObject.appendQueryString("DROP KEYSPACE IF EXISTS " + keyspaceName);
        MusicCore.eventualPut(testObject);
    }

    @Test
    public void test_isAAF_False() throws Exception {
        System.out.println("Testing isAAF, nonaaf app");
        assertEquals("false", CachingUtil.isAAFApplication(appName));
    }

    @Test
    public void test_getUUidFromMusicCache() throws Exception {
        System.out.println("Testing getUUID from MusicCache");
        assertEquals(uuid.toString(), CachingUtil.getUuidFromMusicCache(keyspaceName));
    }

    @Test
    public void test_getAppName() throws Exception {
        System.out.println("Testing getAppName");
        assertEquals(appName, CachingUtil.getAppName(keyspaceName));
    }

    @Test
    public void test_verifyOnboarding() throws Exception {
        System.out.println("Testing verifyOnboarding");
        assertTrue(CachingUtil.verifyOnboarding(appName, userId, password).isEmpty());
    }

    @Test
    public void test_verifyOnboardingFailure() throws Exception {
        System.out.println("Testing verifyOnboarding with bad password");
        assertFalse(CachingUtil.verifyOnboarding(appName, userId, "pass").isEmpty());
    }


    @Test
    public void test_authenticateAIDUser() throws Exception {
        System.out.println("Testing authenticateAIDUser");
        assertTrue(CachingUtil.authenticateAIDUser(appName, userId, password, keyspaceName).isEmpty());
    }

    @Test
    public void test_authenticateAIDUserFailure() throws Exception {
        System.out.println("Testing authenticateAIDUser bad password");
        assertFalse(CachingUtil.authenticateAIDUser(appName, userId, "pass", keyspaceName).isEmpty());
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
        CachingUtil.updateMusicCache(keyspaceName, appName);
        CachingUtil.updateMusicValidateCache(appName, userId, hashedpwd);
        MusicCore.eventualPut(query);
    }

}
