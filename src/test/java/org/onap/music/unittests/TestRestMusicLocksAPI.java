/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2019 IBM Intellectual Property
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

import java.util.Map;
import java.util.UUID;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.onap.music.datastore.jsonobjects.JsonLeasedLock;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.rest.RestMusicLocksAPI;
import org.onap.music.rest.service.impl.MusicLocksAPIServiceImpl;
import org.onap.music.spring.data.cassandra.MusicApplication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.jersey.core.util.Base64;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = MusicApplication.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@ContextConfiguration
@WebAppConfiguration
public class TestRestMusicLocksAPI {

    @InjectMocks
    RestMusicLocksAPI restMusicLocksAPI;

    @Mock
    MusicLocksAPIServiceImpl musicLocksAPIService;

    protected MockMvc mockMVC;

    @Autowired
    WebApplicationContext webApplicationContext;

    @Mock
    Response response;

    @Before
    public void setUp() {
        mockMVC = MockMvcBuilders.webAppContextSetup(webApplicationContext).build();
    }

    @Mock
    JsonLeasedLock lockObj;

    public String mapToJson(Object obj) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(obj);
    }

    static String userId = "TestUser";
    static String password = "TestPassword";
    static String authData = userId + ":" + password;
    static String wrongAuthData = userId + ":" + "pass";
    static String authorization = new String(Base64.encode(authData.getBytes()));
    static String wrongAuthorization = new String(Base64.encode(wrongAuthData.getBytes()));

    static boolean isAAF = false;
    static UUID uuid = UUID.fromString("abc66ccc-d857-4e90-b1e5-df98a3d40ce6");
    static String appName = "TestApp";
    static String lockId = null;
    static String keyspaceName = "testCassa";
    static String tableName = "employees";
    static String lockName = "testCassa.employees.sample3";
    static String minorVersion = "X-minorVersion";
    static String xLatestVersion = "X-latestVersion";
    static String patchVersion = "X-patchVersion";

    static ResultType statusSuucess = ResultType.SUCCESS;
    static ResultType statusFailure = ResultType.FAILURE;

    @Mock
    Map<String, Object> resultMap;

    ResponseBuilder ressponseBuilder = MusicUtil.buildVersionResponse(xLatestVersion, minorVersion, patchVersion);

    @Test
    public void Test1_createLockReference() throws Exception {

        Mockito.when(musicLocksAPIService.createLockReference("test.", xLatestVersion, minorVersion, patchVersion,
                authorization, uuid.toString(), appName)).thenReturn(response);

        assertEquals(response, musicLocksAPIService.createLockReference("test.", xLatestVersion, minorVersion,
                patchVersion, authorization, uuid.toString(), appName));
    }

    @Test
    public void Test2_accquireLock() throws Exception {

        Mockito.when(musicLocksAPIService.accquireLock(xLatestVersion, minorVersion, patchVersion, "test.",
                authorization, uuid.toString(), appName)).thenReturn(response);

        assertEquals(response, musicLocksAPIService.accquireLock(xLatestVersion, minorVersion, patchVersion, "test.",
                authorization, uuid.toString(), appName));
    }

    @Test
    public void Test3_accquireLockWithLease() throws Exception {

        Mockito.when(musicLocksAPIService.accquireLockWithLease(xLatestVersion, minorVersion, patchVersion, "test.",
                authorization, uuid.toString(), appName, lockObj)).thenReturn(response);

        assertEquals(response, musicLocksAPIService.accquireLockWithLease(xLatestVersion, minorVersion, patchVersion,
                "test.", authorization, uuid.toString(), appName, lockObj));
    }

    @Test
    public void Test4_currentLockHolder() throws Exception {

        Mockito.when(musicLocksAPIService.currentLockHolder(xLatestVersion, minorVersion, patchVersion, authorization,
                "test.", uuid.toString(), appName)).thenReturn(response);

        assertEquals(response, musicLocksAPIService.currentLockHolder(xLatestVersion, minorVersion, patchVersion,
                authorization, "test.", uuid.toString(), appName));
    }

    @Test
    public void Test5_currentLockState() throws Exception {

        Mockito.when(musicLocksAPIService.currentLockState(xLatestVersion, minorVersion, patchVersion, "test.", appName,
                userId, password, uuid.toString())).thenReturn(response);

        assertEquals(response, musicLocksAPIService.currentLockState(xLatestVersion, minorVersion, patchVersion,
                "test.", appName, userId, password, uuid.toString()));
    }

    @Test
    public void Test6_unLock() throws Exception {

        Mockito.when(musicLocksAPIService.unLock(xLatestVersion, minorVersion, patchVersion, "test.", authorization,
                appName, uuid.toString())).thenReturn(response);

        assertEquals(response, musicLocksAPIService.unLock(xLatestVersion, minorVersion, patchVersion, "test.",
                authorization, appName, uuid.toString()));
    }

    @Test
    public void Test7_deleteLock() throws Exception {

        Mockito.when(musicLocksAPIService.deleteLock(xLatestVersion, minorVersion, patchVersion, "test.", authorization,
                appName, uuid.toString())).thenReturn(response);

        assertEquals(response, musicLocksAPIService.deleteLock(xLatestVersion, minorVersion, patchVersion, "test.",
                authorization, appName, uuid.toString()));

    }
}
