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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;
import org.onap.music.datastore.jsonobjects.JsonLeasedLock;
import org.onap.music.main.ResultType;
import org.onap.music.rest.repository.impl.MusicLocksAPIRepositoryImpl;
import org.onap.music.rest.service.impl.MusicLocksAPIServiceImpl;
import org.onap.music.service.MusicCoreService;
import org.onap.music.service.impl.MusicCassaCore;
import org.powermock.core.classloader.annotations.PrepareForTest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.jersey.core.util.Base64;

@RunWith(MockitoJUnitRunner.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@PrepareForTest({ MusicCassaCore.class })
public class TestRestMusicLocksAPIService {

    /*@Mock
    MusicLocksAPIRepositoryImpl musicLocksAPIRepository;

    @Mock
    MusicCoreService musicCoreService;

    @InjectMocks
    MusicLocksAPIServiceImpl musicLocksAPIService;

    @Mock
    Response response;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        this.musicLocksAPIService = new MusicLocksAPIServiceImpl(musicLocksAPIRepository);

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

    @Ignore
    public void Test1_ServiceCreateLockReference() throws Exception {

        Response response = musicLocksAPIService.createLockReference("test.", xLatestVersion, minorVersion,
                patchVersion, authorization, uuid.toString(), appName);

        assertEquals(400, response.getStatus());
    }
    
    @Ignore
    public void Test2_ServiceCreateLockReference() throws Exception {
    	Mockito.when(musicLocksAPIRepository.musicAuthentication(appName, userId, password, keyspaceName,
                uuid.toString(), "createLockReference")).thenThrow(new Exception());
    	musicLocksAPIRepository.musicAuthentication(appName, userId, password, keyspaceName,
                uuid.toString(), "createLockReference");
    	
    }

    @Ignore
    public void Test3_ServiceCreateLockReference() throws Exception {

        Map<String, Object> map = new HashMap<>();
        map.put("aid", uuid.toString());
        map.put("test", "test");

        Mockito.when(musicLocksAPIRepository.musicAuthentication(appName, userId, password, keyspaceName,
                uuid.toString(), "createLockReference")).thenReturn(map);
        
        Response response = musicLocksAPIService.createLockReference(lockName, xLatestVersion, minorVersion,
                patchVersion, authorization, uuid.toString(), appName);

        assertEquals(401, response.getStatus());
    }

    @Ignore
    public void Test4_ServiceCreateLockReference() throws Exception {

        Map<String, Object> map = new HashMap<>();
        map.put("aid", uuid.toString());

        String lockid = null;
        Mockito.when(musicLocksAPIRepository.createLockReference(lockName)).thenReturn(lockid);

        Mockito.when(musicLocksAPIRepository.musicAuthentication(appName, userId, password, keyspaceName,
                uuid.toString(), "createLockReference")).thenReturn(map);

        Response response = musicLocksAPIService.createLockReference(lockName, xLatestVersion, minorVersion,
                patchVersion, authorization, uuid.toString(), appName);

        assertEquals(400, response.getStatus());
    }

    @Ignore
    public void Test1_ServiceAccquireLock() throws Exception {

        Response response = musicLocksAPIService.accquireLock(xLatestVersion, minorVersion, patchVersion, "test.",
                authorization, uuid.toString(), appName);

        assertEquals(400, response.getStatus());
    }
    
    @Ignore
    public void Test2_ServiceAccquireLock() throws Exception {

        Mockito.mock(MusicCassaCore.class);

        Response response = musicLocksAPIService.accquireLock(xLatestVersion, minorVersion, patchVersion, lockName,
                authorization, uuid.toString(), appName);

        assertEquals(400, response.getStatus());
    }

    @Ignore
    public void Test1_ServiceAccquireLockWithLease() throws Exception {

        Response response = musicLocksAPIService.accquireLockWithLease(xLatestVersion, minorVersion, patchVersion,
                "test.", authorization, uuid.toString(), appName, lockObj);

        assertEquals(400, response.getStatus());

    }

    @Ignore
    public void Test2_ServiceAccquireLockWithLease() throws Exception {

        Response response = musicLocksAPIService.accquireLockWithLease(xLatestVersion, minorVersion, patchVersion,
                "$testmusic$x.95351186188533762.0000000000", authorization, uuid.toString(), appName, lockObj);

        assertEquals(400, response.getStatus());

    }

    @Ignore
    public void Test1_ServiceCurrentLockHolder() {

        Response response = musicLocksAPIService.currentLockHolder(xLatestVersion, minorVersion, patchVersion,
                authorization, "test.", uuid.toString(), appName);

        assertEquals(400, response.getStatus());

    }

    @Ignore
    public void Test2_ServiceCurrentLockHolder() throws Exception {

        Response response = musicLocksAPIService.currentLockHolder(xLatestVersion, minorVersion, patchVersion,
                authorization, "$testmusic$x.95351186188533762.0000000000", uuid.toString(), appName);

        assertEquals(400, response.getStatus());

    }

    @Ignore
    public void Test1_ServiceCurrentLockState() {
        Response response = musicLocksAPIService.currentLockState(xLatestVersion, minorVersion, patchVersion, "test.",
                appName, userId, password, uuid.toString());

        assertEquals(400, response.getStatus());
    }

    @Ignore
    public void Test2_ServiceCurrentLockState() {
        Response response = musicLocksAPIService.currentLockState(xLatestVersion, minorVersion, patchVersion, lockName,
                appName, userId, password, uuid.toString());

        assertEquals(400, response.getStatus());
    }

    @Ignore
    public void Test1_ServiceUnlock() {
        Response response = musicLocksAPIService.unLock(xLatestVersion, minorVersion, patchVersion, "test.",
                authorization, appName, uuid.toString());

        assertEquals(400, response.getStatus());

    }

    @Ignore
    public void Test2_ServiceUnlock() {
        Response response = musicLocksAPIService.unLock(xLatestVersion, minorVersion, patchVersion,
                "$testmusic$x.95351186188533762.0000000000", authorization, appName, uuid.toString());

        assertEquals(204, response.getStatus());

    }

    @Ignore
    public void Test3_ServiceUnlock() throws Exception {
        Map<String, Object> map = new HashMap<>();
        map.put("aid", uuid.toString());
        map.put("test", "test");

        Mockito.when(musicLocksAPIRepository.musicAuthentication(appName, userId, password, keyspaceName,
                uuid.toString(), "unLock")).thenReturn(map);

        Response response = musicLocksAPIService.unLock(xLatestVersion, minorVersion, patchVersion, lockName,
                authorization, appName, uuid.toString());

        assertEquals(400, response.getStatus());

    }

    @Ignore
    public void Test1_ServiceDeleteLock() throws Exception {

        Response response = musicLocksAPIService.deleteLock(xLatestVersion, minorVersion, patchVersion, "test.",
                authorization, appName, uuid.toString());

        assertEquals(400, response.getStatus());

    }

    @Ignore
    public void Test12_ServiceDeleteLock() throws Exception {
        Response response = musicLocksAPIService.deleteLock(xLatestVersion, minorVersion, patchVersion, lockName,
                authorization, appName, uuid.toString());

        assertEquals(200, response.getStatus());

    }*/
}
