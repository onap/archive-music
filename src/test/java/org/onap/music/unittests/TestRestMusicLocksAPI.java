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

import org.json.JSONObject;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.mockito.Mock;
import org.onap.music.datastore.jsonobjects.JsonLeasedLock;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.spring.data.cassandra.MusicApplication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
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
    // Response response1 = ressponseBuilder.status(Status.OK).entity(new
    // JsonResponse(statusSuucess).setLock(lockId).toMap()).build();

    @Test
    public void Test_createLockReference(){
      //
    }
    @Ignore
    public void Test1_createLockReference() throws Exception {

        String uri = "/rest/v2/locks/create/testmusic";

        MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders.post(uri)
                .accept(MediaType.APPLICATION_JSON);
        requestBuilder.header("Authorization", authorization);
        requestBuilder.header("aid", uuid.toString());
        requestBuilder.header("ns", appName);
        requestBuilder.content(response.toString());

        MvcResult mvcResult = mockMVC.perform(requestBuilder).andReturn();

        int status = mvcResult.getResponse().getStatus();

        assertEquals(200, status);

    }

    @Ignore
    public void Test2_accquireLock() throws Exception {

        String uri = "/rest/v2/locks/acquire/$testmusic$x-95351186188533762-0000000000";

        MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders.get(uri)
                .accept(MediaType.APPLICATION_JSON);
        requestBuilder.header("Authorization", authorization);
        requestBuilder.header("aid", uuid.toString());
        requestBuilder.header("ns", appName);
        requestBuilder.content(response.toString());

        MvcResult mvcResult = mockMVC.perform(requestBuilder).andReturn();

        int status = mvcResult.getResponse().getStatus();

        assertEquals(200, status);
    }

    @Ignore
    public void Test3_accquireLockWithLease() throws Exception {

        String uri = "/rest/v2/locks/acquire-with-lease/$testmusic$x-95351186188533762-0000000000";
        JSONObject obj1 = new JSONObject();
        MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders.post(uri)
                .accept(MediaType.APPLICATION_JSON);
        requestBuilder.header("Authorization", authorization);
        requestBuilder.contentType(MediaType.APPLICATION_JSON);
        requestBuilder.header("aid", uuid.toString());
        requestBuilder.header("ns", appName);
        requestBuilder.content(obj1.toString());

        MvcResult mvcResult = mockMVC.perform(requestBuilder).andReturn();

        int status = mvcResult.getResponse().getStatus();

        assertEquals(200, status);
    }

    @Ignore
    public void Test4_currentLockHolder() throws Exception {

        String uri = "/rest/v2/locks/enquire/testmusic";

        MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders.get(uri)
                .accept(MediaType.APPLICATION_JSON);
        requestBuilder.header("Authorization", authorization);
        requestBuilder.header("aid", uuid.toString());
        requestBuilder.header("ns", appName);
        requestBuilder.content(response.toString());

        MvcResult mvcResult = mockMVC.perform(requestBuilder).andReturn();

        int status = mvcResult.getResponse().getStatus();

        assertEquals(200, status);
    }

    @Ignore
    public void Test5_currentLockState() throws Exception {

        String uri = "/rest/v2/locks/testmusic";

        MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders.get(uri)
                .accept(MediaType.APPLICATION_JSON);
        requestBuilder.header("Authorization", authorization);
        requestBuilder.header("aid", uuid.toString());
        requestBuilder.header("ns", appName);
        requestBuilder.content(response.toString());
        requestBuilder.header("userId", userId);
        requestBuilder.header("password", password);

        MvcResult mvcResult = mockMVC.perform(requestBuilder).andReturn();

        int status = mvcResult.getResponse().getStatus();

        assertEquals(200, status);

    }

    @Ignore
    public void Test6_unLock() throws Exception {

        String uri = "/rest/v2/locks/release/$testmusic$x-95351186188533762-0000000000";

        MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders.delete(uri)
                .accept(MediaType.APPLICATION_JSON);
        requestBuilder.header("Authorization", authorization);
        requestBuilder.header("aid", uuid.toString());
        requestBuilder.header("ns", appName);
        requestBuilder.content(response.toString());

        MvcResult mvcResult = mockMVC.perform(requestBuilder).andReturn();

        int status = mvcResult.getResponse().getStatus();

        assertEquals(200, status);
    }

    @Ignore
    public void Test7_deleteLock() throws Exception {

        String uri = "/rest/v2/locks/delete/testmusic";

        MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders.delete(uri)
                .accept(MediaType.APPLICATION_JSON);
        requestBuilder.header("Authorization", authorization);
        requestBuilder.header("aid", uuid.toString());
        requestBuilder.header("ns", appName);
        requestBuilder.content(response.toString());

        MvcResult mvcResult = mockMVC.perform(requestBuilder).andReturn();

        int status = mvcResult.getResponse().getStatus();

        assertEquals(200, status);

    }
}
