/*
 * ============LICENSE_START==========================================
 *  org.onap.music
 * ===================================================================
 *  Copyright (c) 2019 IBM.
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

import javax.ws.rs.core.Response;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.onap.music.datastore.jsonobjects.JSONObject;
import org.onap.music.datastore.jsonobjects.JsonOnboard;
import org.onap.music.rest.RestMusicAdminAPI;
import org.onap.music.rest.service.RestMusicAdminService;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@RunWith(MockitoJUnitRunner.class)
public class TestRestMusicAdminAPI {

    @InjectMocks
    RestMusicAdminAPI restMusicAdminAPI;

    @Mock
    RestMusicAdminService restMusicAdminService;

    @Mock
    Response response;

    private JsonOnboard json;

    public String mapToJson(Object obj) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(obj);
    }

    @Test
    public void testOnboardAppWithMusic() throws Exception {
        json = new JsonOnboard("testApp", "testUser", "testPassword", "false", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
        Mockito.when(restMusicAdminService.onboardAppWithMusic(json)).thenReturn(response);
        Assert.assertNotNull(restMusicAdminAPI.onboardAppWithMusic(json));

    }

    @Test
    public void testGetOnboardedInfoSearch() throws Exception {

        JsonOnboard json = new JsonOnboard("testApp", "", "", "false", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
        Mockito.when(restMusicAdminService.getOnboardedInfoSearch(json)).thenReturn(response);
        Assert.assertNotNull(restMusicAdminAPI.getOnboardedInfoSearch(json));

    }

    @Test
    public void testDeleteOnboardApp() throws Exception {

        JsonOnboard json = new JsonOnboard("testApp", "", "", "", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
        Mockito.when(restMusicAdminService.deleteOnboardApp(json)).thenReturn(response);
        Assert.assertNotNull(restMusicAdminAPI.deleteOnboardApp(json));

    }

    @Test
    public void testUpdateOnboardApp() throws Exception {

        JsonOnboard json = new JsonOnboard("testApp", "testUser", "testPassword", "false",
                "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
        Mockito.when(restMusicAdminService.updateOnboardApp(json)).thenReturn(response);
        Assert.assertNotNull(restMusicAdminAPI.updateOnboardApp(json));

    }

    @Test
    public void testCallbackOps() throws Exception {
        JSONObject json = new JSONObject();
        assertEquals("Success", restMusicAdminAPI.callbackOps(json));

    }

}
