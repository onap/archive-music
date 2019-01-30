/*
 * ============LICENSE_START==========================================
 * org.onap.music
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

package org.onap.music.unittests.jsonobjects;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.onap.music.datastore.jsonobjects.JsonCallback;

public class JsonCallBackTest {
    private JsonCallback jsonCallBack;

    @Before
    public void setUp() {
        jsonCallBack = new JsonCallback();
    }

    @Test
    public void testUuid() {
        jsonCallBack.setUuid("uuid");
        assertEquals("uuid", jsonCallBack.getUuid());
    }

    @Test
    public void testApplicationName() {
        jsonCallBack.setApplicationName("ApplicationName");
        assertEquals("ApplicationName", jsonCallBack.getApplicationName());
    }

    @Test
    public void testNotifyOn() {
        jsonCallBack.setNotifyOn("NotifyOn");
        assertEquals("NotifyOn", jsonCallBack.getNotifyOn());
    }

    @Test
    public void testApplicationUsername() {
        jsonCallBack.setApplicationUsername("ApplicationUsername");
        assertEquals("ApplicationUsername", jsonCallBack.getApplicationUsername());
    }

    @Test
    public void testApplicationPassword() {
        jsonCallBack.setApplicationPassword("ApplicationPassword");
        assertEquals("ApplicationPassword", jsonCallBack.getApplicationPassword());
    }

    @Test
    public void testApplicationNotificationEndpoint() {
        jsonCallBack.setApplicationNotificationEndpoint("ApplicationNotificationEndpoint");
        assertEquals("ApplicationNotificationEndpoint", jsonCallBack.getApplicationNotificationEndpoint());
    }

    @Test
    public void testNotifyWhenChangeIn() {
        jsonCallBack.setNotifyWhenChangeIn("NotifyWhenChangeIn");
        assertEquals("NotifyWhenChangeIn", jsonCallBack.getNotifyWhenChangeIn());
    }

    @Test
    public void testNotifyWhenInsertsIn() {
        jsonCallBack.setNotifyWhenInsertsIn("NotifyWhenInsertsIn");
        assertEquals("NotifyWhenInsertsIn", jsonCallBack.getNotifyWhenInsertsIn());
    }

    @Test
    public void testNotifyWhenDeletesIn() {
        jsonCallBack.setNotifyWhenDeletesIn("NotifyWhenDeletesIn");
        assertEquals("NotifyWhenDeletesIn", jsonCallBack.getNotifyWhenDeletesIn());
    }

    @Test
    public void testResponseBody() {
        Map<String, String> response = new HashMap<>();
        jsonCallBack.setResponseBody(response);
        assertEquals(response, jsonCallBack.getResponseBody());
    }

}
