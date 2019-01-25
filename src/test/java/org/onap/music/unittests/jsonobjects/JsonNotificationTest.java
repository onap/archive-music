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
import org.onap.music.datastore.jsonobjects.JsonNotification;

public class JsonNotificationTest {

    private JsonNotification jsonNotification;
    
    @Before
    public void setUp()
    {
        jsonNotification= new JsonNotification();
    }
    
    @Test
    public void testGetSetNotify_field()
    {
        jsonNotification.setNotify_field("notify_field");
        assertEquals("notify_field", jsonNotification.getNotify_field());
    }
    
    @Test
    public void testGetSetEndpoint()
    {
        jsonNotification.setEndpoint("endpoint");
        assertEquals("endpoint", jsonNotification.getEndpoint());
    }
    
    @Test
    public void testGetSetUsername()
    {
        jsonNotification.setUsername("Username");
        assertEquals("Username", jsonNotification.getUsername());
    }
    
    @Test
    public void testGetSetPassword()
    {
        jsonNotification.setPassword("Password");
        assertEquals("Password", jsonNotification.getPassword());
    }
    
    @Test
    public void testGetSetResponse_body()
    {
        Map<String, String> ResponseBody= new HashMap<>();
        jsonNotification.setResponse_body(ResponseBody);
        assertEquals(ResponseBody, jsonNotification.getResponse_body());
    }
    
    @Test
    public void testGetSetNotify_change()
    {
        jsonNotification.setNotify_change("Notify_change");
        assertEquals("Notify_change", jsonNotification.getNotify_change());
    }
    
    @Test
    public void testGetSetNotify_insert()
    {
        jsonNotification.setNotify_insert("Notify_insert");
        assertEquals("Notify_insert", jsonNotification.getNotify_insert());
    }
    
    @Test
    public void testGetSetNotify_delete()
    {
        jsonNotification.setNotify_delete("Notify_delete");
        assertEquals("Notify_delete", jsonNotification.getNotify_delete());
    }
    
    @Test
    public void testGetSetOperation_type()
    {
        jsonNotification.setOperation_type("Operation_type");
        assertEquals("Operation_type", jsonNotification.getOperation_type());
    }
    
    @Test
    public void testGetSetTriggerName()
    {
        jsonNotification.setTriggerName("TriggerName");
        assertEquals("TriggerName", jsonNotification.getTriggerName());
    }
    
    
}
