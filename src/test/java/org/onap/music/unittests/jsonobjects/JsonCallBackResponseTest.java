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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.onap.music.datastore.jsonobjects.JSONCallbackResponse;

public class JsonCallBackResponseTest {
    
    private JSONCallbackResponse jsonCallBackResponse;
    
    @Before
    public void setUp()
    {
        jsonCallBackResponse  = new JSONCallbackResponse();
    }
    
    @Test
    public void testFullTable()
    {
        jsonCallBackResponse.setFull_table("fullTable");
        assertEquals("fullTable", jsonCallBackResponse.getFull_table());
    }
    
    @Test
    public void testKeyspace()
    {
        jsonCallBackResponse.setKeyspace("keyspace");
        assertEquals("keyspace", jsonCallBackResponse.getKeyspace());
    }
    
    @Test
    public void testOperation()
    {
        jsonCallBackResponse.setOperation("Operation");
        assertEquals("Operation", jsonCallBackResponse.getOperation());
    }
    
    @Test
    public void testTable_name()
    {
        jsonCallBackResponse.setTable_name("Table_name");
        assertEquals("Table_name", jsonCallBackResponse.getTable_name());
    }
    
    
    @Test
    public void testPrimary_key()
    {
        jsonCallBackResponse.setPrimary_key("Primary_key");
        assertEquals("Primary_key", jsonCallBackResponse.getPrimary_key());
    }
    
    
    @Test
    public void testMiscObjects()
    {
        jsonCallBackResponse.setMiscObjects("MiscObjects");
        assertEquals("MiscObjects", jsonCallBackResponse.getMiscObjects());
    }
    
    @Test
    public void testChangeValue()
    {
        Map<String, String> changeValue = new HashMap<>();
        jsonCallBackResponse.setChangeValue(changeValue);
        assertEquals(changeValue, jsonCallBackResponse.getChangeValue());
    }
    
    @Test
    public void testUpdateList()
    {
        List<String> list= new ArrayList<>();
        jsonCallBackResponse.setUpdateList(list);
        assertEquals(list, jsonCallBackResponse.getUpdateList());
    }
    
    

}
