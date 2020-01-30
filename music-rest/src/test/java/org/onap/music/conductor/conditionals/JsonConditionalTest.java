/*******************************************************************************
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
 *******************************************************************************/

package org.onap.music.conductor.conditionals;

import static org.junit.Assert.assertEquals;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class JsonConditionalTest {

    Map<String,Object> tableValues;
    Map<String,Object> casscadeColumnData;
    Map<String,Map<String,String>> conditions;
    JsonConditional jsonConditional;
    
    @Before
    public void setup() {
        tableValues = Mockito.mock(Map.class);
        casscadeColumnData = Mockito.mock(Map.class);
        conditions = Mockito.mock(Map.class);
        jsonConditional = new JsonConditional();
    }
    
    @Test
    public void testSetTableValues() {
        jsonConditional.setTableValues(tableValues);
        assertEquals(tableValues, jsonConditional.getTableValues());
    }
    
    @Test
    public void testSetPrimaryKey() {
        jsonConditional.setPrimaryKey("primarykey");
        assertEquals("primarykey", jsonConditional.getPrimaryKey());
    }
    
    @Test
    public void testSetPrimaryKeyValue() {
        jsonConditional.setPrimaryKeyValue("primarykeyvalue");
        assertEquals("primarykeyvalue", jsonConditional.getPrimaryKeyValue());
    }
    
    @Test
    public void testSetCasscadeColumnName() {
        jsonConditional.setCasscadeColumnName("columnname");
        assertEquals("columnname", jsonConditional.getCasscadeColumnName());
    }
    
    @Test
    public void testSetCasscadeColumnData() {
        jsonConditional.setCasscadeColumnData(casscadeColumnData);
        assertEquals(casscadeColumnData, jsonConditional.getCasscadeColumnData());
    }
    
    @Test
    public void testSetConditions() {
        jsonConditional.setConditions(conditions);
        assertEquals(conditions, jsonConditional.getConditions());
    }
    
}
